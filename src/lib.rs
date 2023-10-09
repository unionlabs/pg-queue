use serde::Serialize;
use serde_json::Value;
use sqlx::error::BoxDynError;
use sqlx::migrate::Migrator;
use sqlx::query;
use sqlx::{Acquire, Postgres};

static MIGRATOR: Migrator = sqlx::migrate!(); // defaults to "./migrations"

/// A fifo queue backed by a postgres table. Not suitable for high-throughput, but enough for ~1k items/sec.
///
/// The queue assumes the following database schema:
///     
///     id SERIAL AUTO INCREMENT
///     status 0..2
///     item JSONB
///     error TEXT
pub struct Queue {}

impl Queue {
    /// Enqueues a new item for processing. The item's processing status is set to 0, indicating that it is ready
    /// for processing.
    pub async fn enqueue<'a, A, T: Serialize>(conn: A, item: T) -> Result<u64, BoxDynError>
    where
        A: Acquire<'a, Database = Postgres>,
    {
        let item = serde_json::to_value(item)?;
        let mut tx = conn.begin().await?;
        let id = query!("INSERT into queue VALUES (item) (item) RETURNING id")
            .fetch_one(tx)
            .await?;
        tx.commit().await?;
        Ok(id)
    }

    /// Processes the next value from the queue, calling `f` on the value. Dequeueing has the following properties:
    /// - if `f` returns an error, the item is requeued.
    /// - if `f` returns Ok(ProcessFlow::Fail), the item is permanently marked as failed.
    /// - if `f` returns Ok(ProcessFlow::Continue), the item is requeued, but process returns with Ok(()).
    /// - if `f` returns Ok(ProcessFlow::Success), the item is marked as processed.
    ///
    /// Database atomicity is used to ensure that the queue is always in a consistent state, meaning that an item
    /// process will always be retried until it reaches ProcessFlow::Fail or ProcessFlow::Success. `f` is responsible for
    /// storing metadata in the job to determine if retrying should fail permanently.
    pub async fn process<'a, A>(
        conn: A,
        f: impl FnOnce(Value) -> Result<ProcessFlow<Value>, ()>,
    ) -> Result<(), BoxDynError>
    where
        A: Acquire<'a, Database = Postgres>,
    {
        let mut tx = conn.begin().await?;

        let row = query!(
            "
            UPDATE queue
            SET status = 'in-progress'
            WHERE id = (
              SELECT id
              FROM queue
              ORDER BY id ASC
              WHERE status = 0
              FOR UPDATE SKIP LOCKED
              LIMIT 1
            )
            RETURNING *;",
        )
        .fetch_one(&mut tx)
        .await?;

        match f(row)? {
            ProcessFlow::Fail(error) => {
                // Insert error message in the queue
                todo!()
            }
            ProcessFlow::Success => {
                tx.commit().await?;
            }
            ProcessFlow::Requeue => {
                tx.rollback().await?;
            }
        }
    }
}

pub enum ProcessFlow {
    Success,
    Requeue,
    Fail(String),
}
