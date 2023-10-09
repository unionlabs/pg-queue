CREATE TYPE status AS ENUM ('ready', 'in-progress', 'failed');

CREATE TABLE queue (
    id BIGSERIAL PRIMARY KEY,
    status status NOT NULL DEFAULT 'ready',
    item JSONB NOT NULL,
    -- Error message in case of permanent failure. If set, status MUST be 'failed'.
    message TEXT CHECK ((message == NULL && status != 'failed') OR (message != NULL && status == 'failed')) ,
);