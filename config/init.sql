CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    sent_timestamp BIGINT NOT NULL,
    received_timestamp BIGINT NOT NULL,
    payload BYTEA NOT NULL
); 