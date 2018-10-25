-- Table Definition ----------------------------------------------
CREATE TABLE events (
    sequence_id character(26) PRIMARY KEY,
    aggregate_id uuid,
    user_id uuid,
    created_at timestamp with time zone NOT NULL,
    type character varying(255),
    data bytea
);
COMMENT ON COLUMN events.sequence_id IS 'github.com/oklog/ulid';

-- Indices -------------------------------------------------------
CREATE UNIQUE INDEX events_pkey ON events(sequence_id bpchar_ops);
CREATE INDEX events_aggregate_id_idx ON events(aggregate_id uuid_ops);
CREATE INDEX events_type_idx ON events(type text_ops);
