-- Table Definition ----------------------------------------------
CREATE TABLE events (
    aggregate_id uuid,
    sequence_id character(26) PRIMARY KEY,
    created_at timestamp with time zone NOT NULL,
    user_id uuid,
    type character varying(255),
    data bytea
);
COMMENT ON COLUMN events.sequence_id IS 'github.com/oklog/ulid';

-- Indices -------------------------------------------------------
CREATE INDEX events_user_id_idx ON events(user_id uuid_ops);
CREATE UNIQUE INDEX events_pkey ON events(sequence_id bpchar_ops);
CREATE INDEX events_created_at_idx ON events(created_at timestamptz_ops);
CREATE INDEX events_aggregate_id_idx ON events(aggregate_id uuid_ops);
