-- PostgreSQL 14+; run through an independently controlled migration system.
CREATE TABLE IF NOT EXISTS mira_leases (
    identity TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    fence BIGINT NOT NULL CHECK (fence > 0),
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS mira_leases_expiry ON mira_leases(expires_at);

-- Outbox operations must be inserted in the *same* database transaction as
-- their corresponding business changes to obtain exactly-once effects.
CREATE TABLE IF NOT EXISTS mira_outbox (
    cursor BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    identity TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    payload BYTEA NOT NULL,
    payload_hash BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(identity, operation_id)
);
CREATE INDEX IF NOT EXISTS mira_outbox_replay ON mira_outbox(identity,cursor);

-- Transactional consumer offsets and dedupe, scoped to one identity/group.
-- Apply business effects in the SAME database transaction before advancing
-- the cursor. At least one fenced worker must hold the identity's lease.
CREATE TABLE IF NOT EXISTS mira_consumer_progress (
    identity TEXT NOT NULL,
    group_id TEXT NOT NULL,
    cursor BIGINT NOT NULL DEFAULT 0 CHECK (cursor >= 0),
    PRIMARY KEY (identity,group_id)
);
CREATE TABLE IF NOT EXISTS mira_consumer_dedupe (
    identity TEXT NOT NULL,
    group_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    payload_hash BYTEA NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (identity,group_id,operation_id)
);
CREATE INDEX IF NOT EXISTS mira_consumer_dedupe_time ON mira_consumer_dedupe(processed_at);
