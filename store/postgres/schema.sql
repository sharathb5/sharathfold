-- Canonical DDL for fold's Postgres store.
-- Applied by store/postgres.Migrate. Keep in sync with Store semantics.

CREATE TABLE IF NOT EXISTS fold_subscriber_seq (
    subscriber_id TEXT PRIMARY KEY,
    next_seq      BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS fold_subscribers (
    subscriber_id TEXT PRIMARY KEY,
    suspended     BOOLEAN NOT NULL DEFAULT FALSE,
    gap           BOOLEAN NOT NULL DEFAULT FALSE,
    dropped       INT NOT NULL DEFAULT 0,
    marker        TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS fold_deliveries (
    id              TEXT PRIMARY KEY,
    event_id        TEXT NOT NULL,
    subscriber_id   TEXT NOT NULL,
    url             TEXT NOT NULL,
    partition       INT NOT NULL,
    sequence        BIGINT NOT NULL,
    payload         BYTEA NOT NULL,
    event_type      TEXT NOT NULL,
    status          TEXT NOT NULL,
    attempt         INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    last_error      TEXT NOT NULL DEFAULT '',
    owner           TEXT,
    generation      BIGINT NOT NULL DEFAULT 0,
    claimed_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL,
    secret          BYTEA NOT NULL DEFAULT '',
    UNIQUE (subscriber_id, sequence)
);

-- Existing installs created before secret existed.
ALTER TABLE fold_deliveries ADD COLUMN IF NOT EXISTS secret BYTEA NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS fold_claim_idx
    ON fold_deliveries (partition, next_attempt_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS fold_subscriber_status_idx
    ON fold_deliveries (subscriber_id, status, sequence);

-- Authoritative cross-process partition ownership (logical node, not claim token).
CREATE TABLE IF NOT EXISTS fold_partition_owners (
    partition   INT PRIMARY KEY,
    owner_node  TEXT NOT NULL,
    generation  BIGINT NOT NULL,
    state       TEXT NOT NULL,
    next_owner  TEXT,
    CONSTRAINT fold_partition_owners_state_check
        CHECK (state IN ('active', 'draining')),
    CONSTRAINT fold_partition_owners_next_check
        CHECK (
            (state = 'active' AND next_owner IS NULL) OR
            (state = 'draining' AND next_owner IS NOT NULL)
        )
);
