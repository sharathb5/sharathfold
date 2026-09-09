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
