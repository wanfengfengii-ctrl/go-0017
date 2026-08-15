-- SettleMesh schema (reference). The default deployment uses the pure-Go
-- in-process store in internal/store, whose schema mirrors these tables. A
-- SQLite (or other SQL) adapter can be slotted in behind the same store
-- interface; this migration is the source of truth for the relational layout.

CREATE TABLE IF NOT EXISTS sources (
    source_id   TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    role        TEXT NOT NULL,           -- internal | processor | bank
    currency    TEXT NOT NULL,
    timezone    TEXT NOT NULL,
    format      TEXT NOT NULL,           -- csv | ndjson
    field_map   TEXT NOT NULL,           -- JSON
    config      TEXT
);

CREATE TABLE IF NOT EXISTS rulesets (
    revision    INTEGER PRIMARY KEY,
    payload     TEXT NOT NULL            -- JSON RuleSet
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id            TEXT PRIMARY KEY,
    source_id           TEXT NOT NULL REFERENCES sources(source_id),
    status              TEXT NOT NULL,   -- receiving|validating|committed|failed|cancelled
    idempotency_key     TEXT UNIQUE,
    file_digest         TEXT NOT NULL,
    row_policy          TEXT NOT NULL,   -- atomic|isolate
    summary             TEXT,            -- JSON BatchSummary (committed only)
    revision            INTEGER NOT NULL DEFAULT 1,
    created_at          TEXT NOT NULL,
    committed_at        TEXT
);

CREATE TABLE IF NOT EXISTS records (
    record_id       TEXT PRIMARY KEY,
    batch_id        TEXT NOT NULL REFERENCES batches(batch_id),
    source_id       TEXT NOT NULL REFERENCES sources(source_id),
    role            TEXT NOT NULL,
    external_id     TEXT NOT NULL,
    business_id     TEXT,
    amount_minor    INTEGER NOT NULL,
    currency        TEXT NOT NULL,
    fee_minor       INTEGER NOT NULL DEFAULT 0,
    direction       TEXT,
    timestamp_utc   TEXT NOT NULL,
    timestamp_raw   TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    stable_key      TEXT NOT NULL,
    source_line     INTEGER NOT NULL,
    isolated        INTEGER NOT NULL DEFAULT 0,
    invalid_reason  TEXT
);
CREATE INDEX IF NOT EXISTS idx_records_batch ON records(batch_id);
CREATE INDEX IF NOT EXISTS idx_records_source_ext ON records(source_id, external_id);
CREATE INDEX IF NOT EXISTS idx_records_content ON records(content_hash);

CREATE TABLE IF NOT EXISTS duplicate_relations (
    relation_id          TEXT PRIMARY KEY,
    type                 TEXT NOT NULL,   -- exact_key|retransmit|content_fingerprint|suspected
    canonical_record_id  TEXT NOT NULL REFERENCES records(record_id),
    duplicate_record_id  TEXT NOT NULL REFERENCES records(record_id),
    source_id            TEXT NOT NULL,
    action               TEXT NOT NULL   -- exclude|report
);

CREATE TABLE IF NOT EXISTS runs (
    run_id        TEXT PRIMARY KEY,
    status        TEXT NOT NULL,         -- pending|running|succeeded|failed|cancelled
    snapshot      TEXT,                  -- JSON RunSnapshot
    started_at    TEXT,
    finished_at   TEXT,
    fault_point   TEXT
);

CREATE TABLE IF NOT EXISTS match_groups (
    match_group_id  TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL REFERENCES runs(run_id),
    type            TEXT NOT NULL,       -- exact_id|tolerance|one_to_many
    anchor_role     TEXT,
    amount_minor    INTEGER NOT NULL,
    currency        TEXT NOT NULL,
    score_tuple     TEXT NOT NULL,       -- JSON
    record_ids      TEXT NOT NULL        -- JSON array, stable-sorted
);

CREATE TABLE IF NOT EXISTS discrepancies (
    discrepancy_id  TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL REFERENCES runs(run_id),
    type            TEXT NOT NULL,       -- missing_internal|amount_mismatch|ambiguous|...
    currency        TEXT NOT NULL,
    note            TEXT,
    evidence        TEXT NOT NULL        -- JSON array
);

CREATE TABLE IF NOT EXISTS reports (
    run_id        TEXT PRIMARY KEY REFERENCES runs(run_id),
    summary       TEXT NOT NULL,         -- JSON ReportSummary (immutable once written)
    created_at    TEXT NOT NULL
);
