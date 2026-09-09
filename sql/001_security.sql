-- financial_plumbing schema, v1. Apply once: psql "$PG_DSN" -f sql/001_security.sql
-- Idempotent (IF NOT EXISTS) so re-running is harmless.

-- One row per security. `key` is the composite FIGI when OpenFIGI resolved it, else UNK_<ticker>.
-- Mirrors data/security_master.parquet; the parquet goes away once this is the source of truth.
CREATE TABLE IF NOT EXISTS security (
    key               TEXT PRIMARY KEY,
    figi              TEXT,                         -- NULL when unresolved
    share_class_figi  TEXT,
    ticker            TEXT NOT NULL,                -- current ticker (aliases hold the history)
    name              TEXT,
    venue             TEXT,                         -- NYSE, NASDAQ, PINK, NMFQS, ...
    type              TEXT,                         -- Common Stock, ETF, FUND, Preferred Stock, ...
    openfigi_type     TEXT,
    isin              TEXT,
    in_universe       BOOLEAN NOT NULL DEFAULT FALSE,
    gic_sector        TEXT,
    gic_group         TEXT,
    gic_industry      TEXT,
    gic_sub_industry  TEXT,
    gics_code         TEXT,                         -- 8-digit sub-industry, or coarser (6/4/2)
    ipo_date          DATE,
    is_delisted       BOOLEAN NOT NULL DEFAULT FALSE,
    fund_category     TEXT,
    seeded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS security_ticker_idx   ON security (ticker);
CREATE INDEX IF NOT EXISTS security_universe_idx ON security (in_universe) WHERE in_universe;
CREATE INDEX IF NOT EXISTS security_gics_idx     ON security (gics_code);

-- Which ticker pointed at which key, and when. Current alias = valid_to IS NULL.
-- A rename closes the old row (valid_to = date) and opens a new one for the new ticker, same key.
CREATE TABLE IF NOT EXISTS security_alias (
    ticker      TEXT NOT NULL,
    key         TEXT NOT NULL REFERENCES security (key),
    valid_from  DATE NOT NULL,
    valid_to    DATE,                                -- NULL = current
    PRIMARY KEY (ticker, valid_from)
);
CREATE UNIQUE INDEX IF NOT EXISTS security_alias_current_idx ON security_alias (ticker) WHERE valid_to IS NULL;

-- EODHD fundamentals, raw. General block now; other blocks get their own columns later.
CREATE TABLE IF NOT EXISTS fundamentals (
    key         TEXT PRIMARY KEY REFERENCES security (key),
    general     JSONB NOT NULL,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per job execution; replaces the summary log line as the thing you check in the morning.
CREATE TABLE IF NOT EXISTS job_run (
    id          BIGSERIAL PRIMARY KEY,
    job         TEXT NOT NULL,                       -- refresh | compact | repull_all | seed_master
    started     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished    TIMESTAMPTZ,
    status      TEXT NOT NULL DEFAULT 'running',     -- running | ok | failed
    summary     JSONB,                               -- the dict the job returns
    error       TEXT
);
CREATE INDEX IF NOT EXISTS job_run_job_started_idx ON job_run (job, started DESC);

-- Delisting detection (added 2026-09-09): first date a listed ticker was absent from the daily bulk file.
-- NULL = seen in the latest file. Absent a full trading week (7 calendar days) -> is_delisted, alias closed.
ALTER TABLE security ADD COLUMN IF NOT EXISTS missing_since DATE;
