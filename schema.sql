-- Kalshi whale-tracker sqlite schema.
--
-- Two trade tables because there are two source streams that can't be
-- reconciled 1:1 (public firehose is anonymized, social firehose includes
-- nicknames but is realtime-only -- no historical). We backtest Strategy #2
-- off trades_public, and Strategy #1 off trades_social once it's built up.

CREATE TABLE IF NOT EXISTS trades_public (
    trade_id        TEXT PRIMARY KEY,
    ticker          TEXT NOT NULL,
    created_time    TEXT NOT NULL,        -- ISO8601 UTC
    created_ts      INTEGER NOT NULL,     -- epoch seconds, for fast range scans
    yes_price_cents INTEGER NOT NULL,     -- price of YES side at fill, 0-100
    no_price_cents  INTEGER NOT NULL,
    count_fp        REAL NOT NULL,        -- contracts (fixed-point from Kalshi)
    taker_side      TEXT NOT NULL         -- 'yes' or 'no'
);
CREATE INDEX IF NOT EXISTS ix_trades_public_ticker ON trades_public(ticker);
CREATE INDEX IF NOT EXISTS ix_trades_public_ts ON trades_public(created_ts);

CREATE TABLE IF NOT EXISTS trades_social (
    trade_id          TEXT PRIMARY KEY,
    market_id         TEXT NOT NULL,
    ticker            TEXT NOT NULL,
    created_time      TEXT NOT NULL,
    created_ts        INTEGER NOT NULL,
    price_cents       INTEGER NOT NULL,
    count             INTEGER NOT NULL,
    count_fp          REAL NOT NULL,
    taker_side        TEXT NOT NULL,
    maker_action      TEXT,
    taker_action      TEXT,
    maker_nickname    TEXT,               -- '' if user hasn't opted into Ideas
    taker_nickname    TEXT,
    maker_social_id   TEXT,               -- '' if unnamed (NOT a stable id)
    taker_social_id   TEXT
);
CREATE INDEX IF NOT EXISTS ix_trades_social_ticker ON trades_social(ticker);
CREATE INDEX IF NOT EXISTS ix_trades_social_ts ON trades_social(created_ts);
CREATE INDEX IF NOT EXISTS ix_trades_social_maker ON trades_social(maker_social_id);
CREATE INDEX IF NOT EXISTS ix_trades_social_taker ON trades_social(taker_social_id);

-- Market metadata. ticker is the canonical key for lookups in trades tables.
CREATE TABLE IF NOT EXISTS markets (
    ticker          TEXT PRIMARY KEY,
    event_ticker    TEXT,
    series_ticker   TEXT,
    category        TEXT,                 -- Politics, Sports, Crypto, etc.
    subcategory     TEXT,                 -- series-level sub-genre if we can derive
    title           TEXT,
    status          TEXT,                 -- open, closed, settled, etc.
    close_ts        INTEGER,              -- epoch seconds
    settle_ts       INTEGER,
    result          TEXT,                 -- 'yes', 'no', 'void', null if unresolved
    settlement_value INTEGER,             -- 100 if yes won, 0 if no won, NULL otherwise
    last_refreshed_ts INTEGER NOT NULL,   -- when this row was fetched
    raw_json        TEXT                  -- full market payload for later inspection
);

-- Progress marker so backfill is resumable.
CREATE TABLE IF NOT EXISTS backfill_state (
    stream      TEXT PRIMARY KEY,          -- e.g. 'public_trades'
    cursor      TEXT,
    earliest_ts INTEGER,
    latest_ts   INTEGER,
    updated_ts  INTEGER NOT NULL
);

-- ---------------------------------------------------------------------------
-- Daily experiment log: one row per hypothesis tested.
--
-- Intent: every day the scheduler picks a handful of not-yet-tested
-- "freakonomics-flavored" hypotheses from the generator library and tests
-- each as a two-sample comparison (segment vs complement). We store the
-- full spec + result so that later we can (a) detect repeats via
-- hypothesis_key and (b) mine old results for pairs that interact.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS experiments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date        TEXT    NOT NULL,        -- YYYY-MM-DD
    run_ts          INTEGER NOT NULL,
    hypothesis_key  TEXT    NOT NULL,        -- stable identifier; unique per experiment template+params
    hypothesis      TEXT    NOT NULL,        -- human-readable one-liner
    unit            TEXT    NOT NULL,        -- 'user' or 'trade'
    metric          TEXT    NOT NULL,        -- 'roi' or 'win_rate'
    segment_expr    TEXT    NOT NULL,        -- SQL snippet defining segment membership (for rerun)
    segment_size    INTEGER,
    baseline_size   INTEGER,
    segment_value   REAL,                    -- segment metric (e.g. segment ROI)
    baseline_value  REAL,
    effect_size     REAL,                    -- segment_value - baseline_value
    p_value         REAL,                    -- two-sample test p-value (approx)
    status          TEXT    NOT NULL,        -- 'success' | 'insufficient_data' | 'error'
    error_msg       TEXT,
    notes           TEXT                     -- generator-provided rationale
);
CREATE INDEX IF NOT EXISTS ix_exp_key ON experiments(hypothesis_key);
CREATE INDEX IF NOT EXISTS ix_exp_date ON experiments(run_date);
CREATE INDEX IF NOT EXISTS ix_exp_effect ON experiments(effect_size);

-- Pair-interaction results. Populated by combine_experiments.py by taking
-- two promising past experiments and re-running against the intersection
-- of their segments to see if the combination is stronger than either alone.
CREATE TABLE IF NOT EXISTS experiment_pairs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date         TEXT    NOT NULL,
    run_ts           INTEGER NOT NULL,
    exp_a_id         INTEGER NOT NULL,
    exp_b_id         INTEGER NOT NULL,
    combined_size    INTEGER,
    baseline_size    INTEGER,
    combined_value   REAL,
    baseline_value   REAL,
    a_value          REAL,
    b_value          REAL,
    interaction_lift REAL,          -- combined_value - max(a_value, b_value)
    p_value          REAL,
    status           TEXT    NOT NULL,
    FOREIGN KEY (exp_a_id) REFERENCES experiments(id),
    FOREIGN KEY (exp_b_id) REFERENCES experiments(id)
);
CREATE INDEX IF NOT EXISTS ix_pair_lift ON experiment_pairs(interaction_lift);
CREATE INDEX IF NOT EXISTS ix_pair_date ON experiment_pairs(run_date);
