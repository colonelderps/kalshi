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
