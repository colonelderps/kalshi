"""Build per-user behavioural aggregates from trades_social.

Materialises a `user_aggregates` table keyed by social_id. Run whenever you want
a fresh snapshot (it's a full rebuild, not incremental). The table is the
feature set for user-level clustering and "is this user informed?" analysis.

Columns:
  social_id            -- the user's stable id
  nickname             -- most-recent nickname seen
  first_seen_ts        -- earliest trade on either side
  last_seen_ts         -- latest trade on either side
  active_seconds       -- last - first
  n_trades_taker       -- trades where user was taker
  n_trades_maker       -- trades where user was maker
  n_trades_total       -- sum of the two
  total_taker_notional_cents
  max_taker_notional_cents
  avg_taker_notional_cents
  n_distinct_tickers
  n_distinct_categories
  taker_resolved       -- how many taker trades hit a resolved market
  taker_wins
  taker_pnl_cents      -- net cents won/lost as taker
  taker_roi            -- pnl / total_taker_notional

Usage:
    python build_user_aggregates.py
    python build_user_aggregates.py --min-trades 1   # include everyone
"""
from __future__ import annotations

import argparse
import sys
import time

import db


DDL = """
CREATE TABLE IF NOT EXISTS user_aggregates (
    social_id                   TEXT PRIMARY KEY,
    nickname                    TEXT,
    first_seen_ts               INTEGER,
    last_seen_ts                INTEGER,
    active_seconds              INTEGER,
    n_trades_taker              INTEGER,
    n_trades_maker              INTEGER,
    n_trades_total              INTEGER,
    total_taker_notional_cents  INTEGER,
    max_taker_notional_cents    INTEGER,
    avg_taker_notional_cents    REAL,
    n_distinct_tickers          INTEGER,
    n_distinct_categories       INTEGER,
    taker_resolved              INTEGER,
    taker_wins                  INTEGER,
    taker_pnl_cents             REAL,
    taker_roi                   REAL,
    rebuilt_ts                  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_ua_ntrades ON user_aggregates(n_trades_total);
CREATE INDEX IF NOT EXISTS ix_ua_taker_notional ON user_aggregates(total_taker_notional_cents);
"""


# We build the aggregate in SQL so it stays fast even at millions of social rows.
# taker_notional = price_cents * count_fp  (v1 social only gives taker price).
# maker_notional is the complementary side: (100 - price_cents) * count_fp.
BUILD_SQL = """
INSERT OR REPLACE INTO user_aggregates
SELECT
    sid                                                      AS social_id,
    MAX(nickname)                                            AS nickname,
    MIN(ts)                                                  AS first_seen_ts,
    MAX(ts)                                                  AS last_seen_ts,
    MAX(ts) - MIN(ts)                                        AS active_seconds,
    SUM(CASE WHEN role = 'taker' THEN 1 ELSE 0 END)          AS n_trades_taker,
    SUM(CASE WHEN role = 'maker' THEN 1 ELSE 0 END)          AS n_trades_maker,
    COUNT(*)                                                 AS n_trades_total,
    COALESCE(SUM(CASE WHEN role = 'taker' THEN notional ELSE 0 END), 0)
                                                             AS total_taker_notional_cents,
    COALESCE(MAX(CASE WHEN role = 'taker' THEN notional END), 0)
                                                             AS max_taker_notional_cents,
    COALESCE(AVG(CASE WHEN role = 'taker' THEN notional END), 0)
                                                             AS avg_taker_notional_cents,
    COUNT(DISTINCT ticker)                                   AS n_distinct_tickers,
    COUNT(DISTINCT category)                                 AS n_distinct_categories,
    SUM(CASE WHEN role = 'taker' AND result IN ('yes','no') THEN 1 ELSE 0 END)
                                                             AS taker_resolved,
    SUM(CASE WHEN role = 'taker' AND won = 1 THEN 1 ELSE 0 END)
                                                             AS taker_wins,
    COALESCE(SUM(CASE WHEN role = 'taker' AND result IN ('yes','no') THEN profit END), 0)
                                                             AS taker_pnl_cents,
    CASE WHEN SUM(CASE WHEN role = 'taker' AND result IN ('yes','no') THEN notional ELSE 0 END) > 0
         THEN 1.0 * SUM(CASE WHEN role = 'taker' AND result IN ('yes','no') THEN profit END)
              / SUM(CASE WHEN role = 'taker' AND result IN ('yes','no') THEN notional ELSE 0 END)
         ELSE NULL
    END                                                       AS taker_roi,
    :now                                                      AS rebuilt_ts
FROM (
    -- One row per (trade, side-the-user-was-on)
    SELECT
        t.taker_social_id AS sid,
        t.taker_nickname  AS nickname,
        t.created_ts      AS ts,
        t.ticker          AS ticker,
        m.category        AS category,
        m.result          AS result,
        'taker'           AS role,
        CAST(t.price_cents AS REAL) * t.count_fp AS notional,
        CASE
            WHEN t.taker_side = 'yes' AND m.result = 'yes' THEN 1
            WHEN t.taker_side = 'no'  AND m.result = 'no'  THEN 1
            ELSE 0
        END AS won,
        CASE
            WHEN t.taker_side = 'yes' AND m.result = 'yes' THEN (100 - t.price_cents) * t.count_fp
            WHEN t.taker_side = 'no'  AND m.result = 'no'  THEN t.price_cents * t.count_fp
            WHEN m.result IN ('yes','no')                  THEN -1.0 * t.price_cents * t.count_fp
            ELSE 0
        END AS profit
    FROM trades_social t LEFT JOIN markets m ON m.ticker = t.ticker
    WHERE t.taker_social_id != ''

    UNION ALL

    SELECT
        t.maker_social_id AS sid,
        t.maker_nickname  AS nickname,
        t.created_ts      AS ts,
        t.ticker          AS ticker,
        m.category        AS category,
        m.result          AS result,
        'maker'           AS role,
        CAST((100 - t.price_cents) AS REAL) * t.count_fp AS notional,  -- maker side
        0                 AS won,      -- we track wins only for taker role
        0                 AS profit
    FROM trades_social t LEFT JOIN markets m ON m.ticker = t.ticker
    WHERE t.maker_social_id != ''
) u
GROUP BY sid
HAVING n_trades_total >= :min_trades;
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-trades", type=int, default=1)
    args = ap.parse_args()

    con = db.connect()
    con.executescript(DDL)

    print("Clearing existing user_aggregates...")
    con.execute("DELETE FROM user_aggregates")
    con.commit()

    print("Rebuilding user_aggregates...")
    t0 = time.time()
    con.execute(BUILD_SQL, {"now": int(time.time()), "min_trades": args.min_trades})
    con.commit()

    n = con.execute("SELECT COUNT(*) FROM user_aggregates").fetchone()[0]
    with_name = con.execute("SELECT COUNT(*) FROM user_aggregates WHERE nickname != ''").fetchone()[0]
    one_shots = con.execute("SELECT COUNT(*) FROM user_aggregates WHERE n_trades_total = 1").fetchone()[0]
    whales = con.execute("SELECT COUNT(*) FROM user_aggregates WHERE max_taker_notional_cents >= 100000").fetchone()[0]
    dt_s = time.time() - t0

    print(f"Built in {dt_s:.1f}s")
    print(f"  unique social_ids        : {n:,}")
    print(f"  with a nickname          : {with_name:,}")
    print(f"  one-shot (single trade)  : {one_shots:,}")
    print(f"  whales (>= $1K max trade): {whales:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
