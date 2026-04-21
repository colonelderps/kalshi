"""Experiment engine for the daily freakonomics-hunter.

What lives here:
  * GENERATORS         -- a library of hypothesis specs (segment_expr + narrative)
  * build_units_sql()  -- turn a spec into the SQL that returns one row per "unit"
                          (user or trade) with pnl, notional, win flag, in_segment flag
  * run_experiment()   -- execute a spec, compute segment vs baseline stats,
                          return a dict of results ready to INSERT into `experiments`
  * welch_pvalue(), two_proportion_pvalue() -- minimal stats helpers so we don't
                          drag scipy into the project

Segmentation philosophy: we pick features that a casual observer might think
are irrelevant but could plausibly separate informed traders from noise -- the
"freakonomics" angle. Many will fizzle; that's fine. The daily log is the
point: over time we spot patterns across them.

Adding a new hypothesis: append to GENERATORS. Keep `key` stable once chosen
(it's how we detect "already tested this").
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


# ---------------------------------------------------------------------------
# Stats helpers (no scipy dependency)
# ---------------------------------------------------------------------------

def _normal_cdf(z: float) -> float:
    """Standard normal CDF via erf."""
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def welch_pvalue(xs: list[float], ys: list[float]) -> float | None:
    """Two-sided Welch's t-test p-value (approximate via normal for n>=30).

    Returns None if either sample is too small or has zero variance.
    """
    n1, n2 = len(xs), len(ys)
    if n1 < 3 or n2 < 3:
        return None
    m1 = sum(xs) / n1
    m2 = sum(ys) / n2
    v1 = sum((x - m1) ** 2 for x in xs) / (n1 - 1)
    v2 = sum((y - m2) ** 2 for y in ys) / (n2 - 1)
    se = math.sqrt(v1 / n1 + v2 / n2)
    if se <= 0:
        return None
    t = (m1 - m2) / se
    # Normal approx good enough at the scale we care about (>30 per group)
    p = 2.0 * (1.0 - _normal_cdf(abs(t)))
    return p


def two_proportion_pvalue(k1: int, n1: int, k2: int, n2: int) -> float | None:
    """Two-sided z-test on proportions p1=k1/n1 vs p2=k2/n2."""
    if n1 < 10 or n2 < 10:
        return None
    p1 = k1 / n1
    p2 = k2 / n2
    pooled = (k1 + k2) / (n1 + n2)
    se = math.sqrt(pooled * (1 - pooled) * (1 / n1 + 1 / n2))
    if se <= 0:
        return None
    z = (p1 - p2) / se
    return 2.0 * (1.0 - _normal_cdf(abs(z)))


# ---------------------------------------------------------------------------
# SQL builder
# ---------------------------------------------------------------------------
#
# Every experiment needs rows of the form:
#   in_segment (0 or 1), pnl (cents), notional (cents), won (0/1), resolved (0/1)
# aggregated to the `unit` (user or trade). The ROI for each side is then
# SUM(pnl)/SUM(notional) over its rows; win-rate is SUM(won)/SUM(resolved).
#
# Both modes lean on trades_social + markets, restricted to resolved markets
# so ROI/win math is well-defined.

_TRADE_ROW_CTE = """
trade_base AS (
    SELECT
        t.taker_social_id AS sid,
        t.created_ts      AS ts,
        t.taker_nickname  AS nickname,
        t.ticker          AS ticker,
        t.price_cents     AS price_cents,
        t.count_fp        AS count_fp,
        t.taker_side      AS taker_side,
        m.result          AS result,
        m.category        AS category,
        m.series_ticker   AS series_ticker,
        m.close_ts        AS close_ts,
        ({segment_expr})  AS trade_in_segment,
        CASE
            WHEN m.result = 'yes' AND t.taker_side = 'yes' THEN (100.0 - t.price_cents) * t.count_fp
            WHEN m.result = 'no'  AND t.taker_side = 'no'  THEN t.price_cents * t.count_fp
            WHEN m.result IN ('yes','no')                  THEN -1.0 * t.price_cents * t.count_fp
            ELSE 0
        END                 AS pnl,
        CASE WHEN m.result IN ('yes','no') THEN t.price_cents * t.count_fp ELSE 0 END AS notional,
        CASE WHEN m.result IN ('yes','no') AND t.taker_side = m.result THEN 1 ELSE 0 END AS won,
        CASE WHEN m.result IN ('yes','no') THEN 1 ELSE 0 END AS resolved
    FROM trades_social t
    LEFT JOIN markets m ON m.ticker = t.ticker
    WHERE t.taker_social_id != ''
      AND m.result IN ('yes','no')
)
"""


def build_units_sql(unit: str, segment_expr: str) -> str:
    """Return SQL that yields one row per unit with aggregated pnl/notional/etc.

    unit = 'trade': every resolved taker trade is a row; in_segment = trade_in_segment.
    unit = 'user':  aggregated per social_id; in_segment = 1 if ANY of the user's
                    trades match the segment expression.
    """
    if unit == "trade":
        return f"""
        WITH {_TRADE_ROW_CTE.format(segment_expr=segment_expr)}
        SELECT
            trade_in_segment AS in_segment,
            pnl,
            notional,
            won,
            resolved
        FROM trade_base
        WHERE notional > 0
        """
    if unit == "user":
        return f"""
        WITH {_TRADE_ROW_CTE.format(segment_expr=segment_expr)}
        SELECT
            MAX(trade_in_segment) AS in_segment,
            SUM(pnl)              AS pnl,
            SUM(notional)         AS notional,
            SUM(won)              AS won,
            SUM(resolved)         AS resolved
        FROM trade_base
        GROUP BY sid
        HAVING SUM(notional) > 0
        """
    raise ValueError(f"unknown unit {unit!r}")


# ---------------------------------------------------------------------------
# Generator library -- ~30 hypotheses to start.
# Each dict is a self-contained experiment spec.
#
# Fields:
#   key           -- stable, unique. Don't change once released (that's the
#                    "already tested?" lookup). Add a new key instead.
#   hypothesis    -- the narrative for humans
#   unit          -- 'user' or 'trade'
#   metric        -- 'roi' or 'win_rate'
#   segment_expr  -- SQL expr evaluated per-trade. Must reference t.*, m.*.
#   notes         -- optional freakonomics rationale
# ---------------------------------------------------------------------------

GENERATORS: list[dict[str, str]] = [
    # -- Nickname patterns ----------------------------------------------------
    {
        "key": "nickname_ends_digits_user_roi",
        "hypothesis": "Users whose nickname ends with digits earn a different ROI than those whose don't.",
        "unit": "user", "metric": "roi",
        "segment_expr": "t.taker_nickname GLOB '*[0-9]'",
        "notes": "Numeric-suffix nicknames often auto-generated; may indicate disengaged retail.",
    },
    {
        "key": "nickname_ends_digits_user_winrate",
        "hypothesis": "Users whose nickname ends with digits win a different share of trades.",
        "unit": "user", "metric": "win_rate",
        "segment_expr": "t.taker_nickname GLOB '*[0-9]'",
        "notes": "Same theory as above; win-rate view.",
    },
    {
        "key": "nickname_all_caps_user_roi",
        "hypothesis": "All-caps nicknames have a different ROI.",
        "unit": "user", "metric": "roi",
        "segment_expr": "t.taker_nickname = UPPER(t.taker_nickname) AND LENGTH(t.taker_nickname) >= 3",
        "notes": "Shouting-username personality proxy.",
    },
    {
        "key": "nickname_short_user_roi",
        "hypothesis": "Short nicknames (<= 4 chars) have a different ROI.",
        "unit": "user", "metric": "roi",
        "segment_expr": "LENGTH(t.taker_nickname) BETWEEN 1 AND 4",
        "notes": "Short, claimed-early handles may signal early/savvy users.",
    },
    {
        "key": "nickname_long_user_roi",
        "hypothesis": "Long nicknames (>= 15 chars) have a different ROI.",
        "unit": "user", "metric": "roi",
        "segment_expr": "LENGTH(t.taker_nickname) >= 15",
        "notes": "Long handles often verbose/jokey; counter-test to short-nickname hypothesis.",
    },
    {
        "key": "nickname_has_nonascii_user_roi",
        "hypothesis": "Nicknames with non-ASCII characters (incl. emoji) have different ROI.",
        "unit": "user", "metric": "roi",
        "segment_expr": "LENGTH(t.taker_nickname) != LENGTH(CAST(t.taker_nickname AS BLOB))",
        "notes": "Emoji / multi-byte chars as a personality signal.",
    },

    # -- Time-of-day & weekday -----------------------------------------------
    {
        "key": "trade_overnight_et_roi",
        "hypothesis": "Trades placed overnight (00:00-06:00 UTC, ~early-morning ET) have different ROI.",
        "unit": "trade", "metric": "roi",
        "segment_expr": "(t.created_ts % 86400) < 21600",
        "notes": "Night trades may be insomniacs or international users.",
    },
    {
        "key": "trade_business_hours_et_roi",
        "hypothesis": "Trades during US business hours (13:00-21:00 UTC) have different ROI.",
        "unit": "trade", "metric": "roi",
        "segment_expr": "(t.created_ts % 86400) BETWEEN 46800 AND 75600",
        "notes": "Are pros or distracted-at-work losers dominant?",
    },
    {
        "key": "trade_weekend_roi",
        "hypothesis": "Weekend trades have a different ROI than weekday trades.",
        "unit": "trade", "metric": "roi",
        # strftime('%w') in sqlite: 0=Sunday, 6=Saturday
        "segment_expr": "strftime('%w', t.created_ts, 'unixepoch') IN ('0','6')",
        "notes": "Weekend crowds skew retail vs. weekday pros.",
    },
    {
        "key": "trade_friday_afternoon_roi",
        "hypothesis": "Friday-afternoon trades (17-22 UTC Fri) have different ROI.",
        "unit": "trade", "metric": "roi",
        "segment_expr": "strftime('%w', t.created_ts, 'unixepoch')='5' AND (t.created_ts%86400) BETWEEN 61200 AND 79200",
        "notes": "End-of-week rush / weekend-prep positioning.",
    },

    # -- Price-range / longshot ---------------------------------------------
    {
        "key": "trade_longshot_yes_roi",
        "hypothesis": "Takers buying YES at <30c earn a different ROI than others.",
        "unit": "trade", "metric": "roi",
        "segment_expr": "t.taker_side='yes' AND t.price_cents < 30",
        "notes": "Lottery-ticket longshots; classic retail behavior.",
    },
    {
        "key": "trade_favorite_yes_roi",
        "hypothesis": "Takers buying YES at >70c earn a different ROI.",
        "unit": "trade", "metric": "roi",
        "segment_expr": "t.taker_side='yes' AND t.price_cents > 70",
        "notes": "Heavy favorites: informed arb or low-edge stacking?",
    },
    {
        "key": "trade_midrange_roi",
        "hypothesis": "Midrange trades (40-60c) have a different ROI.",
        "unit": "trade", "metric": "roi",
        "segment_expr": "t.price_cents BETWEEN 40 AND 60",
        "notes": "Tossup markets -- is it informed edge or noise?",
    },
    {
        "key": "trade_longshot_no_roi",
        "hypothesis": "Takers buying NO at <30c (implied YES >70c) earn different ROI.",
        "unit": "trade", "metric": "roi",
        "segment_expr": "t.taker_side='no' AND t.price_cents < 30",
    },

    # -- Contract-size ------------------------------------------------------
    {
        "key": "trade_huge_notional_roi",
        "hypothesis": "Trades with notional >= $1000 have a different ROI.",
        "unit": "trade", "metric": "roi",
        "segment_expr": "t.price_cents * t.count_fp >= 100000",
        "notes": "Whale-trade effect at trade level.",
    },
    {
        "key": "trade_tiny_notional_roi",
        "hypothesis": "Trades with notional < $10 have a different ROI.",
        "unit": "trade", "metric": "roi",
        "segment_expr": "t.price_cents * t.count_fp < 1000",
        "notes": "Minnow / fun-money trades.",
    },

    # -- Market freshness / closeness-to-close ------------------------------
    {
        "key": "trade_near_close_roi",
        "hypothesis": "Trades within 1h of market close have different ROI.",
        "unit": "trade", "metric": "roi",
        "segment_expr": "m.close_ts IS NOT NULL AND (m.close_ts - t.created_ts) BETWEEN 0 AND 3600",
        "notes": "Last-minute positioning -- informed flow or pure gamble?",
    },
    {
        "key": "trade_far_from_close_roi",
        "hypothesis": "Trades placed >7d before market close have different ROI.",
        "unit": "trade", "metric": "roi",
        "segment_expr": "m.close_ts IS NOT NULL AND (m.close_ts - t.created_ts) > 604800",
        "notes": "Patient money vs. reactive money.",
    },

    # -- Category-specific behavior ----------------------------------------
    {
        "key": "trade_politics_longshot_roi",
        "hypothesis": "Longshot (<30c) YES trades in Politics markets have different ROI.",
        "unit": "trade", "metric": "roi",
        "segment_expr": "m.category='Politics' AND t.taker_side='yes' AND t.price_cents < 30",
    },
    {
        "key": "trade_sports_favorite_roi",
        "hypothesis": "Heavy-favorite (>70c) YES trades in Sports have different ROI.",
        "unit": "trade", "metric": "roi",
        "segment_expr": "m.category='Sports' AND t.taker_side='yes' AND t.price_cents > 70",
    },
    {
        "key": "trade_crypto_roi",
        "hypothesis": "Trades in Crypto-category markets have different ROI.",
        "unit": "trade", "metric": "roi",
        "segment_expr": "m.category='Crypto'",
        "notes": "Crypto users reputed to be degens; test their edge.",
    },

    # -- User-level behavioral segments ------------------------------------
    {
        "key": "user_taker_side_yesbias_roi",
        "hypothesis": "Users who took the YES side in >=80% of their trades have a different ROI.",
        "unit": "user", "metric": "roi",
        # The per-trade expression says "this trade was on YES". MAX=1 if any trade is YES-side,
        # which isn't quite the same as "mostly YES". We approximate: segment_in = the user
        # has at least one YES-side trade. For strict-majority, would need a HAVING clause;
        # using the MAX-based shortcut here and noting it. (Low-cost sanity ground truth.)
        "segment_expr": "t.taker_side='yes'",
        "notes": "Rough: any-YES vs. always-NO. Still catches structural yes-bias.",
    },
    {
        "key": "user_single_category_roi",
        "hypothesis": "Users who trade only in one category (MAX here approximates) have different ROI.",
        "unit": "user", "metric": "roi",
        "segment_expr": "m.category = 'Sports'",
        "notes": "Sports-focused users; a proxy for single-domain specialists.",
    },
    {
        "key": "user_politics_focus_roi",
        "hypothesis": "Users with Politics exposure have a different ROI.",
        "unit": "user", "metric": "roi",
        "segment_expr": "m.category = 'Politics'",
    },

    # -- Price-range + time crossover --------------------------------------
    {
        "key": "trade_overnight_longshot_roi",
        "hypothesis": "Overnight longshot (<30c) trades have different ROI.",
        "unit": "trade", "metric": "roi",
        "segment_expr": "(t.created_ts % 86400) < 21600 AND t.price_cents < 30",
        "notes": "Late-night lottery-ticket behavior.",
    },
    {
        "key": "trade_weekend_whale_roi",
        "hypothesis": "Weekend trades of >=$500 notional have different ROI.",
        "unit": "trade", "metric": "roi",
        "segment_expr": "strftime('%w', t.created_ts, 'unixepoch') IN ('0','6') AND t.price_cents * t.count_fp >= 50000",
    },

    # -- Round-number / clustering effects --------------------------------
    {
        "key": "trade_round_cents_roi",
        "hypothesis": "Trades at round prices (price_cents in 10/25/50/75/90) have different ROI.",
        "unit": "trade", "metric": "roi",
        "segment_expr": "t.price_cents IN (10,25,50,75,90)",
        "notes": "Price anchoring bias; are people who click round numbers worse?",
    },
    {
        "key": "trade_integer_count_roi",
        "hypothesis": "Trades with integer count_fp (whole contracts) have different ROI.",
        "unit": "trade", "metric": "roi",
        "segment_expr": "t.count_fp = CAST(t.count_fp AS INTEGER)",
        "notes": "Integer-only traders vs fractional contract users.",
    },
    {
        "key": "trade_large_round_count_roi",
        "hypothesis": "Trades with count divisible by 100 have different ROI.",
        "unit": "trade", "metric": "roi",
        "segment_expr": "t.count_fp >= 100 AND CAST(t.count_fp AS INTEGER) % 100 = 0",
        "notes": "Round-lot sizing.",
    },

    # -- Win-rate mirrors of a few key ROI experiments ---------------------
    {
        "key": "trade_longshot_yes_winrate",
        "hypothesis": "Longshot YES (<30c) trades win at a different rate.",
        "unit": "trade", "metric": "win_rate",
        "segment_expr": "t.taker_side='yes' AND t.price_cents < 30",
    },
    {
        "key": "trade_near_close_winrate",
        "hypothesis": "Trades within 1h of close win at a different rate.",
        "unit": "trade", "metric": "win_rate",
        "segment_expr": "m.close_ts IS NOT NULL AND (m.close_ts - t.created_ts) BETWEEN 0 AND 3600",
    },
    {
        "key": "trade_overnight_winrate",
        "hypothesis": "Overnight trades (0-6 UTC) win at a different rate.",
        "unit": "trade", "metric": "win_rate",
        "segment_expr": "(t.created_ts % 86400) < 21600",
    },
]


# ---------------------------------------------------------------------------
# Experiment runner
# ---------------------------------------------------------------------------

@dataclass
class ExperimentResult:
    status: str
    segment_size: int = 0
    baseline_size: int = 0
    segment_value: float | None = None
    baseline_value: float | None = None
    effect_size: float | None = None
    p_value: float | None = None
    error_msg: str | None = None


MIN_PER_GROUP = 30  # below this we call it insufficient_data


def run_experiment(con, spec: dict[str, Any]) -> ExperimentResult:
    """Run one spec against the DB. Returns an ExperimentResult.

    Never raises -- errors are captured in .status/.error_msg so a flaky
    generator doesn't wedge the daily run.
    """
    try:
        sql = build_units_sql(spec["unit"], spec["segment_expr"])
        rows = con.execute(sql).fetchall()
    except Exception as e:  # noqa: BLE001
        return ExperimentResult(status="error", error_msg=f"{type(e).__name__}: {e}")

    seg_rows = [r for r in rows if r["in_segment"]]
    base_rows = [r for r in rows if not r["in_segment"]]

    if len(seg_rows) < MIN_PER_GROUP or len(base_rows) < MIN_PER_GROUP:
        return ExperimentResult(
            status="insufficient_data",
            segment_size=len(seg_rows),
            baseline_size=len(base_rows),
        )

    metric = spec["metric"]
    if metric == "roi":
        # Per-unit ROI for the t-test. Pooled ROI for the reported value.
        seg_rois = [r["pnl"] / r["notional"] for r in seg_rows if r["notional"]]
        base_rois = [r["pnl"] / r["notional"] for r in base_rows if r["notional"]]
        seg_pooled = sum(r["pnl"] for r in seg_rows) / max(1, sum(r["notional"] for r in seg_rows))
        base_pooled = sum(r["pnl"] for r in base_rows) / max(1, sum(r["notional"] for r in base_rows))
        effect = seg_pooled - base_pooled
        p = welch_pvalue(seg_rois, base_rois)
        return ExperimentResult(
            status="success",
            segment_size=len(seg_rows),
            baseline_size=len(base_rows),
            segment_value=seg_pooled,
            baseline_value=base_pooled,
            effect_size=effect,
            p_value=p,
        )
    if metric == "win_rate":
        seg_k = sum(r["won"] for r in seg_rows)
        seg_n = sum(r["resolved"] for r in seg_rows)
        base_k = sum(r["won"] for r in base_rows)
        base_n = sum(r["resolved"] for r in base_rows)
        if seg_n < MIN_PER_GROUP or base_n < MIN_PER_GROUP:
            return ExperimentResult(
                status="insufficient_data",
                segment_size=seg_n,
                baseline_size=base_n,
            )
        seg_wr = seg_k / seg_n
        base_wr = base_k / base_n
        return ExperimentResult(
            status="success",
            segment_size=seg_n,
            baseline_size=base_n,
            segment_value=seg_wr,
            baseline_value=base_wr,
            effect_size=seg_wr - base_wr,
            p_value=two_proportion_pvalue(seg_k, seg_n, base_k, base_n),
        )
    return ExperimentResult(status="error", error_msg=f"unknown metric {metric!r}")
