"""Strategy #2 analyzer: if you coat-tailed every single big trade, what would ROI be?

Joins trades_public with markets (resolved only) and computes taker-side P&L. Groups
by category so we can see if the effect is stronger for politics vs sports vs crypto.

Taker-side P&L for one trade:
    cost    = taker_price_cents * count_fp
    payoff  = 100*count_fp  if taker_side matches result, else 0
    profit  = payoff - cost
    roi     = profit / cost

Usage:
    python analyze_bigflow.py                      # all resolved big trades
    python analyze_bigflow.py --min-notional 500   # only >=$500 taker exposure
    python analyze_bigflow.py --by category        # category breakdown (default)
    python analyze_bigflow.py --by series          # series_ticker breakdown
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict

import db


QUERY = """
SELECT
    t.trade_id,
    t.ticker,
    t.created_ts,
    t.yes_price_cents,
    t.no_price_cents,
    t.count_fp,
    t.taker_side,
    m.category,
    m.series_ticker,
    m.status,
    m.result,
    m.settlement_value
FROM trades_public t
JOIN markets m ON m.ticker = t.ticker
WHERE m.result IN ('yes','no')
  AND m.settlement_value IN (0,100)
"""


def pnl(row) -> tuple[float, float]:
    """Return (cost_cents, profit_cents) for this trade."""
    taker_price = row["yes_price_cents"] if row["taker_side"] == "yes" else row["no_price_cents"]
    count = row["count_fp"]
    cost = taker_price * count
    taker_won = (row["taker_side"] == "yes" and row["result"] == "yes") or \
                (row["taker_side"] == "no"  and row["result"] == "no")
    payoff = 100 * count if taker_won else 0.0
    return cost, payoff - cost


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-notional", type=int, default=200, help="Dollar floor on taker exposure")
    ap.add_argument("--by", choices=["category", "series", "overall"], default="category")
    args = ap.parse_args()

    con = db.connect()
    rows = con.execute(QUERY).fetchall()
    print(f"Resolved big trades: {len(rows)}")
    if not rows:
        print("No resolved trades yet. Run backfill_public.py and enrich_markets.py first.")
        return 0

    buckets: dict[str, dict[str, float]] = defaultdict(lambda: {"n": 0, "cost": 0.0, "profit": 0.0, "wins": 0})
    min_cost_cents = args.min_notional * 100

    for r in rows:
        cost, profit = pnl(r)
        if cost < min_cost_cents:
            continue
        if args.by == "category":
            key = r["category"] or "(unknown)"
        elif args.by == "series":
            key = r["series_ticker"] or "(unknown)"
        else:
            key = "ALL"
        b = buckets[key]
        b["n"] += 1
        b["cost"] += cost
        b["profit"] += profit
        b["wins"] += 1 if profit > 0 else 0

    # Global summary too
    total_n = sum(b["n"] for b in buckets.values())
    total_cost = sum(b["cost"] for b in buckets.values())
    total_profit = sum(b["profit"] for b in buckets.values())
    total_wins = sum(b["wins"] for b in buckets.values())

    print()
    print(f"{'bucket':<25} {'trades':>8} {'winrate':>8} {'cost($)':>12} {'profit($)':>12} {'ROI':>8}")
    print("-" * 80)
    for key in sorted(buckets, key=lambda k: -buckets[k]["cost"]):
        b = buckets[key]
        roi = b["profit"] / b["cost"] if b["cost"] else 0
        wr = b["wins"] / b["n"] if b["n"] else 0
        print(f"{key:<25} {b['n']:>8} {wr*100:>7.1f}% {b['cost']/100:>12,.0f} {b['profit']/100:>12,.0f} {roi*100:>7.2f}%")
    print("-" * 80)
    total_roi = total_profit / total_cost if total_cost else 0
    total_wr = total_wins / total_n if total_n else 0
    print(f"{'TOTAL':<25} {total_n:>8} {total_wr*100:>7.1f}% {total_cost/100:>12,.0f} {total_profit/100:>12,.0f} {total_roi*100:>7.2f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
