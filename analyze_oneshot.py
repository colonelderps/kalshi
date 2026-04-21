"""Strategy #1 analyzer: identify "one-shot whales" in trades_social and compute their ROI.

A one-shot whale is a taker_social_id that:
  - made EXACTLY ONE big trade (>= --min-notional) across our collected history, AND
  - has no other smaller trades either (fully unique id in the dataset)

We then look up whether the market they bet on resolved in their favor, and aggregate
ROI by market category. Identical math to analyze_bigflow.py, but restricted to trades
by single-appearance social_ids with a named profile.

Heavily biased sample (silent insiders have no profile), as discussed -- user asked to
run it anyway.

Usage:
    python analyze_oneshot.py                  # default $200 floor
    python analyze_oneshot.py --min-notional 500
    python analyze_oneshot.py --by series
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict

import db


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-notional", type=int, default=200)
    ap.add_argument("--by", choices=["category", "series", "overall"], default="category")
    ap.add_argument("--include-maker", action="store_true",
                    help="Also count maker_social_id one-shots (default: taker only)")
    args = ap.parse_args()

    con = db.connect()
    min_cost_cents = args.min_notional * 100

    # Find social_ids with exactly one trade overall (taker side).
    one_shot_takers = {
        r["taker_social_id"] for r in con.execute(
            "SELECT taker_social_id FROM trades_social "
            "WHERE taker_social_id != '' "
            "GROUP BY taker_social_id HAVING COUNT(*) = 1"
        ).fetchall()
    }
    print(f"One-shot taker social_ids: {len(one_shot_takers)}")
    if args.include_maker:
        one_shot_makers = {
            r["maker_social_id"] for r in con.execute(
                "SELECT maker_social_id FROM trades_social "
                "WHERE maker_social_id != '' "
                "GROUP BY maker_social_id HAVING COUNT(*) = 1"
            ).fetchall()
        }
        print(f"One-shot maker social_ids: {len(one_shot_makers)}")
    else:
        one_shot_makers = set()

    if not one_shot_takers and not one_shot_makers:
        print("No one-shot whales yet. Let collect_social.py run longer.")
        return 0

    rows = con.execute("""
        SELECT t.trade_id, t.ticker, t.price_cents, t.count_fp, t.taker_side,
               t.taker_social_id, t.maker_social_id, t.taker_nickname,
               m.category, m.series_ticker, m.result
        FROM trades_social t
        LEFT JOIN markets m ON m.ticker = t.ticker
        WHERE m.result IN ('yes','no')
    """).fetchall()

    buckets: dict[str, dict] = defaultdict(lambda: {"n": 0, "cost": 0.0, "profit": 0.0, "wins": 0, "names": set()})

    for r in rows:
        # Which side of the trade (if any) is a one-shot whale we care about?
        is_taker_whale = r["taker_social_id"] in one_shot_takers
        is_maker_whale = args.include_maker and r["maker_social_id"] in one_shot_makers
        if not (is_taker_whale or is_maker_whale):
            continue

        # For the whale's side: if taker whale, the whale IS the taker -> use taker_side.
        # If maker whale, the whale is on the opposite side of taker_side.
        whale_side = r["taker_side"] if is_taker_whale else ("no" if r["taker_side"] == "yes" else "yes")
        # v1 social gives us only one price field -- that's the taker price in cents.
        # For a maker whale, their fill price is 100 - taker_price (complementary side).
        taker_price = r["price_cents"]
        whale_price = taker_price if is_taker_whale else (100 - taker_price)

        count = r["count_fp"] or 0
        cost = whale_price * count
        if cost < min_cost_cents:
            continue
        whale_won = (whale_side == r["result"])
        profit = (100 * count if whale_won else 0.0) - cost

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
        if r["taker_nickname"]:
            b["names"].add(r["taker_nickname"])

    if not buckets:
        print("No resolved one-shot whale trades yet.")
        return 0

    total_n = sum(b["n"] for b in buckets.values())
    total_cost = sum(b["cost"] for b in buckets.values())
    total_profit = sum(b["profit"] for b in buckets.values())
    total_wins = sum(b["wins"] for b in buckets.values())

    print()
    print(f"{'bucket':<25} {'trades':>8} {'winrate':>8} {'cost($)':>12} {'profit($)':>12} {'ROI':>8}  sample_names")
    print("-" * 120)
    for key in sorted(buckets, key=lambda k: -buckets[k]["cost"]):
        b = buckets[key]
        roi = b["profit"] / b["cost"] if b["cost"] else 0
        wr = b["wins"] / b["n"] if b["n"] else 0
        sample = ", ".join(list(b["names"])[:3])
        print(f"{key:<25} {b['n']:>8} {wr*100:>7.1f}% {b['cost']/100:>12,.0f} {b['profit']/100:>12,.0f} {roi*100:>7.2f}%  {sample}")
    print("-" * 120)
    roi = total_profit / total_cost if total_cost else 0
    wr = total_wins / total_n if total_n else 0
    print(f"{'TOTAL':<25} {total_n:>8} {wr*100:>7.1f}% {total_cost/100:>12,.0f} {total_profit/100:>12,.0f} {roi*100:>7.2f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
