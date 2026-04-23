"""Realistic fade-the-whale backtest.

For each big taker trade (>= $1K notional, resolved market), try to fade it
using the next public trade in the same market as the execution price. This
is a defensible proxy for "what would the opposite-side ask have cost me
right after the big print?" -- conservative because the actual ask is
usually a hair above the last trade.

Then slice the results by category, taker price bucket, and time-to-close
to see whether the edge is uniform or concentrated.

Usage:
    python fade_backtest.py                        # default: next-trade within 60min
    python fade_backtest.py --window-sec 600       # tighter exec window (10 min)
    python fade_backtest.py --min-notional 2000    # $2K threshold instead of $1K
    python fade_backtest.py --fee-rate 0.03        # stress-test higher fees
    python fade_backtest.py --segment "t.price_cents IN (9,19,29,39,49,59,69,79,89,99)" --min-notional 0
                                                   # backtest a specific hypothesis segment
"""
from __future__ import annotations

import argparse
from collections import defaultdict

import db


def fmt_pct(x: float | None, signed: bool = True) -> str:
    if x is None:
        return "   --  "
    return f"{x*100:+6.2f}%" if signed else f"{x*100:5.2f}%"


def fmt_money(c: float) -> str:
    return f"${c/100:>12,.0f}"


def slice_report(label: str, buckets: dict) -> None:
    print(f"\n--- split by {label} ---")
    print(f"  {'bucket':<22} {'n':>6} {'cov':>6} {'taker_roi':>10} {'fade_roi':>10} {'fee_adj':>10} {'notional':>14}")
    # Sort by n desc
    for key, v in sorted(buckets.items(), key=lambda kv: -kv[1]["n"]):
        if v["n"] < 20:
            continue
        coverage = v["executed"] / v["n"] if v["n"] else 0
        taker_roi = v["taker_pnl"] / v["taker_notional"] if v["taker_notional"] else 0
        fade_roi = v["fade_pnl"] / v["fade_notional"] if v["fade_notional"] else 0
        adj_roi = v["fade_pnl_adj"] / v["fade_notional"] if v["fade_notional"] else 0
        print(f"  {str(key):<22} {v['n']:>6} {coverage:>5.0%} "
              f"{taker_roi*100:>+8.2f}%  {fade_roi*100:>+8.2f}%  {adj_roi*100:>+8.2f}%  {fmt_money(v['fade_notional'])}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-notional", type=int, default=1000, help="Dollar threshold for 'big' taker")
    ap.add_argument("--window-sec", type=int, default=3600, help="Max seconds after big trade to find entry")
    ap.add_argument("--fee-rate", type=float, default=0.02, help="Fee on fade notional")
    ap.add_argument("--exec-source", choices=["public", "social"], default="public",
                    help="Which table to look up next-trade execution prices in")
    ap.add_argument("--segment", type=str, default="",
                    help="Extra SQL filter on taker trade, e.g. \"t.price_cents IN (9,19,29,39,49,59,69,79,89,99)\"")
    args = ap.parse_args()
    min_notional_c = args.min_notional * 100

    con = db.connect()

    seg_clause = f"AND ({args.segment})" if args.segment else ""
    seg_desc = f" matching segment [{args.segment}]" if args.segment else ""
    print(f"Selecting taker trades >= ${args.min_notional}{seg_desc} in resolved markets...")
    big = con.execute(f"""
        SELECT t.trade_id, t.ticker, t.created_ts, t.price_cents, t.count_fp,
               t.taker_side, m.result, m.category, m.close_ts
        FROM trades_social t
        JOIN markets m ON m.ticker = t.ticker
        WHERE m.result IN ('yes','no')
          AND t.price_cents * t.count_fp >= ?
          {seg_clause}
    """, (min_notional_c,)).fetchall()
    print(f"  {len(big):,} taker trades found")

    totals = defaultdict(float)
    totals["n"] = 0
    totals["executed"] = 0

    by_category = defaultdict(lambda: defaultdict(float))
    by_price_bucket = defaultdict(lambda: defaultdict(float))
    by_time_to_close = defaultdict(lambda: defaultdict(float))

    def bump(buckets, key, field, val):
        buckets[key][field] += val
        buckets[key]["n"] = buckets[key].get("n", 0)  # keep key alive

    # Prepare the "next public trade" query once
    next_q = con.cursor()

    for i, r in enumerate(big):
        if i % 500 == 0 and i:
            print(f"  ...{i:,} processed")

        totals["n"] += 1

        # Price bucket for the taker price
        p_t = r["price_cents"]
        if p_t < 30: price_bucket = "<30c (longshot)"
        elif p_t < 50: price_bucket = "30-50c"
        elif p_t <= 70: price_bucket = "50-70c"
        else: price_bucket = ">70c (favorite)"

        cat = r["category"] or "(none)"

        # Time-to-close bucket
        if r["close_ts"]:
            dt = r["close_ts"] - r["created_ts"]
            if dt < 3600: ttc_bucket = "<1h"
            elif dt < 86400: ttc_bucket = "1-24h"
            elif dt < 7 * 86400: ttc_bucket = "1-7d"
            else: ttc_bucket = ">7d"
        else:
            ttc_bucket = "(unknown)"

        for buckets, key in [(by_category, cat), (by_price_bucket, price_bucket), (by_time_to_close, ttc_bucket)]:
            buckets[key]["n"] = buckets[key].get("n", 0) + 1

        # Taker PnL (as if taker invested)
        taker_won = (r["taker_side"] == r["result"])
        t_cost = r["price_cents"] * r["count_fp"]
        t_payoff = 100 * r["count_fp"] if taker_won else 0
        t_pnl = t_payoff - t_cost
        totals["taker_notional"] += t_cost
        totals["taker_pnl"] += t_pnl

        for buckets, key in [(by_category, cat), (by_price_bucket, price_bucket), (by_time_to_close, ttc_bucket)]:
            buckets[key]["taker_notional"] += t_cost
            buckets[key]["taker_pnl"] += t_pnl

        # Find next trade in same market, after this taker, within window.
        # Two sources supported:
        #   public: trades_public (has explicit yes/no price columns)
        #   social: trades_social (we have taker_side + price_cents; derive yes/no)
        if args.exec_source == "public":
            nxt = next_q.execute("""
                SELECT yes_price_cents, no_price_cents, created_ts
                FROM trades_public
                WHERE ticker = ? AND created_ts > ? AND created_ts <= ?
                ORDER BY created_ts LIMIT 1
            """, (r["ticker"], r["created_ts"], r["created_ts"] + args.window_sec)).fetchone()
            if nxt is None:
                continue
            next_yes = nxt["yes_price_cents"]
            next_no = nxt["no_price_cents"]
        else:
            nxt = next_q.execute("""
                SELECT taker_side, price_cents, created_ts, trade_id
                FROM trades_social
                WHERE ticker = ? AND created_ts > ? AND created_ts <= ?
                  AND trade_id != ?
                ORDER BY created_ts LIMIT 1
            """, (r["ticker"], r["created_ts"], r["created_ts"] + args.window_sec, r["trade_id"])).fetchone()
            if nxt is None:
                continue
            # In trades_social we have one side's price. The other side = 100 - that.
            if nxt["taker_side"] == "yes":
                next_yes = nxt["price_cents"]; next_no = 100 - nxt["price_cents"]
            else:
                next_no = nxt["price_cents"]; next_yes = 100 - nxt["price_cents"]

        totals["executed"] += 1
        for buckets, key in [(by_category, cat), (by_price_bucket, price_bucket), (by_time_to_close, ttc_bucket)]:
            buckets[key]["executed"] = buckets[key].get("executed", 0) + 1

        # Fade = opposite side. If taker bought YES, we buy NO at next_no
        fade_side_price = next_no if r["taker_side"] == "yes" else next_yes
        if fade_side_price >= 100 or fade_side_price <= 0:
            continue  # degenerate, skip

        qty = r["count_fp"]
        fade_cost = fade_side_price * qty
        fade_won = not taker_won
        fade_payoff = 100 * qty if fade_won else 0
        fade_pnl = fade_payoff - fade_cost
        fee = fade_cost * args.fee_rate
        fade_pnl_adj = fade_pnl - fee

        totals["fade_notional"] += fade_cost
        totals["fade_pnl"] += fade_pnl
        totals["fade_pnl_adj"] += fade_pnl_adj

        for buckets, key in [(by_category, cat), (by_price_bucket, price_bucket), (by_time_to_close, ttc_bucket)]:
            buckets[key]["fade_notional"] += fade_cost
            buckets[key]["fade_pnl"] += fade_pnl
            buckets[key]["fade_pnl_adj"] += fade_pnl_adj

    # -----------------------------------------------------------------------
    # Print
    # -----------------------------------------------------------------------
    print()
    print("=" * 72)
    seg_label = f"  segment=[{args.segment}]" if args.segment else ""
    print(f"Config: min_notional=${args.min_notional}  window={args.window_sec}s  fee={args.fee_rate:.0%}{seg_label}")
    print("=" * 72)
    n = totals["n"]
    execd = totals["executed"]
    print(f"Big taker trades:      {int(n):>6,}")
    print(f"With fade execution:   {int(execd):>6,}  ({execd/n:.1%} coverage)")
    print()
    print(f"Taker side ROI:        {fmt_pct(totals['taker_pnl']/totals['taker_notional'])}  "
          f"pnl={fmt_money(totals['taker_pnl'])}  notional={fmt_money(totals['taker_notional'])}")
    print(f"Fade side ROI (raw):   {fmt_pct(totals['fade_pnl']/totals['fade_notional'])}  "
          f"pnl={fmt_money(totals['fade_pnl'])}  notional={fmt_money(totals['fade_notional'])}")
    print(f"Fade side ROI (-fee):  {fmt_pct(totals['fade_pnl_adj']/totals['fade_notional'])}  "
          f"pnl={fmt_money(totals['fade_pnl_adj'])}")

    slice_report("category", by_category)
    slice_report("taker price bucket", by_price_bucket)
    slice_report("time-to-close", by_time_to_close)

    # Fee sensitivity at the top level
    print(f"\n--- fee sensitivity (raw fade ROI is {totals['fade_pnl']/totals['fade_notional']*100:+.2f}%) ---")
    for f in [0.0, 0.01, 0.02, 0.03, 0.05, 0.08]:
        # Need to recompute since we only have one snapshot; use simple subtraction
        adj = (totals["fade_pnl"] - f * totals["fade_notional"]) / totals["fade_notional"]
        print(f"  fee={f:>4.0%}  ->  ROI={adj*100:+6.2f}%")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
