"""Realistic coat-tail (copy-the-taker) backtest.

The mirror of fade_backtest.py. For each taker trade in a resolved market,
we COPY them: buy the SAME side they took, at the next trade's price (a
defensible proxy for "what could I have filled at right after seeing their
print?"). Hold to settlement.

This is the right tool for segments where takers WIN (e.g. politics /
geopolitics series where retail appears informed). fade_backtest.py can only
measure the losing fade side there; this measures what WE'd make following.

The critical realism knob is **entry slippage**: if a taker is informed, the
price runs toward their side after they trade, so our entry (next-trade price)
is worse than theirs. We report avg slippage (our entry minus taker price) so
you can see how much edge the delay eats.

Concentration audit is baked in (it's been the make-or-break filter on every
candidate): per-market top-2 P&L share, per-close-date breakdown, and ROI
after removing the top-2 markets.

Usage:
    python coat_tail_backtest.py --segment "m.series_ticker = 'KXCABOUT'" --exec-source social
    python coat_tail_backtest.py --segment "m.category IN ('Politics','Elections')" --exec-source social
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone

import db


def safe_div(num: float, den: float) -> float | None:
    return num / den if den else None


def fmt_pct(x: float | None, signed: bool = True) -> str:
    if x is None:
        return "   --  "
    return f"{x*100:+6.2f}%" if signed else f"{x*100:5.2f}%"


def fmt_money(c: float) -> str:
    return f"${c/100:>12,.0f}"


def slice_report(label: str, buckets: dict) -> None:
    print(f"\n--- split by {label} ---")
    print(f"  {'bucket':<22} {'n':>6} {'cov':>6} {'taker_roi':>10} {'coat_roi':>10} {'fee_adj':>10} {'notional':>14}")
    for key, v in sorted(buckets.items(), key=lambda kv: -kv[1]["n"]):
        if v["n"] < 20:
            continue
        cov = v.get("executed", 0) / v["n"] if v["n"] else 0
        taker_roi = v["taker_pnl"] / v["taker_notional"] if v.get("taker_notional") else 0
        coat_roi = v["coat_pnl"] / v["coat_notional"] if v.get("coat_notional") else 0
        adj_roi = v["coat_pnl_adj"] / v["coat_notional"] if v.get("coat_notional") else 0
        print(f"  {str(key):<22} {v['n']:>6} {cov:>5.0%} "
              f"{taker_roi*100:>+8.2f}%  {coat_roi*100:>+8.2f}%  {adj_roi*100:>+8.2f}%  {fmt_money(v['coat_notional'])}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-notional", type=int, default=0, help="Dollar threshold for taker trade")
    ap.add_argument("--window-sec", type=int, default=3600, help="Max seconds after taker to find our entry")
    ap.add_argument("--fee-rate", type=float, default=0.02, help="Fee on coat-tail notional")
    ap.add_argument("--exec-source", choices=["public", "social"], default="social",
                    help="Which table to look up next-trade entry prices in (social = better coverage for thin series)")
    ap.add_argument("--segment", type=str, default="",
                    help="SQL filter on taker trade, e.g. \"m.series_ticker = 'KXCABOUT'\"")
    ap.add_argument("--include-sports", action="store_true",
                    help="Override the project-wide Sports exclusion (default OFF)")
    args = ap.parse_args()
    min_notional_c = args.min_notional * 100

    con = db.connect()
    seg_clause = f"AND ({args.segment})" if args.segment else ""
    sports_clause = "" if args.include_sports else "AND (m.category IS NULL OR m.category != 'Sports')"
    seg_desc = f" matching [{args.segment}]" if args.segment else ""
    sports_desc = " (Sports INCLUDED)" if args.include_sports else " (Sports excluded)"
    print(f"Selecting taker trades >= ${args.min_notional}{seg_desc}{sports_desc} in resolved markets...")
    big = con.execute(f"""
        SELECT t.trade_id, t.ticker, t.created_ts, t.price_cents, t.count_fp,
               t.taker_side, m.result, m.category, m.close_ts
        FROM trades_social t
        JOIN markets m ON m.ticker = t.ticker
        WHERE m.result IN ('yes','no')
          AND t.price_cents * t.count_fp >= ?
          {sports_clause}
          {seg_clause}
    """, (min_notional_c,)).fetchall()
    print(f"  {len(big):,} taker trades found")

    totals = defaultdict(float)
    totals["n"] = 0
    totals["executed"] = 0
    totals["slippage_sum"] = 0.0   # sum of (our entry price - taker price), qty-weighted
    totals["slippage_qty"] = 0.0

    by_category = defaultdict(lambda: defaultdict(float))
    by_price_bucket = defaultdict(lambda: defaultdict(float))
    by_time_to_close = defaultdict(lambda: defaultdict(float))
    per_market = defaultdict(lambda: {"cost": 0.0, "pnl": 0.0, "n": 0})
    per_close_date = defaultdict(lambda: {"cost": 0.0, "pnl": 0.0, "n": 0})

    next_q = con.cursor()

    for i, r in enumerate(big):
        if i % 500 == 0 and i:
            print(f"  ...{i:,} processed")
        totals["n"] += 1

        p_t = r["price_cents"]
        if p_t < 30: price_bucket = "<30c (longshot)"
        elif p_t < 50: price_bucket = "30-50c"
        elif p_t <= 70: price_bucket = "50-70c"
        else: price_bucket = ">70c (favorite)"
        cat = r["category"] or "(none)"
        if r["close_ts"]:
            dtc = r["close_ts"] - r["created_ts"]
            if dtc < 3600: ttc_bucket = "<1h"
            elif dtc < 86400: ttc_bucket = "1-24h"
            elif dtc < 7 * 86400: ttc_bucket = "1-7d"
            else: ttc_bucket = ">7d"
        else:
            ttc_bucket = "(unknown)"

        for buckets, key in [(by_category, cat), (by_price_bucket, price_bucket), (by_time_to_close, ttc_bucket)]:
            buckets[key]["n"] = buckets[key].get("n", 0) + 1

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
        if args.exec_source == "public":
            nxt = next_q.execute("""
                SELECT yes_price_cents, no_price_cents FROM trades_public
                WHERE ticker = ? AND created_ts > ? AND created_ts <= ?
                ORDER BY created_ts LIMIT 1
            """, (r["ticker"], r["created_ts"], r["created_ts"] + args.window_sec)).fetchone()
            if nxt is None:
                continue
            next_yes, next_no = nxt["yes_price_cents"], nxt["no_price_cents"]
        else:
            nxt = next_q.execute("""
                SELECT taker_side, price_cents FROM trades_social
                WHERE ticker = ? AND created_ts > ? AND created_ts <= ? AND trade_id != ?
                ORDER BY created_ts LIMIT 1
            """, (r["ticker"], r["created_ts"], r["created_ts"] + args.window_sec, r["trade_id"])).fetchone()
            if nxt is None:
                continue
            if nxt["taker_side"] == "yes":
                next_yes = nxt["price_cents"]; next_no = 100 - nxt["price_cents"]
            else:
                next_no = nxt["price_cents"]; next_yes = 100 - nxt["price_cents"]

        # Coat-tail = SAME side as taker. If taker bought YES, we also buy YES.
        coat_side_price = next_yes if r["taker_side"] == "yes" else next_no
        if coat_side_price >= 100 or coat_side_price <= 0:
            continue  # degenerate

        totals["executed"] += 1
        for buckets, key in [(by_category, cat), (by_price_bucket, price_bucket), (by_time_to_close, ttc_bucket)]:
            buckets[key]["executed"] = buckets[key].get("executed", 0) + 1

        qty = r["count_fp"]
        coat_cost = coat_side_price * qty
        coat_won = taker_won  # same side as taker
        coat_payoff = 100 * qty if coat_won else 0
        coat_pnl = coat_payoff - coat_cost
        fee = coat_cost * args.fee_rate
        coat_pnl_adj = coat_pnl - fee

        # Slippage: how much worse is our entry than the taker's own price?
        totals["slippage_sum"] += (coat_side_price - r["price_cents"]) * qty
        totals["slippage_qty"] += qty

        totals["coat_notional"] += coat_cost
        totals["coat_pnl"] += coat_pnl
        totals["coat_pnl_adj"] += coat_pnl_adj
        for buckets, key in [(by_category, cat), (by_price_bucket, price_bucket), (by_time_to_close, ttc_bucket)]:
            buckets[key]["coat_notional"] += coat_cost
            buckets[key]["coat_pnl"] += coat_pnl
            buckets[key]["coat_pnl_adj"] += coat_pnl_adj

        per_market[r["ticker"]]["cost"] += coat_cost
        per_market[r["ticker"]]["pnl"] += coat_pnl
        per_market[r["ticker"]]["n"] += 1
        d = datetime.fromtimestamp(r["close_ts"], timezone.utc).strftime("%Y-%m-%d") if r["close_ts"] else "?"
        per_close_date[d]["cost"] += coat_cost
        per_close_date[d]["pnl"] += coat_pnl
        per_close_date[d]["n"] += 1

    # -----------------------------------------------------------------------
    print()
    print("=" * 74)
    seg_label = f"  segment=[{args.segment}]" if args.segment else ""
    print(f"COAT-TAIL  exec={args.exec_source}  window={args.window_sec}s  fee={args.fee_rate:.0%}{seg_label}")
    print("=" * 74)
    n, execd = totals["n"], totals["executed"]
    coat_notional = totals["coat_notional"]
    print(f"Taker trades:          {int(n):>6,}")
    cov = safe_div(execd, n)
    print(f"With coat-tail entry:  {int(execd):>6,}  ({cov*100:.1f}% coverage)" if cov is not None
          else f"With coat-tail entry:  {int(execd):>6,}  (no takers in segment)")
    print()
    print(f"Taker side ROI:        {fmt_pct(safe_div(totals['taker_pnl'], totals['taker_notional']))}  "
          f"pnl={fmt_money(totals['taker_pnl'])}  notional={fmt_money(totals['taker_notional'])}")
    if not coat_notional:
        print(f"Coat-tail ROI:         --   (0% entry coverage in trades_{args.exec_source} within "
              f"{args.window_sec}s -- no fills to copy into)")
        return 0
    print(f"Coat-tail ROI (raw):   {fmt_pct(safe_div(totals['coat_pnl'], coat_notional))}  "
          f"pnl={fmt_money(totals['coat_pnl'])}  notional={fmt_money(coat_notional)}")
    print(f"Coat-tail ROI (-fee):  {fmt_pct(safe_div(totals['coat_pnl_adj'], coat_notional))}  "
          f"pnl={fmt_money(totals['coat_pnl_adj'])}")
    avg_slip = safe_div(totals["slippage_sum"], totals["slippage_qty"])
    if avg_slip is not None:
        print(f"Avg entry slippage:    {avg_slip:+.2f}c per contract vs the taker's own price "
              f"({'we pay up — informed-flow signature' if avg_slip > 0.5 else 'minimal'})")

    slice_report("category", by_category)
    slice_report("taker price bucket", by_price_bucket)
    slice_report("time-to-close", by_time_to_close)

    # Fee sensitivity
    print(f"\n--- fee sensitivity (raw coat ROI is {totals['coat_pnl']/coat_notional*100:+.2f}%) ---")
    for f in [0.0, 0.01, 0.02, 0.03, 0.05, 0.08]:
        adj = (totals["coat_pnl"] - f * coat_notional) / coat_notional
        print(f"  fee={f:>4.0%}  ->  ROI={adj*100:+6.2f}%")

    # Concentration audit
    print(f"\n--- concentration audit ---")
    mkts = sorted(per_market.values(), key=lambda m: -m["pnl"])
    total_pnl = sum(m["pnl"] for m in mkts)
    print(f"  distinct markets: {len(mkts)}")
    if total_pnl > 0 and len(mkts) >= 2:
        top2 = mkts[0]["pnl"] + mkts[1]["pnl"]
        print(f"  top-2 markets drive: {top2/total_pnl*100:.1f}% of P&L")
        ex = mkts[2:]
        ex_cost = sum(m["cost"] for m in ex)
        ex_pnl = sum(m["pnl"] for m in ex)
        if ex_cost:
            print(f"  EX-top-2: notional={fmt_money(ex_cost)}  ROI={ex_pnl/ex_cost*100:+.2f}%  "
                  f"(-{args.fee_rate:.0%} fee: {(ex_pnl - ex_cost*args.fee_rate)/ex_cost*100:+.2f}%)")
    print(f"\n  per close-date:")
    pos = neg = 0
    for d in sorted(per_close_date.keys()):
        s = per_close_date[d]
        if s["cost"] < 100:
            continue
        roi = s["pnl"] / s["cost"] * 100
        if roi > 0: pos += 1
        else: neg += 1
        print(f"    {d}  n={s['n']:>4}  notional={fmt_money(s['cost'])}  ROI={roi:>+8.1f}%")
    print(f"  close-dates positive: {pos}  negative: {neg}  "
          f"-> {'PASS' if pos > neg else 'FAIL'} (majority-positive)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
