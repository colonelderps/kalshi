"""Paper-trade tracker for Kalshi strategy candidates.

Log paper positions when a candidate strategy fires, auto-resolve them against
the `markets` table once the market settles, and track running post-fee P&L
per strategy. This turns "the backtest says +X%" into "our live paper book is
+Y% over N real trades" -- the validation step before risking real capital.

The pattern (log -> resolve -> P&L) is lifted from Marcus's Bender sports
tracker; ported here to Kalshi's yes/no contract model and our `markets`
settlement table. No credits/parlays/multi-user -- just clean strategy P&L.

Prices are in cents (0-100), the Kalshi convention. A contract pays 100 if its
side wins, 0 if it loses. Post-fee P&L subtracts fee_rate * entry_notional.

Usage:
    # log a paper entry when a candidate fires
    python paper_trades.py log --strategy politics_coattail \
        --ticker KXGOVSHUTDOWN-26 --side yes --price 62 --qty 100 --note "copied taker warm.slope"

    python paper_trades.py resolve          # settle pending trades whose markets have resolved
    python paper_trades.py report           # running P&L per strategy
    python paper_trades.py list             # all trades (or --status open|won|lost|void)
"""
from __future__ import annotations

import argparse
import time

import db

FEE_RATE_DEFAULT = 0.02

SCHEMA = """
CREATE TABLE IF NOT EXISTS paper_trades (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy          TEXT    NOT NULL,           -- e.g. politics_coattail | gas_nearclose
    ticker            TEXT    NOT NULL,           -- Kalshi market ticker
    side              TEXT    NOT NULL,           -- yes | no  (the side WE hold)
    entry_price_cents INTEGER NOT NULL,           -- what we "paid" per contract
    qty               REAL    NOT NULL,           -- contracts
    fee_rate          REAL    DEFAULT 0.02,
    note              TEXT,
    status            TEXT    DEFAULT 'open',      -- open | won | lost | void
    result            TEXT,                        -- filled at resolution: yes | no
    pnl_cents         REAL,                        -- post-fee, filled at resolution
    created_ts        INTEGER NOT NULL,
    resolved_ts       INTEGER
);
"""


def connect():
    con = db.connect()
    con.executescript(SCHEMA)
    return con


def cmd_log(con, args) -> int:
    side = args.side.lower()
    if side not in ("yes", "no"):
        print("side must be 'yes' or 'no'")
        return 1
    if not (0 < args.price < 100):
        print("price must be in cents, 1-99")
        return 1
    con.execute(
        "INSERT INTO paper_trades (strategy, ticker, side, entry_price_cents, qty, fee_rate, note, created_ts) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (args.strategy, args.ticker, side, args.price, args.qty, args.fee_rate, args.note, int(time.time())),
    )
    con.commit()
    tid = con.execute("SELECT last_insert_rowid()").fetchone()[0]
    cost = args.price * args.qty / 100
    print(f"Paper trade #{tid} logged: [{args.strategy}] {args.ticker} {side.upper()} "
          f"@ {args.price}c x {args.qty:g}  (entry cost ${cost:,.2f})")
    return 0


def cmd_resolve(con, args) -> int:
    open_trades = con.execute("SELECT * FROM paper_trades WHERE status='open'").fetchall()
    if not open_trades:
        print("No open paper trades to resolve.")
        return 0
    n_res = 0
    now = int(time.time())
    for t in open_trades:
        m = con.execute(
            "SELECT result, settlement_value FROM markets WHERE ticker=?", (t["ticker"],)
        ).fetchone()
        if m is None or m["result"] not in ("yes", "no"):
            continue  # market not settled yet (or unknown ticker) -> leave open
        won = (t["side"] == m["result"])
        payoff = 100 * t["qty"] if won else 0
        cost = t["entry_price_cents"] * t["qty"]
        fee = cost * (t["fee_rate"] if t["fee_rate"] is not None else FEE_RATE_DEFAULT)
        pnl = payoff - cost - fee
        status = "won" if won else "lost"
        con.execute(
            "UPDATE paper_trades SET status=?, result=?, pnl_cents=?, resolved_ts=? WHERE id=?",
            (status, m["result"], pnl, now, t["id"]),
        )
        n_res += 1
        print(f"  #{t['id']} [{t['strategy']}] {t['ticker']} {t['side'].upper()} -> "
              f"{m['result'].upper()} = {status.upper()}  pnl=${pnl/100:+,.2f}")
    con.commit()
    print(f"Resolved {n_res} trade(s); {len(open_trades)-n_res} still open (markets not settled).")
    return 0


def cmd_report(con, args) -> int:
    rows = con.execute("""
        SELECT strategy,
               COUNT(*)                                            AS n,
               SUM(status='open')                                 AS open,
               SUM(status='won')                                  AS won,
               SUM(status='lost')                                 AS lost,
               SUM(CASE WHEN status IN ('won','lost') THEN pnl_cents ELSE 0 END)              AS pnl,
               SUM(CASE WHEN status IN ('won','lost') THEN entry_price_cents*qty ELSE 0 END)  AS cost
        FROM paper_trades
        GROUP BY strategy
        ORDER BY pnl DESC
    """).fetchall()
    if not rows:
        print("No paper trades logged yet.")
        return 0
    print(f"{'strategy':<24} {'settled':>8} {'W-L':>9} {'win%':>6} {'P&L':>12} {'ROI':>9}")
    print("-" * 72)
    tot_pnl = tot_cost = 0.0
    for r in rows:
        settled = (r["won"] or 0) + (r["lost"] or 0)
        winpct = (r["won"] / settled * 100) if settled else 0
        roi = (r["pnl"] / r["cost"] * 100) if r["cost"] else 0
        tot_pnl += r["pnl"] or 0
        tot_cost += r["cost"] or 0
        openpart = f"  (+{r['open']} open)" if r["open"] else ""
        print(f"{r['strategy']:<24} {settled:>8} {int(r['won'] or 0):>4}-{int(r['lost'] or 0):<4} "
              f"{winpct:>5.0f}% ${(r['pnl'] or 0)/100:>+10,.2f} {roi:>+8.2f}%{openpart}")
    print("-" * 72)
    tot_roi = (tot_pnl / tot_cost * 100) if tot_cost else 0
    print(f"{'TOTAL':<24} {'':>8} {'':>9} {'':>6} ${tot_pnl/100:>+10,.2f} {tot_roi:>+8.2f}%")
    return 0


def cmd_list(con, args) -> int:
    q = "SELECT * FROM paper_trades"
    params = ()
    if args.status:
        q += " WHERE status=?"
        params = (args.status,)
    q += " ORDER BY created_ts DESC"
    rows = con.execute(q, params).fetchall()
    if not rows:
        print("No matching paper trades.")
        return 0
    print(f"{'id':>4} {'strategy':<20} {'ticker':<28} {'side':<4} {'entry':>6} {'qty':>7} {'status':<7} {'pnl':>10}")
    for r in rows:
        pnl = f"${r['pnl_cents']/100:+,.2f}" if r["pnl_cents"] is not None else "--"
        print(f"{r['id']:>4} {r['strategy'][:20]:<20} {r['ticker'][:28]:<28} {r['side']:<4} "
              f"{r['entry_price_cents']:>5}c {r['qty']:>7g} {r['status']:<7} {pnl:>10}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Paper-trade tracker for Kalshi strategy candidates")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("log", help="log a paper entry")
    p.add_argument("--strategy", required=True)
    p.add_argument("--ticker", required=True)
    p.add_argument("--side", required=True, help="yes | no (side we hold)")
    p.add_argument("--price", type=int, required=True, help="entry price in cents (1-99)")
    p.add_argument("--qty", type=float, required=True, help="contracts")
    p.add_argument("--fee-rate", type=float, default=FEE_RATE_DEFAULT)
    p.add_argument("--note", default="")

    sub.add_parser("resolve", help="settle pending trades against the markets table")
    sub.add_parser("report", help="running P&L per strategy")

    p = sub.add_parser("list", help="list trades")
    p.add_argument("--status", choices=["open", "won", "lost", "void"], default=None)

    args = ap.parse_args()
    con = connect()
    return {
        "log": cmd_log,
        "resolve": cmd_resolve,
        "report": cmd_report,
        "list": cmd_list,
    }[args.cmd](con, args)


if __name__ == "__main__":
    raise SystemExit(main())
