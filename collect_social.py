"""Forward-only collector for the social firehose (/v1/social/trades).

The social endpoint returns real-time trades with maker/taker nicknames + social_ids
for users who've opted into Kalshi Ideas. There's no historical pagination -- we must
collect from "now" forward, so this is meant to run continuously (e.g. screen/tmux).

Deduplicates on trade_id. Safe to Ctrl-C and restart.

Usage:
    python collect_social.py                    # poll every 5 seconds forever
    python collect_social.py --poll-seconds 3   # faster polling
    python collect_social.py --once             # single fetch, for testing
"""
from __future__ import annotations

import argparse
import datetime as dt
import sqlite3
import sys
import time
from typing import Any

import client
import db

STREAM = "social_trades_forward"


def parse_iso(s: str) -> int:
    return int(dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())


def insert_batch(con, trades: list[dict]) -> int:
    if not trades:
        return 0
    rows = []
    for t in trades:
        created = t.get("create_date") or t.get("created_time")
        rows.append((
            t["trade_id"],
            t.get("market_id"),
            t["ticker"],
            created,
            parse_iso(created),
            int(t.get("price", 0)),              # cents
            int(t.get("count", 0)),              # integer contracts
            float(t.get("count_fp", 0) or 0),    # fp contracts (string in v1)
            t.get("taker_side") or "",
            t.get("maker_action") or "",
            t.get("taker_action") or "",
            (t.get("maker_nickname") or ""),
            (t.get("taker_nickname") or ""),
            (t.get("maker_social_id") or ""),
            (t.get("taker_social_id") or ""),
        ))
    for attempt in range(6):
        try:
            cur = con.executemany(
                "INSERT OR IGNORE INTO trades_social "
                "(trade_id, market_id, ticker, created_time, created_ts, price_cents, count, count_fp, "
                " taker_side, maker_action, taker_action, maker_nickname, taker_nickname, "
                " maker_social_id, taker_social_id) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rows,
            )
            con.commit()
            return cur.rowcount
        except sqlite3.OperationalError as e:
            if "locked" not in str(e).lower() or attempt == 5:
                raise
            backoff = 5 * (attempt + 1)
            print(f"DB locked in social insert (attempt {attempt+1}/6). Waiting {backoff}s...")
            time.sleep(backoff)
    return 0


def fetch_once() -> list[dict]:
    # v1 endpoint ignores pagination and filter params; it returns the most recent page.
    body = client.get("/v1/social/trades")
    return body.get("trades", []) or []


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--poll-seconds", type=float, default=5.0)
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    con = db.connect()
    total_inserted = 0
    polls = 0

    print(f"Starting social collector. poll={args.poll_seconds}s  db={db.DB_PATH}")
    while True:
        try:
            trades = fetch_once()
        except Exception as e:
            print(f"[{dt.datetime.now():%H:%M:%S}] fetch failed: {e}")
            if args.once:
                return 1
            time.sleep(args.poll_seconds * 2)
            continue

        new = insert_batch(con, trades)
        total_inserted += new
        polls += 1

        named = sum(1 for t in trades if t.get("taker_social_id") or t.get("maker_social_id"))
        if polls % 10 == 0 or args.once:
            print(f"[{dt.datetime.now():%H:%M:%S}] poll={polls} batch={len(trades)} "
                  f"new={new} named={named} total_new={total_inserted}")

        if args.once:
            break
        time.sleep(args.poll_seconds)
    return 0


if __name__ == "__main__":
    sys.exit(main())
