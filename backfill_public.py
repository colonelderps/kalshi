"""Backfill the public anonymized trade firehose via /trade-api/v2/markets/trades.

Walks backward in time using the API's cursor, inserting into trades_public.
Resumable: stores cursor + timestamps in backfill_state. Safe to Ctrl-C.

Usage:
    python backfill_public.py              # resume from last cursor
    python backfill_public.py --fresh      # start from newest
    python backfill_public.py --stop-ts 1704067200  # stop when we reach this ts
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

STREAM = "public_trades"
PAGE = 1000
# No minimum notional -- we now capture every trade so we can mine for complex
# patterns later (whales vs minnows, behavioral features, etc). Earlier rows in
# the table were inserted under a $200 filter; newer pages include everything.
# Set via CLI with --min-notional if you want to filter again.
SLEEP_BETWEEN_REQUESTS = 0.1  # polite 10 req/sec


def parse_iso(s: str) -> int:
    return int(dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())


def notional_cents(t: dict) -> int:
    # Taker's dollar exposure: taker-side price x count.
    price_cents = int(round(float(t["yes_price_dollars" if t["taker_side"] == "yes" else "no_price_dollars"]) * 100))
    return int(round(price_cents * float(t["count_fp"])))


def insert_trades(con, trades: list[dict], min_notional_cents: int = 0) -> tuple[int, int]:
    """Returns (seen_count, inserted_count). Inserts every trade unless min_notional_cents set."""
    if not trades:
        return 0, 0
    rows = []
    for t in trades:
        if min_notional_cents and notional_cents(t) < min_notional_cents:
            continue
        rows.append((
            t["trade_id"], t["ticker"], t["created_time"], parse_iso(t["created_time"]),
            int(round(float(t["yes_price_dollars"]) * 100)),
            int(round(float(t["no_price_dollars"]) * 100)),
            float(t["count_fp"]),
            t["taker_side"],
        ))
    if not rows:
        return len(trades), 0
    cur = con.executemany(
        "INSERT OR IGNORE INTO trades_public "
        "(trade_id, ticker, created_time, created_ts, yes_price_cents, no_price_cents, count_fp, taker_side) "
        "VALUES (?,?,?,?,?,?,?,?)",
        rows,
    )
    return len(trades), cur.rowcount


def save_state(con, cursor: str | None, earliest_ts: int | None, latest_ts: int | None) -> None:
    con.execute(
        "INSERT OR REPLACE INTO backfill_state (stream, cursor, earliest_ts, latest_ts, updated_ts) "
        "VALUES (?, ?, ?, ?, ?)",
        (STREAM, cursor, earliest_ts, latest_ts, int(time.time())),
    )
    con.commit()


def load_state(con) -> dict[str, Any]:
    row = con.execute("SELECT * FROM backfill_state WHERE stream = ?", (STREAM,)).fetchone()
    return dict(row) if row else {}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fresh", action="store_true", help="Ignore saved cursor")
    ap.add_argument("--stop-ts", type=int, default=0, help="Stop once we reach this epoch ts (go no older)")
    ap.add_argument("--max-pages", type=int, default=1000000, help="Safety cap on number of API calls")
    ap.add_argument("--min-notional", type=int, default=0,
                    help="Dollar floor on taker notional to insert (0 = keep all)")
    args = ap.parse_args()
    min_notional_cents = args.min_notional * 100

    con = db.connect()
    state = {} if args.fresh else load_state(con)
    cursor = state.get("cursor") or ""
    earliest = state.get("earliest_ts")
    latest = state.get("latest_ts")
    total_inserted = 0

    print(f"Starting backfill. resume_cursor={'<empty>' if not cursor else cursor[:24]+'...'} "
          f"earliest_seen={dt.datetime.fromtimestamp(earliest, dt.UTC) if earliest else None}")

    page = 0
    while page < args.max_pages:
        params: dict[str, Any] = {"limit": PAGE}
        if cursor:
            params["cursor"] = cursor
        try:
            body = client.get("/trade-api/v2/markets/trades", params)
        except Exception as e:
            # Transient network/DNS/rate-limit after client's own retries exhausted.
            # Save state, back off, and try again from the same cursor.
            save_state(con, cursor, earliest, latest)
            wait = 30
            print(f"Request failed (page={page}): {e.__class__.__name__}: {str(e)[:180]}")
            print(f"Backing off {wait}s then retrying from same cursor...")
            time.sleep(wait)
            continue

        trades = body.get("trades", [])
        next_cursor = body.get("cursor", "")
        if not trades:
            print("No trades in response. Stopping.")
            break

        # Retry the insert/state-save on SQLite "database is locked": another writer
        # (enrich loop / social collector) is mid-transaction. Busy_timeout handles most
        # contention; this loop handles the worst-case case where we still time out.
        for attempt in range(6):
            try:
                seen, inserted = insert_trades(con, trades, min_notional_cents)
                total_inserted += inserted
                page_earliest = parse_iso(trades[-1]["created_time"])
                page_latest = parse_iso(trades[0]["created_time"])
                earliest = min(earliest, page_earliest) if earliest else page_earliest
                latest = max(latest, page_latest) if latest else page_latest
                con.commit()
                save_state(con, next_cursor, earliest, latest)
                break
            except sqlite3.OperationalError as e:
                if "locked" not in str(e).lower() or attempt == 5:
                    raise
                backoff = 5 * (attempt + 1)
                print(f"DB locked on page {page} (attempt {attempt+1}/6). Waiting {backoff}s...")
                time.sleep(backoff)

        if page % 20 == 0:
            oldest_str = dt.datetime.fromtimestamp(page_earliest, dt.UTC).strftime("%Y-%m-%d %H:%M")
            print(f"page={page:4d}  seen={seen:4d}  new={inserted:4d}  "
                  f"oldest={oldest_str}  total_new={total_inserted}")

        if args.stop_ts and page_earliest <= args.stop_ts:
            print(f"Reached stop_ts. Done.")
            break
        if not next_cursor or next_cursor == cursor:
            print("No further cursor. End of history.")
            break
        cursor = next_cursor
        page += 1
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    print(f"Total new rows: {total_inserted}")
    print(f"Earliest ts seen: {dt.datetime.fromtimestamp(earliest, dt.UTC) if earliest else None}")
    print(f"Latest ts seen:   {dt.datetime.fromtimestamp(latest, dt.UTC) if latest else None}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
