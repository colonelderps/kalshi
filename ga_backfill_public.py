"""Backfill the public trades firehose backward via Kalshi's cursor. Cloud version.

Mirror of the local backfill_public.py, but:
  * State lives in data/public_backfill/state.json (not local SQLite).
  * Output is gzipped JSONL at data/public_backfill/<ISO>.jsonl.gz, to be
    merged locally by sync_from_cloud.py (which INSERT OR IGNOREs into
    trades_public, same target table as the forward tail).
  * Each run has a wall-clock budget so it can commit and exit under the
    GitHub Actions timeout.

State schema (data/public_backfill/state.json):
  cursor        : opaque Kalshi pagination cursor (resume point, walking backward)
  earliest_ts   : oldest trade ts we've seen
  latest_ts     : newest trade ts we've seen (mostly for reporting)
  finished      : True once the API returns no further cursor (end of history)
  last_run_ts   : epoch sec of last successful run

Env:
  KALSHI_KEY_ID, KALSHI_PRIVATE_KEY_PEM  (required in CI)
  BACKFILL_MAX_PAGES    (default 200)
  BACKFILL_MAX_SECONDS  (default 240)
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import time

import cloud_lib

STREAM = "public_backfill"
PAGE_SIZE = 1000


def parse_iso(s: str) -> int:
    return int(dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())


def _trade_row(t: dict) -> dict:
    return {
        "trade_id": t["trade_id"],
        "ticker": t["ticker"],
        "created_time": t["created_time"],
        "created_ts": parse_iso(t["created_time"]),
        "yes_price_cents": int(round(float(t["yes_price_dollars"]) * 100)),
        "no_price_cents": int(round(float(t["no_price_dollars"]) * 100)),
        "count_fp": float(t["count_fp"]),
        "taker_side": t["taker_side"],
    }


def main() -> int:
    max_pages = int(os.environ.get("BACKFILL_MAX_PAGES", "200"))
    max_seconds = int(os.environ.get("BACKFILL_MAX_SECONDS", "240"))

    run_ts = int(time.time())
    state = cloud_lib.load_state(STREAM)

    if state.get("finished"):
        print("backfill: already finished (state.finished=True). Exiting no-op.")
        # Touch state so commit step has something to push? No -- nothing to do.
        return 0

    cursor = state.get("cursor") or ""
    earliest = state.get("earliest_ts")
    latest = state.get("latest_ts")
    finished = False

    print(f"backfill: resume cursor={'<empty>' if not cursor else cursor[:24]+'...'}  "
          f"earliest={dt.datetime.fromtimestamp(earliest, dt.UTC) if earliest else None}")

    rows: list[dict] = []
    pages = 0
    start = time.time()
    page_earliest = None

    while pages < max_pages and (time.time() - start) < max_seconds:
        params: dict = {"limit": PAGE_SIZE}
        if cursor:
            params["cursor"] = cursor

        body = cloud_lib.get("/trade-api/v2/markets/trades", params)
        trades = body.get("trades", [])
        next_cursor = body.get("cursor", "")

        if not trades:
            print("backfill: no trades in response; treating as end of history.")
            finished = True
            break

        for t in trades:
            rows.append(_trade_row(t))

        page_earliest = parse_iso(trades[-1]["created_time"])
        page_latest = parse_iso(trades[0]["created_time"])
        earliest = min(earliest, page_earliest) if earliest else page_earliest
        latest = max(latest, page_latest) if latest else page_latest

        pages += 1

        # End-of-history detection
        if not next_cursor or next_cursor == cursor:
            print("backfill: no further cursor. End of history.")
            finished = True
            break
        cursor = next_cursor
        time.sleep(0.1)  # polite

    out = cloud_lib.write_jsonl_gz(STREAM, run_ts, rows)
    new_state = {
        "cursor": cursor if not finished else None,
        "earliest_ts": earliest,
        "latest_ts": latest,
        "finished": finished,
        "last_run_ts": run_ts,
        "last_rows": len(rows),
        # Keep seed provenance if it was present so we don't lose it on rewrite
        **({"seeded_from_local_ts": state["seeded_from_local_ts"]}
           if "seeded_from_local_ts" in state else {}),
    }
    cloud_lib.save_state(STREAM, new_state)

    oldest = dt.datetime.fromtimestamp(page_earliest, dt.UTC) if page_earliest else None
    print(f"backfill: pages={pages}  rows={len(rows)}  "
          f"oldest_this_run={oldest}  finished={finished}  "
          f"elapsed={time.time()-start:.1f}s  out={out.name if out else 'none'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
