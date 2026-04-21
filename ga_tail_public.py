"""Forward-tail the public trades firehose into gzipped JSONL.

Designed for GitHub Actions:
  * No DB. Writes data/public/<ISO>.jsonl.gz plus data/public/state.json.
  * Resumable: state.json stores the newest-trade-timestamp we've committed;
    each run pages forward (newest-first API, stop when we pass it) and
    writes only NEW rows.
  * First-ever run bootstraps by grabbing one page (1000 trades) -- local
    backfill fills history separately.

This is the forward-only twin of backfill_public.py (which walks backward).
Running both closes the gap that broke fade_backtest.

Env:
  KALSHI_KEY_ID, KALSHI_PRIVATE_KEY_PEM  (required in CI)
  KALSHI_BASE_URL                         (optional, defaults to prod)
  TAIL_MAX_PAGES    (default 30; ~30k trades worth per run)
  TAIL_MAX_SECONDS  (default 240; hard wall-clock cap so workflow can commit)
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import time

import cloud_lib

STREAM = "public"
PAGE_SIZE = 1000


def parse_iso(s: str) -> int:
    return int(dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())


def main() -> int:
    max_pages = int(os.environ.get("TAIL_MAX_PAGES", "30"))
    max_seconds = int(os.environ.get("TAIL_MAX_SECONDS", "240"))

    run_ts = int(time.time())
    state = cloud_lib.load_state(STREAM)
    # newest ts we've already committed; trades <= this are skipped
    watermark_ts = int(state.get("latest_ts", 0))

    print(f"tail_public: watermark={dt.datetime.fromtimestamp(watermark_ts, dt.UTC) if watermark_ts else 'none (bootstrap)'}")

    rows: list[dict] = []
    cursor = ""
    pages = 0
    start = time.time()
    hit_watermark = False
    newest_this_run = watermark_ts

    while pages < max_pages and (time.time() - start) < max_seconds:
        params = {"limit": PAGE_SIZE}
        if cursor:
            params["cursor"] = cursor
        body = cloud_lib.get("/trade-api/v2/markets/trades", params)
        trades = body.get("trades", [])
        if not trades:
            break

        for t in trades:
            ts = parse_iso(t["created_time"])
            if watermark_ts and ts <= watermark_ts:
                hit_watermark = True
                break
            newest_this_run = max(newest_this_run, ts)
            rows.append({
                "trade_id": t["trade_id"],
                "ticker": t["ticker"],
                "created_time": t["created_time"],
                "created_ts": ts,
                "yes_price_cents": int(round(float(t["yes_price_dollars"]) * 100)),
                "no_price_cents": int(round(float(t["no_price_dollars"]) * 100)),
                "count_fp": float(t["count_fp"]),
                "taker_side": t["taker_side"],
            })

        # Bootstrap: no prior watermark -> only grab one page so the cloud
        # doesn't re-walk history every run. Local backfill handles history.
        if not watermark_ts:
            break
        if hit_watermark:
            break
        cursor = body.get("cursor", "")
        if not cursor:
            break
        pages += 1
        time.sleep(0.1)

    out = cloud_lib.write_jsonl_gz(STREAM, run_ts, rows)
    cloud_lib.save_state(STREAM, {
        "latest_ts": newest_this_run,
        "last_run_ts": run_ts,
        "last_rows": len(rows),
    })
    if out:
        print(f"tail_public: wrote {len(rows)} rows to {out.name}")
    else:
        print("tail_public: 0 new rows")
    print(f"tail_public: pages={pages}  elapsed={time.time()-start:.1f}s  "
          f"new_watermark={dt.datetime.fromtimestamp(newest_this_run, dt.UTC) if newest_this_run else 'none'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
