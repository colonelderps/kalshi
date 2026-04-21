"""Continuous forward-tail of the Kalshi social firehose.

Each GitHub Actions run polls /v1/social/trades in a tight loop for ~4.5
minutes, then flushes new rows (by trade_id, vs the committed watermark)
to data/social/<run_ts>.jsonl.gz and exits. A 5-minute cron relaunches
the next run, giving continuous coverage.

Why not a plain cron-every-5min-with-one-poll: the v1 endpoint returns
only the 100 most-recent trades with no pagination. At current velocity
(~9 trades/sec) a 5-minute gap would drop ~96% of rows. We must poll
every ~8 seconds to stay inside the 100-row window.

Env:
  KALSHI_KEY_ID, KALSHI_PRIVATE_KEY_PEM   required in CI
  SOCIAL_POLL_SEC      poll cadence (default 8)
  SOCIAL_MAX_SECONDS   per-run wall clock (default 270 = 4m30s)
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import time

import cloud_lib

STREAM = "social"


def parse_iso(s: str) -> int:
    return int(dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())


def fetch_once() -> list[dict]:
    body = cloud_lib.get("/v1/social/trades")
    return body.get("trades", []) or []


def main() -> int:
    poll_sec = float(os.environ.get("SOCIAL_POLL_SEC", "8"))
    max_seconds = int(os.environ.get("SOCIAL_MAX_SECONDS", "270"))

    run_ts = int(time.time())
    state = cloud_lib.load_state(STREAM)
    watermark_ts = int(state.get("latest_ts", 0))
    # trade_ids we've already committed in previous runs (we only need those
    # within the overlap window; server only returns 100 most-recent anyway)
    seen_ids: set[str] = set(state.get("recent_trade_ids", []))

    print(f"tail_social: watermark={dt.datetime.fromtimestamp(watermark_ts, dt.UTC) if watermark_ts else 'bootstrap'}  "
          f"poll={poll_sec}s  budget={max_seconds}s")

    buffer: list[dict] = []
    buffer_ids: set[str] = set()
    newest = watermark_ts
    polls = 0
    errors = 0
    start = time.time()

    while (time.time() - start) < max_seconds:
        polls += 1
        try:
            trades = fetch_once()
        except Exception as e:
            errors += 1
            print(f"  poll {polls}: fetch failed ({e.__class__.__name__}: {str(e)[:120]})")
            # brief backoff then continue; don't blow the whole run for one blip
            time.sleep(min(poll_sec * 2, 30))
            continue

        batch_new = 0
        for t in trades:
            tid = t.get("trade_id")
            if not tid or tid in seen_ids or tid in buffer_ids:
                continue
            created = t.get("create_date") or t.get("created_time")
            try:
                ts = parse_iso(created) if created else 0
            except Exception:
                ts = 0
            if watermark_ts and ts and ts < watermark_ts - 3600:
                # way older than watermark — ignore (likely a stale row)
                continue
            row = {
                "trade_id": tid,
                "market_id": t.get("market_id"),
                "ticker": t.get("ticker"),
                "created_time": created,
                "created_ts": ts,
                "price_cents": int(t.get("price", 0) or 0),
                "count": int(t.get("count", 0) or 0),
                "count_fp": float(t.get("count_fp", 0) or 0),
                "taker_side": t.get("taker_side") or "",
                "maker_action": t.get("maker_action") or "",
                "taker_action": t.get("taker_action") or "",
                "maker_nickname": t.get("maker_nickname") or "",
                "taker_nickname": t.get("taker_nickname") or "",
                "maker_social_id": t.get("maker_social_id") or "",
                "taker_social_id": t.get("taker_social_id") or "",
            }
            buffer.append(row)
            buffer_ids.add(tid)
            batch_new += 1
            if ts > newest:
                newest = ts

        if polls % 10 == 0 or polls == 1:
            elapsed = time.time() - start
            print(f"  poll={polls:>3} elapsed={elapsed:6.1f}s buffer={len(buffer):>5} "
                  f"last_batch={len(trades)} new_in_batch={batch_new} errors={errors}")

        # break early if we're within one poll of the budget
        if (time.time() - start) + poll_sec >= max_seconds:
            break
        time.sleep(poll_sec)

    # Flush
    out = cloud_lib.write_jsonl_gz(STREAM, run_ts, buffer)
    # Keep a rolling window of the last 500 seen ids so the next run can dedupe
    # the first few polls without re-fetching. (Server-side dedupe is by tid.)
    combined = list(seen_ids) + [r["trade_id"] for r in buffer]
    cloud_lib.save_state(STREAM, {
        "latest_ts": newest,
        "last_run_ts": run_ts,
        "last_rows": len(buffer),
        "last_polls": polls,
        "last_errors": errors,
        "recent_trade_ids": combined[-500:],
    })
    if out:
        print(f"tail_social: wrote {len(buffer)} rows to {out.name}")
    else:
        print("tail_social: 0 new rows")
    print(f"tail_social: polls={polls}  errors={errors}  elapsed={time.time()-start:.1f}s  "
          f"new_watermark={dt.datetime.fromtimestamp(newest, dt.UTC) if newest else 'none'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
