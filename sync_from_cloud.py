"""Pull JSONL-gz blobs the cloud collectors produced and merge into master DB.

Run on the LOCAL box (same one that holds kalshi.db). Steps:
  1. git pull  -> grab any new data/<stream>/*.jsonl.gz the Actions pushed
  2. For each stream (public, social), for each file newer than our local
     watermark, read rows and do INSERT OR IGNORE into the target table.
  3. Update sync_state.json per-stream with the last ingested run_ts.

Idempotent. Safe to run as often as you like.

Usage:
    python sync_from_cloud.py             # ingest every stream
    python sync_from_cloud.py --dry-run   # report only, no writes
    python sync_from_cloud.py --no-pull   # skip git pull (useful for tests)
    python sync_from_cloud.py --stream public   # limit to one stream
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import subprocess
import sys
import time
from pathlib import Path

import db

HERE = Path(__file__).resolve().parent
SYNC_STATE = HERE / "sync_state.json"


# ---------------------------------------------------------------------------
# Per-stream ingesters. Each returns (rows_read, rows_new_inserted).
# ---------------------------------------------------------------------------


def _ingest_public(con, f: Path) -> tuple[int, int]:
    rows = []
    with gzip.open(f, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            rows.append((
                r["trade_id"], r["ticker"], r["created_time"], r["created_ts"],
                r["yes_price_cents"], r["no_price_cents"],
                r["count_fp"], r["taker_side"],
            ))
    if not rows:
        return 0, 0
    cur = con.executemany(
        "INSERT OR IGNORE INTO trades_public "
        "(trade_id, ticker, created_time, created_ts, yes_price_cents, no_price_cents, count_fp, taker_side) "
        "VALUES (?,?,?,?,?,?,?,?)",
        rows,
    )
    con.commit()
    return len(rows), cur.rowcount


def _ingest_social(con, f: Path) -> tuple[int, int]:
    rows = []
    with gzip.open(f, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            rows.append((
                r["trade_id"], r.get("market_id") or "", r["ticker"],
                r["created_time"], r["created_ts"],
                r.get("price_cents", 0), r.get("count", 0), r.get("count_fp", 0.0),
                r.get("taker_side") or "",
                r.get("maker_action") or "", r.get("taker_action") or "",
                r.get("maker_nickname") or "", r.get("taker_nickname") or "",
                r.get("maker_social_id") or "", r.get("taker_social_id") or "",
            ))
    if not rows:
        return 0, 0
    cur = con.executemany(
        "INSERT OR IGNORE INTO trades_social "
        "(trade_id, market_id, ticker, created_time, created_ts, price_cents, count, count_fp, "
        " taker_side, maker_action, taker_action, maker_nickname, taker_nickname, "
        " maker_social_id, taker_social_id) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    con.commit()
    return len(rows), cur.rowcount


# stream name -> (subdir, ingester)
# public_backfill uses the same row shape/target table as public (both feed
# trades_public); they differ only in which direction they walk (forward tail
# vs backward cursor). INSERT OR IGNORE dedupes any overlap.
STREAMS: dict[str, callable] = {
    "public": _ingest_public,
    "public_backfill": _ingest_public,
    "social": _ingest_social,
}


# ---------------------------------------------------------------------------
# State + filename helpers
# ---------------------------------------------------------------------------


def _git_pull() -> None:
    try:
        out = subprocess.run(
            ["git", "pull", "--ff-only"],
            cwd=HERE, capture_output=True, text=True, check=True,
        )
        print(out.stdout.strip() or "git pull: up to date")
    except subprocess.CalledProcessError as e:
        print(f"git pull failed:\n{e.stdout}\n{e.stderr}")
        sys.exit(1)


def _load_state() -> dict:
    if not SYNC_STATE.exists():
        return {}
    return json.loads(SYNC_STATE.read_text())


def _save_state(state: dict) -> None:
    tmp = SYNC_STATE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True))
    tmp.replace(SYNC_STATE)


def _parse_stamp(stem: str) -> int:
    """Parse 'YYYY-MM-DDTHH-MM-SSZ.jsonl' stem -> epoch seconds."""
    if stem.endswith(".jsonl"):
        stem = stem[:-len(".jsonl")]
    date_part, _, time_part = stem.partition("T")
    time_part = time_part.rstrip("Z")
    hh, mm, ss = time_part.split("-")
    iso = f"{date_part}T{hh}:{mm}:{ss}+00:00"
    return int(dt.datetime.fromisoformat(iso).timestamp())


# ---------------------------------------------------------------------------


def _sync_one_stream(con, stream: str, state: dict, dry_run: bool) -> None:
    ingester = STREAMS[stream]
    key_last = f"{stream}_last_run_ts"
    last = int(state.get(key_last, 0))

    sdir = HERE / "data" / stream
    if not sdir.exists():
        print(f"[{stream}] no dir; skip")
        return
    all_files = sorted(sdir.glob("*.jsonl.gz"))
    new_files = [f for f in all_files if _parse_stamp(f.stem) > last]

    since = (dt.datetime.fromtimestamp(last, dt.UTC).strftime("%Y-%m-%d %H:%M UTC")
             if last else "epoch")
    print(f"[{stream}] {len(all_files)} files on disk, {len(new_files)} new since {since}")

    if not new_files:
        return
    if dry_run:
        for f in new_files:
            print(f"  DRY: would ingest {f.name}")
        return

    total_rows = 0
    total_new = 0
    for f in new_files:
        rows_read, rows_new = ingester(con, f)
        total_rows += rows_read
        total_new += rows_new
        print(f"  {f.name:<40} rows={rows_read:>5}  new={rows_new:>5}")

    print(f"[{stream}] {total_rows} rows read, {total_new} new rows inserted")

    # Advance watermark to newest file's run_ts
    newest = max(_parse_stamp(f.stem) for f in new_files)
    state[key_last] = newest
    state[f"{stream}_last_sync_ts"] = int(time.time())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-pull", action="store_true")
    ap.add_argument("--stream", choices=list(STREAMS.keys()), default=None,
                    help="Only ingest this stream (default: all)")
    args = ap.parse_args()

    if not args.no_pull:
        _git_pull()

    state = _load_state()

    streams_to_run = [args.stream] if args.stream else list(STREAMS.keys())
    con = None if args.dry_run else db.connect()

    for s in streams_to_run:
        _sync_one_stream(con, s, state, args.dry_run)

    if not args.dry_run:
        _save_state(state)
    return 0


if __name__ == "__main__":
    sys.exit(main())
