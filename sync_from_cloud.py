"""Pull JSONL-gz blobs the cloud collectors produced and merge into master DB.

Run on the LOCAL box (same one that holds kalshi.db). Steps:
  1. git pull  -> grab any new data/public/*.jsonl.gz the Action pushed
  2. For each file newer than our local sync watermark, read rows and do
     INSERT OR IGNORE into trades_public (schema matches ga_tail_public output).
  3. Update sync_state.json with last ingested run_ts.

Idempotent. Safe to run as often as you like.

Usage:
    python sync_from_cloud.py             # default: ingest data/public/
    python sync_from_cloud.py --dry-run   # report only, no writes
    python sync_from_cloud.py --no-pull   # skip git pull (useful for tests)
"""
from __future__ import annotations

import argparse
import gzip
import json
import subprocess
import sys
import time
from pathlib import Path

import db

HERE = Path(__file__).resolve().parent
SYNC_STATE = HERE / "sync_state.json"


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


def _ingest_public(con, files: list[Path]) -> tuple[int, int]:
    total_rows = 0
    total_inserted = 0
    for f in files:
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
        total_rows += len(rows)
        if not rows:
            continue
        cur = con.executemany(
            "INSERT OR IGNORE INTO trades_public "
            "(trade_id, ticker, created_time, created_ts, yes_price_cents, no_price_cents, count_fp, taker_side) "
            "VALUES (?,?,?,?,?,?,?,?)",
            rows,
        )
        total_inserted += cur.rowcount
        con.commit()
        print(f"  {f.name:<40} rows={len(rows):>5}  new={cur.rowcount:>5}")
    return total_rows, total_inserted


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-pull", action="store_true")
    args = ap.parse_args()

    if not args.no_pull:
        _git_pull()

    state = _load_state()
    last_ingested = int(state.get("public_last_run_ts", 0))

    public_dir = HERE / "data" / "public"
    all_files = sorted(public_dir.glob("*.jsonl.gz")) if public_dir.exists() else []
    # File name is the run_ts ISO stamp; use mtime-independent ordering via name
    new_files = [f for f in all_files if _parse_stamp(f.stem) > last_ingested]

    print(f"sync_from_cloud: {len(all_files)} files on disk, {len(new_files)} new since "
          f"{last_ingested} ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(last_ingested)) if last_ingested else 'epoch'})")

    if not new_files:
        return 0

    if args.dry_run:
        for f in new_files:
            print(f"  DRY: would ingest {f.name}")
        return 0

    con = db.connect()
    total_rows, total_inserted = _ingest_public(con, new_files)
    print(f"sync_from_cloud: {total_rows} total rows read, {total_inserted} new rows inserted")

    # Advance watermark to newest file's run_ts
    newest = max(_parse_stamp(f.stem) for f in new_files)
    state["public_last_run_ts"] = newest
    state["public_last_sync_ts"] = int(time.time())
    _save_state(state)
    return 0


def _parse_stamp(stem: str) -> int:
    """Parse 'YYYY-MM-DDTHH-MM-SSZ.jsonl' stem -> epoch seconds."""
    # stem is e.g. "2026-04-21T18-30-00Z.jsonl"  (after .jsonl.gz -> .jsonl stem)
    # strip '.jsonl' suffix first
    if stem.endswith(".jsonl"):
        stem = stem[:-len(".jsonl")]
    import datetime as dt
    # Replace the hyphens in the time portion back to colons for fromisoformat
    date_part, _, time_part = stem.partition("T")
    time_part = time_part.rstrip("Z")
    hh, mm, ss = time_part.split("-")
    iso = f"{date_part}T{hh}:{mm}:{ss}+00:00"
    return int(dt.datetime.fromisoformat(iso).timestamp())


if __name__ == "__main__":
    sys.exit(main())
