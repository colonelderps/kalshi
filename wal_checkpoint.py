"""Force a full WAL checkpoint and truncate the WAL file.

Run this when data/kalshi.db-wal has grown large (it should be a few MB; if
it's gigabytes, a reader has been blocking the passive auto-checkpoint).

    python wal_checkpoint.py

Safe to run anytime, but works best when no other process holds the DB
(stop the collector first for a guaranteed full truncate). A TRUNCATE
checkpoint that can't fully complete will still flush what it can.

Can be scheduled (e.g. hourly) as a maintenance task to keep the WAL bounded.
"""
from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
DB = HERE / "data" / "kalshi.db"
WAL = HERE / "data" / "kalshi.db-wal"


def wal_mb() -> float:
    return WAL.stat().st_size / 1024 / 1024 if WAL.exists() else 0.0


def main() -> int:
    before = wal_mb()
    print(f"WAL before: {before:,.1f} MB")

    con = sqlite3.connect(DB, timeout=120)
    con.execute("PRAGMA busy_timeout = 120000")
    # Make sure future passive checkpoints fire often (default 1000 pages ~4MB).
    con.execute("PRAGMA wal_autocheckpoint = 1000")

    t0 = time.time()
    # (busy_frames, log_frames, checkpointed_frames)
    row = con.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    con.commit()
    con.close()
    dt = time.time() - t0

    after = wal_mb()
    print(f"checkpoint result (busy, log, checkpointed): {row}")
    print(f"WAL after:  {after:,.1f} MB   (reclaimed {before - after:,.1f} MB in {dt:.1f}s)")
    if row and row[0] == 1:
        print("NOTE: busy=1 -> a reader blocked full truncate. Stop the collector and re-run.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
