"""Long-running wrapper: re-runs enrich_markets.py every SLEEP_SECONDS.

Refreshes stale (>4h) markets so `result` gets populated as they settle.
Safe to Ctrl-C; the inner enrich_markets.py is safe to restart.
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
SLEEP_SECONDS = 300  # 5 min between cycles


def main() -> int:
    while True:
        print(f"[{time.strftime('%H:%M:%S')}] enrich cycle starting", flush=True)
        r = subprocess.run(
            [sys.executable, "-u", str(HERE / "enrich_markets.py"), "--stale-hours", "4"],
            cwd=HERE,
        )
        print(f"[{time.strftime('%H:%M:%S')}] cycle exit={r.returncode}. Sleeping {SLEEP_SECONDS}s.", flush=True)
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(0)
