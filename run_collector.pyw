"""Auto-restarting wrapper around collect_social.py with NO console window.

Launched by Windows Task Scheduler via `pythonw.exe run_collector.pyw`.
The .pyw extension routes through pythonw.exe (windowless Python), so this
process is completely invisible -- no flashing cmd boxes, no visible console.

If the underlying collect_social.py crashes (network error, Kalshi 5xx,
SQLite lock, anything), we wait 30 seconds and restart. Logs go to
logs/social_collector_YYYY-MM-DD.log, rotated daily.

Replaces the previous collect_social_continuous.bat which spawned visible
cmd windows.
"""
from __future__ import annotations

import datetime as dt
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
LOGS = HERE / "logs"
LOGS.mkdir(exist_ok=True)


def logfile_for_today() -> Path:
    return LOGS / f"social_collector_{dt.datetime.now():%Y-%m-%d}.log"


def write_log(msg: str) -> None:
    line = f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}\n"
    try:
        with logfile_for_today().open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass  # If the log file is locked or unwritable, swallow it -- the show must go on.


def main() -> None:
    # Use the same Python that's running us (handles venvs cleanly).
    python_exe = sys.executable
    # Force regular python.exe (not pythonw.exe) for the child so stdio works.
    if python_exe.lower().endswith("pythonw.exe"):
        python_exe = python_exe[:-len("pythonw.exe")] + "python.exe"

    collector = HERE / "collect_social.py"

    write_log(f"wrapper starting; python={python_exe}; collector={collector}")

    backoff = 30  # seconds between restarts

    while True:
        logfile = logfile_for_today()
        write_log("launching collect_social.py")
        try:
            with logfile.open("a", encoding="utf-8") as logf:
                proc = subprocess.Popen(
                    [python_exe, "-u", str(collector), "--poll-seconds", "8"],
                    cwd=str(HERE),
                    stdout=logf,
                    stderr=subprocess.STDOUT,
                    creationflags=subprocess.CREATE_NO_WINDOW,
                )
                rc = proc.wait()
            write_log(f"collector exited with code {rc}; restarting in {backoff}s")
        except Exception as e:
            write_log(f"failed to launch collector: {e!r}; retrying in {backoff}s")

        time.sleep(backoff)


if __name__ == "__main__":
    main()
