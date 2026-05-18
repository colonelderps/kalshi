"""Hang-aware auto-restarting wrapper around collect_social.py. NO console window.

Launched by Windows Task Scheduler via `pythonw.exe run_collector.pyw`.
The .pyw extension routes through pythonw.exe (windowless Python), so this
process is completely invisible -- no flashing cmd boxes.

Two failure modes this guards against:
  1. CRASH / external termination -- collect_social.py exits. We detect the
     exit and relaunch after a 30s backoff.
  2. HANG -- collect_social.py stays alive but stops making progress (seen
     2026-05-18: blocked 7h on a SQLite write lock, 1.3s CPU total). The
     old wrapper used proc.wait() which blocks forever on a hung child.
     Now we poll: if the log file hasn't grown in STALL_LIMIT seconds, the
     child is hung -> kill it -> relaunch.

Logs go to logs/social_collector_YYYY-MM-DD.log, rotated daily.
"""
from __future__ import annotations

import datetime as dt
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
LOGS = HERE / "logs"
LOGS.mkdir(exist_ok=True)

RESTART_BACKOFF = 30      # seconds to wait after a crash before relaunching
STALL_LIMIT = 300         # seconds without log growth => treat child as hung
POLL_INTERVAL = 30        # how often the wrapper checks on the child


def logfile_for_today() -> Path:
    return LOGS / f"social_collector_{dt.datetime.now():%Y-%m-%d}.log"


def write_log(msg: str) -> None:
    line = f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] [wrapper] {msg}\n"
    try:
        with logfile_for_today().open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass


def main() -> None:
    python_exe = sys.executable
    if python_exe.lower().endswith("pythonw.exe"):
        python_exe = python_exe[:-len("pythonw.exe")] + "python.exe"

    collector = HERE / "collect_social.py"
    write_log(f"wrapper starting; python={python_exe}; STALL_LIMIT={STALL_LIMIT}s")

    while True:
        logfile = logfile_for_today()
        write_log("launching collect_social.py")
        try:
            logf = logfile.open("a", encoding="utf-8")
            proc = subprocess.Popen(
                [python_exe, "-u", str(collector), "--poll-seconds", "8"],
                cwd=str(HERE),
                stdout=logf,
                stderr=subprocess.STDOUT,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
        except Exception as e:
            write_log(f"failed to launch: {e!r}; retrying in {RESTART_BACKOFF}s")
            time.sleep(RESTART_BACKOFF)
            continue

        # Supervise: watch for exit OR hang.
        last_size = -1
        last_progress = time.time()
        hung = False
        while True:
            time.sleep(POLL_INTERVAL)
            rc = proc.poll()
            if rc is not None:
                write_log(f"collector exited code {rc}")
                break
            # Child still alive -- has the log grown?
            try:
                size = logfile.stat().st_size
            except OSError:
                size = last_size
            if size != last_size:
                last_size = size
                last_progress = time.time()
            elif time.time() - last_progress > STALL_LIMIT:
                write_log(f"collector HUNG ({STALL_LIMIT}s no log growth); killing")
                try:
                    proc.kill()
                    proc.wait(timeout=15)
                except Exception as e:
                    write_log(f"kill failed: {e!r}")
                hung = True
                break

        try:
            logf.close()
        except Exception:
            pass

        delay = 5 if hung else RESTART_BACKOFF
        write_log(f"restarting in {delay}s")
        time.sleep(delay)


if __name__ == "__main__":
    main()
