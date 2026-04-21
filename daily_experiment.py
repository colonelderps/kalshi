"""Daily freakonomics-hunter: pick novel hypotheses and log results.

Called once a day by Windows Task Scheduler (see register_daily_task.ps1).

Each run:
  1. Reads the generator library from experiments.py
  2. Picks up to N hypotheses we haven't tested yet (or haven't tested in a
     long time -- the "re-check under new data" refresh).
  3. Runs each as a two-sample segment-vs-complement test.
  4. Stores every result in `experiments` so future pair-combination analysis
     can mine them.
  5. On days that are multiples of 7 since the first run, also invokes
     combine_experiments.run_pairs() to see if any past hypotheses combine
     into a stronger signal.

Usage:
    python daily_experiment.py                 # default: 5 new hypotheses
    python daily_experiment.py --n 10          # pick more
    python daily_experiment.py --refresh-days 30  # re-run experiments older than 30d
    python daily_experiment.py --force-combine    # always run combination pass
    python daily_experiment.py --dry-run       # print plan, don't write
"""
from __future__ import annotations

import argparse
import datetime as dt
import random
import sys
import time

import db
from experiments import GENERATORS, run_experiment


def _pick_hypotheses(con, n: int, refresh_days: int) -> list[dict]:
    """Pick n specs: prioritise never-tested, then oldest tests beyond refresh_days."""
    tested = con.execute(
        "SELECT hypothesis_key, MAX(run_ts) AS last_ts FROM experiments "
        "WHERE status != 'error' GROUP BY hypothesis_key"
    ).fetchall()
    last_run: dict[str, int] = {r["hypothesis_key"]: r["last_ts"] for r in tested}

    now = int(time.time())
    refresh_cutoff = now - refresh_days * 86400

    never = [g for g in GENERATORS if g["key"] not in last_run]
    stale = [g for g in GENERATORS if last_run.get(g["key"], 0) < refresh_cutoff and g["key"] in last_run]

    random.shuffle(never)
    stale.sort(key=lambda g: last_run.get(g["key"], 0))  # oldest first

    plan = (never + stale)[:n]
    return plan


def _store(con, spec: dict, result, run_date: str, run_ts: int) -> int:
    cur = con.execute(
        """
        INSERT INTO experiments
        (run_date, run_ts, hypothesis_key, hypothesis, unit, metric,
         segment_expr, segment_size, baseline_size, segment_value,
         baseline_value, effect_size, p_value, status, error_msg, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_date, run_ts, spec["key"], spec["hypothesis"],
            spec["unit"], spec["metric"], spec["segment_expr"],
            result.segment_size, result.baseline_size,
            result.segment_value, result.baseline_value,
            result.effect_size, result.p_value,
            result.status, result.error_msg, spec.get("notes"),
        ),
    )
    con.commit()
    return cur.lastrowid


def _format_value(v: float | None, metric: str) -> str:
    if v is None:
        return "  --"
    if metric == "roi":
        # effect is in ROI units (fraction of notional). Display as %.
        return f"{v * 100:+6.2f}%"
    return f"{v * 100:5.2f}%"


def _print_summary(spec: dict, result) -> None:
    status = result.status
    if status == "success":
        eff = _format_value(result.effect_size, spec["metric"])
        seg = _format_value(result.segment_value, spec["metric"])
        base = _format_value(result.baseline_value, spec["metric"])
        p = f"{result.p_value:.3g}" if result.p_value is not None else "  --"
        flag = ""
        if result.p_value is not None and result.p_value < 0.05 and abs(result.effect_size or 0) > 0.01:
            flag = "  <-- candidate"
        print(f"  [{spec['key']}]")
        print(f"    {spec['hypothesis']}")
        print(f"    n_seg={result.segment_size:>7}  n_base={result.baseline_size:>7}  "
              f"seg={seg}  base={base}  effect={eff}  p={p}{flag}")
    elif status == "insufficient_data":
        print(f"  [{spec['key']}] insufficient_data  n_seg={result.segment_size}  n_base={result.baseline_size}")
    else:
        print(f"  [{spec['key']}] ERROR: {result.error_msg}")


def run_daily(n: int, refresh_days: int, dry_run: bool, run_combine: bool) -> int:
    con = db.connect()
    run_ts = int(time.time())
    run_date = dt.datetime.fromtimestamp(run_ts, dt.UTC).strftime("%Y-%m-%d")

    plan = _pick_hypotheses(con, n, refresh_days)
    if not plan:
        print("No new or stale hypotheses to run today. Library exhausted at this refresh cadence.")
    else:
        print(f"[{run_date}] running {len(plan)} hypothesis test(s):")

    candidates = 0
    for spec in plan:
        result = run_experiment(con, spec)
        _print_summary(spec, result)
        if not dry_run:
            _store(con, spec, result, run_date, run_ts)
        if (result.status == "success" and result.p_value is not None
                and result.p_value < 0.05 and abs(result.effect_size or 0) > 0.01):
            candidates += 1

    # Weekly combination pass: every 7 calendar days since first experiment.
    first = con.execute("SELECT MIN(run_ts) AS t FROM experiments WHERE status='success'").fetchone()
    days_in = 0
    if first and first["t"]:
        days_in = (run_ts - first["t"]) // 86400
    should_combine = run_combine or (days_in > 0 and days_in % 7 == 0)
    if should_combine and not dry_run:
        try:
            import combine_experiments
            print()
            combine_experiments.run_pairs(con, run_date=run_date, run_ts=run_ts)
        except Exception as e:  # noqa: BLE001
            print(f"combine step failed: {type(e).__name__}: {e}")

    print()
    print(f"Done. candidates (p<0.05 & |effect|>1%): {candidates}/{len(plan)}")
    if dry_run:
        print("(dry-run: nothing written)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=5, help="How many hypotheses to run today")
    ap.add_argument("--refresh-days", type=int, default=60,
                    help="Re-run known hypotheses older than this many days")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force-combine", action="store_true",
                    help="Always run the pair-interaction pass (otherwise only weekly)")
    args = ap.parse_args()
    return run_daily(args.n, args.refresh_days, args.dry_run, args.force_combine)


if __name__ == "__main__":
    sys.exit(main())
