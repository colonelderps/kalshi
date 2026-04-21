"""Pair-interaction miner.

After we've logged a bunch of individual hypotheses, look for pairs whose
combination is stronger than either alone -- the "lucky relationship"
hunt from the original brief.

Criteria for considering a past experiment a pair candidate:
  * status = 'success'
  * |effect_size| >= MIN_EFFECT (default 0.005, i.e. 0.5 ROI points or WR points)
  * p_value <= MAX_P (default 0.2 -- loose; pair test will re-adjudicate)
  * segment_size >= MIN_SEG
  * we take the most-recent row per hypothesis_key, so the segment_expr is fresh

For each pair (A, B) with the same unit+metric, we build the combined
segment (A.segment_expr AND B.segment_expr) and run it via the same
experiments.run_experiment harness (with a synthetic spec), then log the
result into `experiment_pairs` together with A and B's most-recent values
for easy side-by-side comparison.

Usage:
    python combine_experiments.py
    python combine_experiments.py --top 20     # only the 20 strongest bases
    python combine_experiments.py --min-effect 0.01
"""
from __future__ import annotations

import argparse
import datetime as dt
import itertools
import sys
import time

import db
from experiments import run_experiment


MIN_EFFECT = 0.005
MAX_P = 0.2
MIN_SEG = 100
MAX_PAIRS_PER_RUN = 200  # safety cap so we don't do N^2 on a huge library


def _pick_candidates(con, top: int, min_effect: float, max_p: float, min_seg: int) -> list:
    """Latest row per hypothesis_key that clears the filters, ranked by |effect|."""
    rows = con.execute(
        """
        SELECT e.*
        FROM experiments e
        JOIN (
            SELECT hypothesis_key, MAX(run_ts) AS t
            FROM experiments WHERE status='success'
            GROUP BY hypothesis_key
        ) latest
          ON latest.hypothesis_key = e.hypothesis_key AND latest.t = e.run_ts
        WHERE e.status='success'
          AND e.segment_size >= :min_seg
          AND e.baseline_size >= :min_seg
          AND ABS(COALESCE(e.effect_size, 0)) >= :min_effect
          AND COALESCE(e.p_value, 1.0) <= :max_p
        ORDER BY ABS(COALESCE(e.effect_size, 0)) DESC
        """,
        {"min_effect": min_effect, "max_p": max_p, "min_seg": min_seg},
    ).fetchall()
    return rows[:top] if top else rows


def _make_pair_spec(a, b) -> dict:
    """Synthesize an experiments.py-compatible spec representing A AND B."""
    return {
        "key": f"PAIR::{a['hypothesis_key']}+{b['hypothesis_key']}",
        "hypothesis": f"{a['hypothesis']}  AND  {b['hypothesis']}",
        "unit": a["unit"],
        "metric": a["metric"],
        "segment_expr": f"({a['segment_expr']}) AND ({b['segment_expr']})",
        "notes": "synthesized pair",
    }


def _already_tested(con, a_id: int, b_id: int, within_days: int = 14) -> bool:
    """Skip pairs we already tested recently (either order)."""
    cutoff = int(time.time()) - within_days * 86400
    row = con.execute(
        "SELECT 1 FROM experiment_pairs WHERE run_ts >= ? AND "
        "((exp_a_id=? AND exp_b_id=?) OR (exp_a_id=? AND exp_b_id=?)) LIMIT 1",
        (cutoff, a_id, b_id, b_id, a_id),
    ).fetchone()
    return bool(row)


def run_pairs(con, *, run_date: str, run_ts: int,
              top: int = 30, min_effect: float = MIN_EFFECT, max_p: float = MAX_P,
              min_seg: int = MIN_SEG, max_pairs: int = MAX_PAIRS_PER_RUN) -> int:
    candidates = _pick_candidates(con, top, min_effect, max_p, min_seg)
    if len(candidates) < 2:
        print(f"combine: only {len(candidates)} candidate(s); need >= 2. Skipping.")
        return 0

    print(f"combine: {len(candidates)} candidate hypotheses, testing pair interactions")

    tested = 0
    hits = 0
    for a, b in itertools.combinations(candidates, 2):
        if a["unit"] != b["unit"] or a["metric"] != b["metric"]:
            continue
        if _already_tested(con, a["id"], b["id"]):
            continue
        if tested >= max_pairs:
            break
        spec = _make_pair_spec(a, b)
        result = run_experiment(con, spec)
        tested += 1

        # Baseline for the pair is the same baseline we'd use for any experiment
        # (everything OUTSIDE the combined segment). We report the component
        # values from a/b's latest row for easy comparison.
        a_val = a["segment_value"]
        b_val = b["segment_value"]
        if result.status == "success":
            best_solo = max(
                (abs(a_val - (result.baseline_value or 0)) if a_val is not None else 0),
                (abs(b_val - (result.baseline_value or 0)) if b_val is not None else 0),
            )
            combined_delta = abs((result.segment_value or 0) - (result.baseline_value or 0))
            lift = combined_delta - best_solo
        else:
            lift = None

        con.execute(
            """
            INSERT INTO experiment_pairs
            (run_date, run_ts, exp_a_id, exp_b_id, combined_size, baseline_size,
             combined_value, baseline_value, a_value, b_value, interaction_lift,
             p_value, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (run_date, run_ts, a["id"], b["id"],
             result.segment_size, result.baseline_size,
             result.segment_value, result.baseline_value,
             a_val, b_val, lift, result.p_value, result.status),
        )
        con.commit()

        if result.status == "success" and lift is not None and lift > 0.005:
            hits += 1
            print(f"  + {a['hypothesis_key']} x {b['hypothesis_key']}  "
                  f"n={result.segment_size}  combined={result.segment_value:+.4f}  "
                  f"a={a_val:+.4f}  b={b_val:+.4f}  lift={lift:+.4f}  p={result.p_value}")

    print(f"combine: tested {tested} pairs, {hits} with meaningful lift.")
    return tested


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--min-effect", type=float, default=MIN_EFFECT)
    ap.add_argument("--max-p", type=float, default=MAX_P)
    ap.add_argument("--min-seg", type=int, default=MIN_SEG)
    ap.add_argument("--max-pairs", type=int, default=MAX_PAIRS_PER_RUN)
    args = ap.parse_args()

    con = db.connect()
    ts = int(time.time())
    date = dt.datetime.fromtimestamp(ts, dt.UTC).strftime("%Y-%m-%d")
    run_pairs(con, run_date=date, run_ts=ts,
              top=args.top, min_effect=args.min_effect, max_p=args.max_p,
              min_seg=args.min_seg, max_pairs=args.max_pairs)
    return 0


if __name__ == "__main__":
    sys.exit(main())
