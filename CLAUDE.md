# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A whale-tracker / research pipeline for Kalshi prediction markets. Continuously ingests two trade firehoses, enriches them with market metadata, and runs a self-driving library of "freakonomics-style" hypothesis tests to find tradable edges. Python 3.13, SQLite, no ORM.

## Hybrid cloud/local architecture (the key mental model)

Data collection runs in **GitHub Actions**. Analysis runs **locally** against a single SQLite file. They are stitched together through the git repo itself.

```
Kalshi API ──▶ GitHub Actions (3 workflows) ──▶ data/<stream>/*.jsonl.gz (committed)
                                                         │
                                                         ▼
                                       local box:  git pull  +  sync_from_cloud.py
                                                         │
                                                         ▼
                                                   data/kalshi.db  (SQLite)
                                                         │
                                                         ▼
                                         enrich_markets.py + daily_experiment.py
```

- Cloud collectors write gzipped JSONL (not SQLite — binary diffs would bloat git). State files under `data/<stream>/state.json` carry resume cursors between runs.
- Three cloud streams run on independent `*/10` cron schedules and **race to push to master**. Each workflow uses a `concurrency` group + a rebase-push loop (5 retries) in the commit step. When editing these workflows, preserve that pattern.
- The local `sync_from_cloud.py` is idempotent (`INSERT OR IGNORE`). It's fine to run whenever.

## Two trade tables, two purposes

`trades_public` and `trades_social` look similar but are NOT interchangeable:

- **`trades_public`** — anonymized firehose. Has yes/no price columns. The only source with historical depth (backfill via cursor). Used for **Strategy #2** (coat-tail big flow) in `analyze_bigflow.py`.
- **`trades_social`** — realtime-only with `taker_nickname` + `taker_social_id`. Used for **Strategy #1** (follow specific whales) and all `experiments.py` hypotheses (segmentation needs nicknames).

`markets.settlement_value` is `100` / `0` / `NULL`. Any P&L query must filter `m.result IN ('yes','no')` AND usually `m.settlement_value IN (0,100)` to avoid void markets.

## The daily experiment engine

`run_daily_experiment.bat` is the 4am Windows Task Scheduler entry point. Sequence:

1. `sync_from_cloud.py` — pull + merge overnight JSONL blobs
2. `enrich_markets.py --stale-hours 12 --limit 2000` — two-pass market metadata refresh (pass 1: `/markets/{ticker}`; pass 2: `/events/{event_ticker}` for category). **The `--limit 2000` cap can lag behind cloud backfill ingestion**, producing a growing `category IS NULL` backlog; when this happens, a pass-2-only catchup against `/events/` (dedupe to unique events) is ~10× faster than a full enrich.
3. `daily_experiment.py --n 5` — picks up to 5 never-tested or stale (>60d) hypotheses from `experiments.GENERATORS`, runs each as two-sample test, writes one row per hypothesis into `experiments`. Every 7 days since the first ever run, also triggers `combine_experiments.run_pairs()` for pair-interaction mining.

Log message **"No new or stale hypotheses to run today. Library exhausted at this refresh cadence."** = every hypothesis in the library was tested within the last 60 days. Add to `GENERATORS` or lower `--refresh-days` to get more runs.

### Adding a hypothesis

Append a dict to `GENERATORS` in `experiments.py`. Required fields: `key` (stable, unique — dedupe lookup; never reuse or rename), `hypothesis`, `unit` (`'user'` or `'trade'`), `metric` (`'roi'` or `'win_rate'`), `segment_expr` (SQL boolean over `t.*` / `m.*`, evaluated inside the `_TRADE_ROW_CTE`). Optional: `notes`.

The SQL is plugged into a CTE that joins `trades_social` + `markets` restricted to resolved markets, so a segment_expr can reference any column of either.

## Common commands

```bash
# Install
pip install -r requirements.txt

# Sync cloud data + merge into local DB
python sync_from_cloud.py                    # everything
python sync_from_cloud.py --stream public    # one stream
python sync_from_cloud.py --dry-run          # report only

# Enrich market metadata
python enrich_markets.py                     # default --stale-hours 6, no limit
python enrich_markets.py --limit 2000        # what the daily cron uses

# Daily pipeline (what cron runs)
run_daily_experiment.bat                     # sync + enrich + experiments

# Run experiments manually
python daily_experiment.py --n 10            # more than the default 5
python daily_experiment.py --dry-run         # print plan, don't write
python daily_experiment.py --force-combine   # pair-interaction pass today
python combine_experiments.py --top 30       # standalone pair mining

# Ad-hoc analysis
python analyze_bigflow.py                    # Strategy #2: coat-tail ROI by category
python analyze_bigflow.py --min-notional 500 --by series
python fade_backtest.py                      # default: --exec-source public, --window-sec 3600
python fade_backtest.py --exec-source social # much higher coverage when trades_public is sparse

# Install/unregister the Windows cron
powershell -ExecutionPolicy Bypass -File .\register_daily_task.ps1
Unregister-ScheduledTask -TaskName "KalshiDailyExperiment" -Confirm:$false
```

## Credentials

`creds.json` + `kalshi_private_key.pem` at repo root (both git-ignored). In GitHub Actions, the same values come from `KALSHI_KEY_ID` and `KALSHI_PRIVATE_KEY_PEM` secrets. `client.py` is local-only (reads `creds.json`); `cloud_lib.py` supports both, env-first. Kalshi auth is **RSA-PSS-SHA256** signing over `timestamp + method + path`.

## Gotchas

- **`fade_backtest.py` default `--exec-source=public` will show ~1% coverage** if `trades_public` is sparse per-ticker (the backfill walks broadly rather than densifying markets one at a time). Use `--exec-source=social` for realistic coverage while backfill catches up.
- **GitHub Actions `*/10` crons throttle on public repos** — actual firings are closer to 1 / 45min. That's fine for the backfill math; don't "fix" it.
- **`experiments.GENERATORS` keys are immutable once released.** Renaming one silently turns past runs into orphans (breaks dedupe) and makes the pair-interaction table reference a ghost. Add a new key instead.
- **Power/reboot safety**: everything 24/7 runs in Actions. The only local automation is the 4am Task Scheduler job, which is configured `-StartWhenAvailable` so a missed run catches up on boot.
