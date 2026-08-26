# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 🛑 PROJECT HALTED (2026-08-26) — Kalshi banned in Washington state

**All automated/recurring Kalshi scripts have been deliberately stopped.** Do not re-enable, re-register, or restart any of the following without Dave's explicit go-ahead — this was a compliance-driven shutdown, not routine cleanup:

- Windows Scheduled Tasks `KalshiSocialCollector` and `KalshiDailyExperiment` — **Disabled** (not deleted; task definitions still exist).
- Any live `collect_social.py` / `run_collector.pyw` process — killed.
- GitHub Actions workflows `tail_public`, `backfill_public`, `tail_social` — all **`disabled_manually`** (the latter was already off from an earlier consolidation, unrelated to this).
- `kalshi_trading.py`'s live order-placement path (`execute --live`) was **not** touched by this shutdown — it was already manual-trigger-only with no scheduler, so it didn't fall under "automated/recurring." Its status re: the Washington ban is an open question Dave still needs to decide (API key revocation, etc.) — don't assume it's been addressed just because the schedulers are off.

If a future session is asked to "get things running again" or "why isn't data flowing," **check this section and ask before touching any of the above** — don't silently re-enable something that was intentionally stopped for a legal/regulatory reason.

> **⚠️ Environment: Dave works EXCLUSIVELY in the Claude Desktop app on Windows 11 Home — never CLI, browser, or mobile.** No CLI-only slash-command panels; live-stream/Monitor output does NOT render inline for him (use auto-refreshing web UIs, on-demand snapshots, or polling digests); he runs shell commands in a separate terminal himself and reports back. Full note in global `~/.claude/CLAUDE.md`.

## What this is

A whale-tracker / research pipeline for Kalshi prediction markets. Continuously ingests two trade firehoses, enriches them with market metadata, and runs a self-driving library of "freakonomics-style" hypothesis tests to find tradable edges. Python 3.13, SQLite, no ORM.

## Hybrid cloud/local architecture (the key mental model)

Data collection runs in **GitHub Actions**. Analysis runs **locally** against a single SQLite file. They are stitched together through the git repo itself.

```
Kalshi API ──▶ GitHub Actions (public streams) ──▶ data/<stream>/*.jsonl.gz (committed)
   │              [social stream is LOCAL-ONLY — see note below]
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
- Cloud streams run on independent cron schedules and **race to push to master**. Each workflow uses a `concurrency` group + a rebase-push loop (5 retries) in the commit step. When editing these workflows, preserve that pattern.
- The local `sync_from_cloud.py` is idempotent (`INSERT OR IGNORE`). It's fine to run whenever.

### Social collection is LOCAL-ONLY (2026-06-04)

**The cloud `tail_social` workflow is `disabled_manually` — do not re-enable it.** Social trades are collected by the **local** `KalshiSocialCollector` Windows task (`run_collector.pyw` → `collect_social.py`, 8s polling, writes the DB directly). Reasons it won the bake-off:
- Local has ~100% uptime on Dave's always-on box; cloud is throttled to ~1 run / 45min on public repos.
- The cloud `tail_social` job was *cancelling before its commit step* (collected 266s of trades, then "operation canceled" wiped the buffer → no JSONL committed → nothing to sync). Running both also risked dedupe confusion.
- Local task is hardened: S4U logon (survives reboots, no login needed), 15-min watchdog trigger, hang-detection in the wrapper, periodic WAL checkpoint.

`tail_public` and `backfill_public` stay **active** in the cloud — they feed `trades_public` (the anonymized firehose with historical backfill depth), which has no local collector. Only social was duplicated.

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

- **Sports is excluded everywhere by project-wide policy** (2026-05-12). Dave has no domain edge in Sports and the category was drowning out non-Sports signals during analysis. The exclusion is enforced in two places: `_TRADE_ROW_CTE` in `experiments.py` (so every `GENERATORS` hypothesis automatically filters out Sports) and `fade_backtest.py` (default ON, override with `--include-sports`). When adding new analysis scripts, replicate the `AND (m.category IS NULL OR m.category != 'Sports')` filter unless there's a specific reason not to.
- **`fade_backtest.py` default `--exec-source=public` will show ~1% coverage** if `trades_public` is sparse per-ticker (the backfill walks broadly rather than densifying markets one at a time). Use `--exec-source=social` for realistic coverage while backfill catches up.
- **GitHub Actions `*/10` crons throttle on public repos** — actual firings are closer to 1 / 45min. That's fine for the backfill math; don't "fix" it.
- **`experiments.GENERATORS` keys are immutable once released.** Renaming one silently turns past runs into orphans (breaks dedupe) and makes the pair-interaction table reference a ghost. Add a new key instead.
- **Power/reboot safety**: everything 24/7 runs in Actions. The only local automation is the 4am Task Scheduler job, which is configured `-StartWhenAvailable` so a missed run catches up on boot.
