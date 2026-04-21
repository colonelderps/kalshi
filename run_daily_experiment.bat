@echo off
REM Wrapper invoked by Windows Task Scheduler. Writes a dated log file so
REM you can see what happened each morning without opening the DB.
cd /d "%~dp0"
if not exist logs mkdir logs
for /f "tokens=1-3 delims=/- " %%a in ("%date%") do set DATESTAMP=%%c-%%a-%%b

REM Pull any JSONL blobs the GitHub Actions tail collected overnight and
REM merge them into the master DB before running experiments. Non-fatal if
REM it fails (e.g. no network).
python -u sync_from_cloud.py >> "logs\daily_%DATESTAMP%.log" 2>&1

REM Refresh market metadata (categories, resolutions) for every ticker we've
REM seen. Lets today's experiments classify trades properly and fills in
REM `result` for markets that resolved overnight. Limit capped so one bad day
REM can't stall the daily job indefinitely.
python -u enrich_markets.py --stale-hours 12 --limit 2000 >> "logs\daily_%DATESTAMP%.log" 2>&1

python -u daily_experiment.py --n 5 >> "logs\daily_%DATESTAMP%.log" 2>&1
