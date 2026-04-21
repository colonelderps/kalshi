@echo off
REM Launches the three Kalshi whale-tracker jobs in independent windows.
REM Survives closing this terminal / closing Claude Code. Close each window to stop.
cd /d "%~dp0"
start "Kalshi Backfill" cmd /k python -u backfill_public.py
start "Kalshi Social"   cmd /k python -u collect_social.py --poll-seconds 3
start "Kalshi Enrich"   cmd /k python -u enrich_loop.py
echo.
echo Launched 3 windows. You can close this one.
