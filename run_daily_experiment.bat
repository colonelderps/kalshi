@echo off
REM Wrapper invoked by Windows Task Scheduler. Writes a dated log file so
REM you can see what happened each morning without opening the DB.
cd /d "%~dp0"
if not exist logs mkdir logs
for /f "tokens=1-3 delims=/- " %%a in ("%date%") do set DATESTAMP=%%c-%%a-%%b
python -u daily_experiment.py --n 5 >> "logs\daily_%DATESTAMP%.log" 2>&1
