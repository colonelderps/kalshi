@echo off
REM Continuous social-trades collector wrapper.
REM
REM Launched at boot by Windows Task Scheduler (see register_social_collector.ps1).
REM Auto-restarts the Python collector on crash with a 30-second backoff so a
REM transient error (network blip, Kalshi 5xx, SQLite lock) doesn't take us
REM offline permanently.
REM
REM Output goes to logs\social_collector_YYYY-MM-DD.log, rotated daily.

setlocal
cd /d %~dp0

if not exist logs mkdir logs

:loop
for /f "tokens=2 delims==" %%i in ('wmic os get localdatetime /value ^| find "="') do set dt=%%i
set today=%dt:~0,4%-%dt:~4,2%-%dt:~6,2%
set logfile=logs\social_collector_%today%.log

echo. >> "%logfile%"
echo [%date% %time%] starting collect_social.py >> "%logfile%"
python -u collect_social.py --poll-seconds 8 >> "%logfile%" 2>&1
echo [%date% %time%] collector exited with code %ERRORLEVEL%, restarting in 30s >> "%logfile%"

timeout /t 30 /nobreak > nul
goto loop
