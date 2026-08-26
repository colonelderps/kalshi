@echo off
REM Same-day catch-up wrapper for run_daily_experiment.bat.
REM
REM Why this exists: the fixed 4am trigger alone has no way to notice a run
REM that started but never finished (Ctrl+C / killed / DNS blip / anything
REM else) -- a missed COMPLETION, not a missed FIRING. -StartWhenAvailable
REM only catches the latter. This wrapper checks whether today's log already
REM contains daily_experiment.py's completion marker; if not, it (re)runs
REM the full pipeline. Registered on a periodic trigger (see
REM register_daily_task.ps1) alongside the fixed 4am trigger, so a failed
REM overnight run gets caught and retried same-day instead of silently
REM waiting until tomorrow's 4am slot.
cd /d "%~dp0"
if not exist logs mkdir logs
for /f "tokens=1-3 delims=/- " %%a in ("%date%") do set DATESTAMP=%%c-%%a-%%b

set LOGFILE=logs\daily_%DATESTAMP%.log

if exist "%LOGFILE%" (
    findstr /C:"Done. candidates" "%LOGFILE%" >nul
    if not errorlevel 1 (
        echo [%date% %time%] Today's run already completed -- skipping. >> "%LOGFILE%"
        exit /b 0
    )
)

echo [%date% %time%] No completed run found for today -- running now. >> "%LOGFILE%"
call "%~dp0run_daily_experiment.bat"
