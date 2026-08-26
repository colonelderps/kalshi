# Register the daily Kalshi experiment task with Windows Task Scheduler.
# Run this ONCE (as your normal user, not as admin):
#     powershell -ExecutionPolicy Bypass -File .\register_daily_task.ps1
#
# After that, Task Scheduler handles it. Re-running is idempotent (-Force).
# To unschedule:  Unregister-ScheduledTask -TaskName "KalshiDailyExperiment" -Confirm:$false

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$bat  = Join-Path $here "run_daily_experiment_safe.bat"

if (-not (Test-Path $bat)) {
    Write-Error "Cannot find $bat"
    exit 1
}

$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$bat`""

# Two triggers, both hitting the SAME safe wrapper (which no-ops if today's
# run already has a completion marker in its log -- see
# run_daily_experiment_safe.bat):
#   1. Fixed 4am -- the normal daily firing.
#   2. Every 3 hours, all day -- catch-up watchdog. If the 4am run started
#      but never finished (killed, DNS blip, anything else -- 2026-08-01
#      through 2026-08-03 saw 3 different incomplete runs in a row with no
#      reboot involved, so "just re-fire once a day" wasn't enough), this
#      notices the missing completion marker and retries same-day instead of
#      silently waiting until tomorrow's 4am slot. Mirrors the pattern
#      already proven for KalshiSocialCollector's 15-min watchdog, just at a
#      coarser interval appropriate for a batch job instead of an
#      always-running process.
$triggerDaily = New-ScheduledTaskTrigger -Daily -At 4am

$triggerWatchdog = New-ScheduledTaskTrigger -Once -At (Get-Date)
$triggerWatchdog.Repetition = (New-ScheduledTaskTrigger `
    -Once -At (Get-Date) `
    -RepetitionInterval (New-TimeSpan -Hours 3) `
    -RepetitionDuration (New-TimeSpan -Days 3650)).Repetition

$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
    -MultipleInstances IgnoreNew

# S4U ("Service For User"): runs without an interactive session, survives a
# reboot landing at/around 4am without waiting for Dave to log in. Not the
# confirmed cause of the 2026-08-01..03 failures (uptime showed no reboots
# in that window), but it's the same class of gap already found and fixed
# for KalshiSocialCollector on 2026-05-17 -- cheap insurance to close it here
# too rather than wait for it to bite this task specifically.
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType S4U -RunLevel Limited

Register-ScheduledTask `
    -TaskName "KalshiDailyExperiment" `
    -Description "Daily freakonomics-style hypothesis run on Kalshi trade data. Self-heals via 3h watchdog against incomplete runs; survives reboots via S4U." `
    -Action $action `
    -Trigger @($triggerDaily, $triggerWatchdog) `
    -Settings $settings `
    -Principal $principal `
    -Force | Out-Null

Write-Host "Scheduled. Details:" -ForegroundColor Green
Get-ScheduledTask -TaskName "KalshiDailyExperiment" | Format-List TaskName, State, Triggers
Write-Host "Logs will land in $here\logs\daily_YYYY-MM-DD.log"
Write-Host "Watchdog checks every 3h for a missing 'Done. candidates' completion marker and retries same-day."
