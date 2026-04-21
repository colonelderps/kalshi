# Register the daily Kalshi experiment task with Windows Task Scheduler.
# Run this ONCE (as your normal user, not as admin):
#     powershell -ExecutionPolicy Bypass -File .\register_daily_task.ps1
#
# After that, Task Scheduler handles it. Re-running is idempotent (-Force).
# To unschedule:  Unregister-ScheduledTask -TaskName "KalshiDailyExperiment" -Confirm:$false

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$bat  = Join-Path $here "run_daily_experiment.bat"

if (-not (Test-Path $bat)) {
    Write-Error "Cannot find $bat"
    exit 1
}

$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$bat`""
$trigger = New-ScheduledTaskTrigger -Daily -At 4am
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
    -MultipleInstances IgnoreNew

# Run as the current user, whether or not they're logged in at 4am.
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Register-ScheduledTask `
    -TaskName "KalshiDailyExperiment" `
    -Description "Daily freakonomics-style hypothesis run on Kalshi trade data." `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Force | Out-Null

Write-Host "Scheduled. Details:" -ForegroundColor Green
Get-ScheduledTask -TaskName "KalshiDailyExperiment" | Format-List TaskName, State, Triggers
Write-Host "Logs will land in $here\logs\daily_YYYY-MM-DD.log"
