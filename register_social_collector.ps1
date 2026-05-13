# Register the continuous social-trades collector with Windows Task Scheduler.
# Run this ONCE (as your normal user, not as admin):
#     powershell -ExecutionPolicy Bypass -File .\register_social_collector.ps1
#
# After that, Task Scheduler keeps it alive. Re-running is idempotent (-Force).
# To unschedule:  Unregister-ScheduledTask -TaskName "KalshiSocialCollector" -Confirm:$false
# To check live status: Get-ScheduledTask -TaskName "KalshiSocialCollector" | Get-ScheduledTaskInfo

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$bat  = Join-Path $here "collect_social_continuous.bat"

if (-not (Test-Path $bat)) {
    Write-Error "Cannot find $bat"
    exit 1
}

$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$bat`""

# At-logon trigger only. -AtStartup needs admin; -AtLogOn works as normal user.
# Machine is on 24/7 and Dave is typically logged in. The .bat has its own
# auto-restart loop, so we don't need scheduler-level restart settings either.
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME

$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Days 365) `
    -MultipleInstances IgnoreNew

$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Register-ScheduledTask `
    -TaskName "KalshiSocialCollector" `
    -Description "24/7 collector for the Kalshi /v1/social/trades firehose. Restarts on crash." `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Force | Out-Null

Write-Host "Scheduled. Starting it now..." -ForegroundColor Green
Start-ScheduledTask -TaskName "KalshiSocialCollector"
Start-Sleep -Seconds 3

Get-ScheduledTask -TaskName "KalshiSocialCollector" | Format-List TaskName, State
Write-Host "Logs will land in $here\logs\social_collector_YYYY-MM-DD.log"
Write-Host ""
Write-Host "To stop:        Stop-ScheduledTask  -TaskName KalshiSocialCollector"
Write-Host "To check state: Get-ScheduledTaskInfo -TaskName KalshiSocialCollector"
Write-Host "To unregister:  Unregister-ScheduledTask -TaskName KalshiSocialCollector -Confirm:`$false"
