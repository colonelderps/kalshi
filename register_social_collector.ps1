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

# Three triggers for resilience to Windows-update reboots, mid-day crashes, etc.:
#   1. AtLogOn — fires when Dave logs in fresh (covers post-reboot)
#   2. Every 15 min, indefinitely — watchdog; if collector died, this restarts it.
#      MultipleInstances=IgnoreNew means this is a no-op when collector is healthy.
#   3. AtStartup — would also help, but requires admin. Skipped.
$triggerLogon = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME

$triggerRepeat = New-ScheduledTaskTrigger -Once -At (Get-Date)
$triggerRepeat.Repetition = (New-ScheduledTaskTrigger `
    -Once -At (Get-Date) `
    -RepetitionInterval (New-TimeSpan -Minutes 15) `
    -RepetitionDuration (New-TimeSpan -Days 3650)).Repetition

$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Days 365) `
    -MultipleInstances IgnoreNew

$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Register-ScheduledTask `
    -TaskName "KalshiSocialCollector" `
    -Description "24/7 collector for the Kalshi /v1/social/trades firehose. Self-heals via 15-min watchdog trigger; survives overnight reboots." `
    -Action $action `
    -Trigger @($triggerLogon, $triggerRepeat) `
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
