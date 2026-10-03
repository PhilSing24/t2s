# windows-logon-task.ps1 - register the Windows Task Scheduler task for
# option A in ops/RUNNING.md: start the t2s pipeline in WSL at logon.
# Run from an elevated PowerShell. Nothing in the repo runs this for you.
#
# For option B (systemd inside WSL), register the boot-only task instead:
#   -Argument "-d Ubuntu-22.04 -- true"   with TaskName "WSL boot for t2s"

$action   = New-ScheduledTaskAction -Execute "wsl.exe" `
            -Argument "-d Ubuntu-22.04 -u philippe -- /home/philippe/t2s/start.sh --headless --markets spot,futures"
$trigger  = New-ScheduledTaskTrigger -AtLogOn -User "$env:USERNAME"
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 20) -StartWhenAvailable
Register-ScheduledTask -TaskName "t2s pipeline" -Action $action -Trigger $trigger -Settings $settings `
    -Description "Start the t2s market data pipeline in WSL at logon"

# Remove:  Unregister-ScheduledTask -TaskName "t2s pipeline"
