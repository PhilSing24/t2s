# windows-boot-task.ps1 - start WSL (and with it the t2s pipeline) when
# Windows boots, before anyone logs on, and keep the distro alive.
#
# Run ONCE from an elevated PowerShell (Run as administrator):
#     powershell -ExecutionPolicy Bypass -File \\wsl$\Ubuntu-22.04\home\philippe\t2s\ops\windows-boot-task.ps1
# or paste the commands below. It asks for your Windows password once.
#
# What it sets up
#   A scheduled task "t2s WSL boot" that runs at system startup under YOUR
#   account, whether or not you are logged on. Its action is a wsl.exe
#   command that never exits:
#       wsl.exe -d Ubuntu-22.04 -u philippe --exec /bin/sleep infinity
#   Starting the distro boots systemd; with lingering enabled for your user
#   (sudo loginctl enable-linger philippe) the user manager starts at once
#   and brings up t2s.target, i.e. TP, WDB and the four feed handlers.
#   The sleeping process is what keeps the distro running: systemd services
#   alone do NOT keep a WSL distro alive (Microsoft: "systemd services will
#   NOT keep your WSL instance alive",
#   https://learn.microsoft.com/en-us/windows/wsl/systemd).
#
# Why a stored password
#   A task that runs before logon needs credentials. WSL distros belong to
#   a user, so the task cannot run as SYSTEM. The password is stored by
#   Windows in its credential store, not in this file.
#   IF YOUR WINDOWS PASSWORD CHANGES the task stops working (last run result
#   0x8007052E, "logon failure") and the pipeline no longer starts at boot.
#   Update it, again from an elevated PowerShell:
#       $c = Get-Credential -UserName "$env:USERDOMAIN\$env:USERNAME" -Message "New Windows password"
#       Set-ScheduledTask -TaskName "t2s WSL boot" -User $c.UserName -Password $c.GetNetworkCredential().Password
#   With a Microsoft account, the password is the account's password, not
#   the PIN.
#
# Check / run by hand / remove
#       Get-ScheduledTask -TaskName "t2s WSL boot" | Get-ScheduledTaskInfo
#       Start-ScheduledTask -TaskName "t2s WSL boot"
#       Unregister-ScheduledTask -TaskName "t2s WSL boot" -Confirm:$false

$ErrorActionPreference = "Stop"
$taskName = "t2s WSL boot"
$distro   = "Ubuntu-22.04"
$wslUser  = "philippe"

$cred = Get-Credential -UserName "$env:USERDOMAIN\$env:USERNAME" -Message "Windows password for the scheduled task '$taskName'"

$action   = New-ScheduledTaskAction -Execute "$env:SystemRoot\System32\wsl.exe" `
            -Argument "-d $distro -u $wslUser --exec /bin/sleep infinity"
$trigger  = New-ScheduledTaskTrigger -AtStartup
# No time limit (the action is meant to run forever); start even on battery;
# if it ever exits (e.g. after wsl --shutdown), start it again after a minute.
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) `
            -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable `
            -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew

Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings `
    -User $cred.UserName -Password $cred.GetNetworkCredential().Password -RunLevel Limited `
    -Description "Boot the $distro WSL distro at Windows startup and keep it alive, so the t2s pipeline (systemd user units) runs without a logon." -Force

Write-Host ""
Write-Host "Registered '$taskName'. Starting it now..."
Start-ScheduledTask -TaskName $taskName
Start-Sleep -Seconds 5
Get-ScheduledTask -TaskName $taskName | Get-ScheduledTaskInfo | Format-List TaskName, LastRunTime, LastTaskResult, NextRunTime
Write-Host "LastTaskResult 267009 (0x41301) means 'currently running', which is what we want."
