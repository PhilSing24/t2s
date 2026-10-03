# Running t2s day to day on this laptop (Windows + WSL2)

This note covers what happens to the pipeline through the laptop's life and
the three ways it can be restarted. Nothing here is installed by the repo;
pick one and follow its section. Everything assumes the distro
`Ubuntu-22.04`, the user `philippe`, and the checkout at `/home/philippe/t2s`.

## What happens today

| Event | Effect | Recovery |
|---|---|---|
| Laptop sleeps / wakes | The WSL VM is paused. On wake the WebSocket idle timeout (30 s) and TCP keepalive make each feed handler reconnect to Binance within about a minute; TP and WDB carry on. The WSL clock may now be behind. | Automatic for the data path. Check the clock: `./status.sh` compares WSL to Windows and TP reports `clockSkewMs`; fix with `sudo hwclock -s`. |
| `wsl --shutdown`, Windows restart | Every process dies without warning. Nothing is lost that TP had logged: WDB replays from its checkpoint on the next start, and TP's tpSeqNo continues from its reservation file. Rows a handler had sent in the instant TP died are counted as `missed`. | Nothing restarts the pipeline by itself. That is what the options below are for. |
| Terminal closed | Nothing. The processes live in the tmux session `t2s`; `.wslconfig` has `vmIdleTimeout=-1`, so the VM stays up. | `tmux attach -t t2s` |
| Crash of one process | TP crash: handlers reconnect and re-register when it is back, WDB reconnects and replays. WDB crash: replay on restart. Handler crash: its rows stop until it is relaunched. | Manual relaunch in its tmux window, or option B's restart-on-failure. |

`./status.sh` shows all of this in a few lines and exits non-zero when
anything needs attention.

## Option A: Windows Task Scheduler starts the pipeline at logon

The task runs `start.sh --headless` inside WSL when you log on. Simplest to
reason about: one place to look (Task Scheduler), no systemd involvement,
the tmux session is there to attach to.

- Pros: one moving part; survives Windows restarts; you can run it by hand
  from Task Scheduler at any time.
- Cons: no restart if a process dies later in the day; `wsl --shutdown`
  still kills everything without a graceful WDB flush; runs only after you
  log on, not at boot.

Install from an elevated PowerShell (text in `ops/windows-logon-task.ps1`):

```powershell
$action  = New-ScheduledTaskAction -Execute "wsl.exe" -Argument "-d Ubuntu-22.04 -u philippe -- /home/philippe/t2s/start.sh --headless --markets spot,futures"
$trigger = New-ScheduledTaskTrigger -AtLogOn -User "$env:USERNAME"
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 20) -StartWhenAvailable
Register-ScheduledTask -TaskName "t2s pipeline" -Action $action -Trigger $trigger -Settings $settings -Description "Start the t2s market data pipeline in WSL at logon"
```

Remove with `Unregister-ScheduledTask -TaskName "t2s pipeline"`.

## Option B: systemd unit inside WSL, booted by a trivial logon task

The unit (`ops/t2s.service`) runs `start.sh --headless` and `stop.sh`,
restarts the pipeline if start.sh fails, and gives WDB a graceful flush when
systemd stops the unit. systemd is already enabled in this WSL
(`/etc/wsl.conf` has `systemd=true`), but nothing starts the distro after a
Windows restart, so a logon task that merely runs `wsl.exe -d Ubuntu-22.04
-- true` is still needed to boot it.

- Pros: `Restart=on-failure` relaunches the pipeline if start.sh fails;
  `systemctl status t2s` and `journalctl -u t2s` give history; `wsl
  --shutdown` asks systemd to stop units, so `stop.sh` gets a chance to
  flush WDB before the VM goes away (not guaranteed: WSL gives a short
  grace period).
- Cons: two moving parts (Task Scheduler for the boot, systemd for the
  service); the unit restarts the whole pipeline, not a single process;
  tmux inside a service needs the user's environment, which the unit sets
  up explicitly.

Install:

```bash
sudo cp /home/philippe/t2s/ops/t2s.service /etc/systemd/system/t2s.service
sudo systemctl daemon-reload
sudo systemctl enable --now t2s.service
systemctl status t2s.service
```

And in an elevated PowerShell, the boot task:

```powershell
$action  = New-ScheduledTaskAction -Execute "wsl.exe" -Argument "-d Ubuntu-22.04 -- true"
$trigger = New-ScheduledTaskTrigger -AtLogOn -User "$env:USERNAME"
Register-ScheduledTask -TaskName "WSL boot for t2s" -Action $action -Trigger $trigger -Description "Boot the Ubuntu-22.04 distro so systemd starts t2s"
```

Remove with `sudo systemctl disable --now t2s.service` and
`Unregister-ScheduledTask -TaskName "WSL boot for t2s"`.

## Option C: manual

`./start.sh --markets spot,futures` after each logon or restart, `./stop.sh`
before a planned shutdown, `./status.sh` whenever you want to know. Nothing
to install; nothing happens while you are not looking.

## Daily checks (cron inside WSL)

cron is running in this WSL. The entries added by the ops step:
`check_eod.sh` at 00:30 UTC (confirms yesterday's UTC partition against its
log), log retention with `-apply` at 00:40 UTC (deletes logs older than 7
days whose rows are all in the HDB; switched from a dry run on 2026-10-03
after the first real run removed 10.6 GB), and `status.sh` at 07:00 UTC
for a summary. Each writes to `ops/cron/*.log` under the repo.

The crontab is scheduled in local time. This WSL's zone is Asia/Singapore
(UTC+8, no daylight saving) and Debian's cron 3.0pl1 ignores `CRON_TZ`
(verified on 2026-10-03: an entry for 18:45 fired at 18:45 local), so the
entries are written as 08:30, 08:40 and 15:00 local with the UTC time in
their comments. If the system zone ever changes, shift them. The scripts
themselves use UTC throughout: `check_eod.sh` takes yesterday with
`date -u`, and logmgr's default date is q's `.z.d`, which is UTC. See the
crontab itself (`crontab -l`) for the exact lines; they are appended after
the existing entries of other projects.

## Resources and the WSL memory cap

Measured on 2026-10-03 over a 10-minute live run with spot and futures
(samples every 5 s), plus the maintenance tools on the real logs:

| Process / task | RSS | CPU |
|---|---|---|
| TP | 8 MB | 1 % average, 3 % peak |
| WDB (buffers flush at 50,000 rows per table) | 14 MB average, 22 MB peak | under 1 % |
| each feed handler (3) | 10 to 11 MB | under 1 % |
| retention scan of 10.6 GB of logs | 1.0 GB peak, 88 s | one core |
| rebuild report of the heaviest day (2.4 GB log, 13 M rows) | 6.5 GB peak, 2 m 51 s | one core |
| WDB replay after a long outage | roughly the gap's rows in memory; a whole 13 M-row day would be in the same range as the rebuild | one core |

So the pipeline itself is tiny; only the one-off tools and a worst-case
replay need gigabytes, one at a time. The host has 15.6 GB and
`.wslconfig` currently gives WSL 12 GB, which leaves Windows under 4 GB.
Proposed `.wslconfig` (text only, not applied by the repo):

```
[wsl2]
vmIdleTimeout=-1
memory=8GB
swap=8GB
```

8 GB covers the pipeline with the heaviest rebuild or replay running alongside
it, and gives Windows back 4 GB. Keep `swap=8GB` as the safety net for a
rebuild bigger than any day seen so far. If you expect to rebuild several
days at once, raise `memory` to 10GB for that session; the tools run one
day at a time either way.

## Clock

WSL2's clock can fall behind after the laptop sleeps. Partitions are dated
by this clock (through the feed handlers' receive timestamps), so a drift
matters. Two independent detectors: `./status.sh` compares the WSL clock
to the Windows clock through interop and warns above 2 s; TP's `.health[]`
reports `clockSkewMs`, the median of receive time minus exchange event
time, and degrades above 5 s.

Fix: `sudo hwclock -s` (re-reads the hardware clock, which Windows keeps
right). Optional, so the fix needs no password: `sudo visudo` and add

```
philippe ALL=(root) NOPASSWD: /usr/sbin/hwclock -s
```

limited to exactly that command. Not installed by the repo.
