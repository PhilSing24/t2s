# Running t2s unattended on this laptop (Windows + WSL2)

The pipeline runs as systemd **user** services inside WSL and comes back by
itself after a crash, a WSL restart or a Windows restart. This note says how
it is set up, what each event does, and what you have to do by hand (very
little). Everything assumes the distro `Ubuntu-22.04`, the user `philippe`
and the checkout at `/home/philippe/t2s`.

## Day to day

```bash
./status.sh                # the pipeline in a few lines; exit 1 if something needs attention
./start.sh --markets spot,futures     # start (through systemd)
./stop.sh                  # stop in order: handlers, WDB (flush + checkpoint), TP
systemctl --user status 't2s-*'       # every unit
journalctl --user -u t2s-tp -f        # follow one process (tp, wdb, trade-fh, quote-fh, trade-fh-fut, quote-fh-fut)
systemctl --user list-timers 't2s-*'  # when the daily jobs run next
```

## What is installed

| Piece | Where | Installed by |
|---|---|---|
| Six services and `t2s.target` | `~/.config/systemd/user/` (templates in `ops/systemd/`) | `ops/systemd/install.sh --enable` |
| Four timers | same | same |
| Environment of the processes (`PATH`, `QHOME`, `QPATH`, `T2S_HDB_DIR`, `T2S_TMP_DIR`) | `~/.config/t2s/t2s.env` | same; edit it there |
| Lingering, so the user services start at WSL boot without a login | systemd | `sudo loginctl enable-linger philippe` (once) |
| Permission to set the clock | `/etc/sudoers.d/t2s-hwclock` | you, see **Clock** |
| Windows task that boots WSL at startup | Task Scheduler | you, `ops/windows-boot-task.ps1` |

After changing a unit template: `ops/systemd/install.sh` again (it re-renders
and reloads), then restart what changed. `ops/systemd/install.sh --uninstall`
removes the units.

## The services

`t2s-tp`, `t2s-wdb`, `t2s-trade-fh`, `t2s-quote-fh`, `t2s-trade-fh-fut`,
`t2s-quote-fh-fut`, grouped by `t2s.target`.

- **Order.** WDB starts after TP; the handlers after TP and WDB. Stopping
  goes in reverse: handlers, then WDB, then TP.
- **Graceful stop.** WDB is asked over IPC to flush its buffers and write
  its checkpoint (SIGTERM as fallback, on which it flushes too). TP saves
  its session file on SIGTERM. The handlers close their sockets.
- **Restart.** `Restart=on-failure` after 2 s: a crash, a `kill -9` or a
  non-zero exit restarts the process. At most 10 restarts in 5 minutes,
  then the unit is left `failed` and `./status.sh` says so. A handler that
  exits with 1 (bad config) or 2 (TP rejected its row width) is not retried.
- **A TP restart does not restart the handlers.** They reconnect and resend
  what TP had not logged; WDB reconnects and replays.
- **systemd or tmux, never both.** `./start.sh --tmux` runs the old way, each
  process in a window of the tmux session `t2s`, with nothing restarting a
  process that dies. `start.sh` refuses tmux mode while a unit is active,
  and every unit refuses to start while the tmux session exists.

## The timers

| Timer | When | Runs | Log |
|---|---|---|---|
| `t2s-check-eod` | 00:30 UTC | `check_eod.sh`: yesterday's partition against its TP log | `ops/cron/check_eod.log` |
| `t2s-retention` | 00:40 UTC | `logmgr.q -retention -apply`: deletes logs older than 7 days whose rows are all in the HDB | `ops/cron/retention.log` |
| `t2s-status` | 07:00 UTC | `status.sh` | `ops/cron/status.log` |
| `t2s-clock` | every 5 minutes | `ops/clock_check.sh` | `ops/cron/clock.log` |

The three daily timers have `Persistent=true`: a run missed while the laptop
was off or asleep happens as soon as the timer is active again. The times are
written in UTC in the units themselves, so nothing depends on the system time
zone (the crontab they replace needed a conversion to Singapore time).
A daily job that fails leaves its unit `failed`; `./status.sh` reports it.

## What happens when

| Event | What happens | What you do |
|---|---|---|
| A process crashes or is killed | systemd restarts it within seconds. TP: handlers resend, WDB replays, `missed` stays 0. A trade handler: the trades it did not receive are recorded in `trade_gap` and backfilled. A quote handler: the hole is marked by the handler restart and the books resync. WDB: replays from its checkpoint. | Nothing. `./status.sh` shows the restart count. |
| `wsl --shutdown`, WSL crash | Everything stops, possibly without a flush. Nothing logged by TP is lost. When the distro starts again, systemd starts the user manager (lingering) and `t2s.target`: TP continues its tpSeqNo, WDB replays the log, the trade handlers backfill what was traded meanwhile. | Nothing if the Windows boot task is installed (it restarts WSL within a minute). Otherwise open a WSL terminal. |
| Windows restart | The boot task starts WSL at system startup, before logon; then as above. | Nothing. |
| Laptop sleep | See the next section. | Nothing; check `./status.sh` if you are curious. |
| Your Windows password changes | The boot task can no longer log on; WSL does not start at boot. | Update the task (below). |

## Laptop sleep and wake

While the laptop sleeps the WSL VM is frozen: no process runs, nothing is
lost that was already received, and the exchange keeps trading.

On wake, in order:

1. **TP notices.** Its one-second timer finds a gap of more than 30 s between
   two ticks and logs `RESUMED - no timer tick for N s`. `./status.sh` shows it
   on the `SLEEP` line with the time, the duration and what has happened since.
2. **The handlers find their WebSocket dead** (idle timeout 30 s, TCP
   keepalive) and reconnect to Binance, usually within a minute. Each counts a
   `wsReconnects`.
3. **Quotes.** Each quote handler publishes one invalid row per symbol, which
   marks the hole in the data, then rebuilds its books from fresh snapshots
   (`resyncs`). `check_quote_seq.q` will list the hole as a *marked* break.
4. **Trades.** The first trade after the reconnect jumps ahead in Binance's
   trade id. The gap is recorded in `trade_gap` and backfilled over REST
   within the rate limits: about 24,000 spot and 12,000 futures trades per
   minute. A sleep longer than about two days leaves the futures part
   `unrecoverable` (`tooOld`), and more than 500,000 missing ids per symbol
   is `tooLarge`; both can be filled later from the Binance daily archive.
5. **Timers.** A daily job whose time passed during the sleep runs now.
6. **Clock.** WSL's clock normally jumps forward on wake. If it does not, it
   stays behind by the length of the sleep, and since partitions are dated by
   this clock that matters. Two things catch it: the `t2s-clock` timer, within
   5 minutes, and `./status.sh` (`CLOCK` lines; TP's `clock skew` against the
   exchange's event times). See **Clock**.

Nothing in this sequence needs you. What to look at afterwards:

```
SLEEP: last resume 06:12:40Z after 7 h 31 min (4 min ago, 1 since TP started); since then: wsReconnects +4, resyncs +6, exchGaps +6, tradesBackfilled +58112, gapsRecovered +4
GAPS : open 2 (oldest 221 s)  recovered 4 (58112 trades backfilled)  unrecoverable 0
```

`GAPS open` goes back to 0 when the backfill is done. Reconnects, resyncs and
recovered gaps are shown but do not raise attention; an unrecoverable gap, an
unmarked quote break or a clock drift does.

## Clock

`ops/clock_check.sh` compares the WSL clock with the VM's hardware clock,
which follows the Windows clock and can be read without root
(`/sys/class/rtc/rtc0/since_epoch`). Above 2 s of drift it runs
`sudo -n /usr/sbin/hwclock -s`, which re-reads the hardware clock. That needs
one sudoers line, limited to exactly that command:

```bash
sudo visudo -f /etc/sudoers.d/t2s-hwclock
```

and in the editor, this single line:

```
philippe ALL=(root) NOPASSWD: /usr/sbin/hwclock -s
```

Without the line the timer cannot fix the clock: it reports the drift in
`ops/cron/clock.log`, its unit shows `failed`, and `./status.sh` raises it with
the manual fix, `sudo hwclock -s`.

## Windows: start WSL at boot

systemd services do not keep a WSL distro alive, and nothing starts the distro
after a Windows restart. `ops/windows-boot-task.ps1` registers a scheduled task
that does both: it runs at system startup under your account, logged on or
not, with a `wsl.exe` command that never exits.

Install, from an **elevated** PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File \\wsl$\Ubuntu-22.04\home\philippe\t2s\ops\windows-boot-task.ps1
```

It asks for your Windows password once and stores it with the task (in the
Windows credential store, not in the repo).

**If your Windows password changes, update the task**, or it fails with
"logon failure" (last result `0x8007052E`) and WSL no longer starts at boot.
Elevated PowerShell:

```powershell
$c = Get-Credential -UserName "$env:USERDOMAIN\$env:USERNAME" -Message "New Windows password"
Set-ScheduledTask -TaskName "t2s WSL boot" -User $c.UserName -Password $c.GetNetworkCredential().Password
```

With a Microsoft account the password is the account's password, not the PIN.
Check with `Get-ScheduledTask -TaskName "t2s WSL boot" | Get-ScheduledTaskInfo`
(last result `0x41301` means it is running, which is right); remove with
`Unregister-ScheduledTask -TaskName "t2s WSL boot" -Confirm:$false`.

`.wslconfig` keeps `vmIdleTimeout=-1` (below), so the VM is not stopped for
being idle.

## Resources and the WSL memory cap

Measured on 2026-10-03 over a 10-minute live run with spot and futures
(samples every 5 s), plus the maintenance tools on the real logs:

| Process / task | RSS | CPU |
|---|---|---|
| TP | 8 MB | 1 % average, 3 % peak |
| WDB (buffers flush at 50,000 rows per table) | 14 MB average, 22 MB peak | under 1 % |
| each feed handler (3 at the time; there are now 4, the quote handlers keep up to 4000 levels per side per symbol) | 10 to 11 MB | under 1 % |
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
