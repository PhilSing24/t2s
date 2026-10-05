# Running t2s on this laptop (Windows + WSL2)

The pipeline is run **on demand**: you start it with `./start.sh` when you
want data and stop it with `./stop.sh`. While it runs, its processes are
systemd **user** services inside WSL, so a process that crashes is restarted
and the data path repairs itself. It does not start by itself when WSL or
Windows starts. This note says how to run it, how it is set up, and what each
event does. Everything assumes the distro `Ubuntu-22.04`, the user `philippe`
and the checkout at `/home/philippe/t2s`.

Running it always-on, started at Windows boot, is still possible; see
**Optional: always on**.

## Day to day

```bash
./start.sh --markets spot,futures     # start all four handlers (through systemd); returns when everything is up
./status.sh                # the pipeline in a few lines; exit 1 if something needs attention
./stop.sh                  # stop in order: handlers, WDB (flush + checkpoint), TP
systemctl --user status 't2s-*'       # every unit
journalctl --user -u t2s-tp -f        # follow one process (tp, wdb, trade-fh, quote-fh, trade-fh-fut, quote-fh-fut)
systemctl --user list-timers 't2s-*'  # when the daily jobs run next
```

**Keep a WSL window open while it runs.** WSL shuts the distro down, abruptly,
about a minute after the last Windows process attached to it has gone (a
terminal window, VS Code). Closing the last window therefore kills the
pipeline without a flush. Nothing TP has logged is lost and the next start
recovers (WDB replays, the trade handlers backfill), but the time in between
is a hole. So: start, leave a terminal or VS Code open, and run `./stop.sh`
before closing it.

**What a stopped pipeline looks like.** After `./stop.sh`, `./status.sh` says
so and exits 0:

```
PROC : tp stopped  wdb stopped  trade-fh stopped  quote-fh stopped  trade-fh-fut stopped  quote-fh-fut stopped  tmux:none
BOOT : t2s.target disabled: the pipeline runs on demand (./start.sh, ./stop.sh); lingering yes (keeps the timers running)
PIPELINE: stopped (not running, nothing failed). Start it with ./start.sh --markets spot,futures
OK   : nothing needs attention
```

"Stopped" means nothing at all is running and no unit has failed
(`ops/pipeline_state.sh`). A pipeline that is partly up, or a failed unit, is
still flagged.

**What each start does by itself.**
- WDB replays what TP logged and it had not stored, then rolls any past day
  still in a `tmp.<date>` directory into the HDB.
- Each trade handler asks TP for the last trade id it logged and backfills
  the trades since then over REST, within the limits: 500,000 ids per symbol,
  and about two days back for futures. A longer stop leaves the gap recorded
  as `unrecoverable` in `trade_gap`; that is expected in this mode and is
  raised by `./status.sh` for the alert window only.
- The quote handlers rebuild their books from fresh snapshots.

**A day you stop before midnight UTC** stays in `tmp.<date>` until the next
start. The daily end-of-day check reports it as `PENDING until the next
start`, not as a failure, notes the date in `run/eod.pending`, and checks it
for real on its first run after the day has been rolled. A day on which the
pipeline did not run at all is reported as `NOT RUN`. `./status.sh` shows
both the waiting day and the pending check without flagging them.

## What is installed

| Piece | Where | Installed by |
|---|---|---|
| Six services and `t2s.target` | `~/.config/systemd/user/` (templates in `ops/systemd/`) | `ops/systemd/install.sh --enable`; the target was then disabled with `systemctl --user disable t2s.target`, so the pipeline starts only with `./start.sh` |
| Four timers (enabled; they run whether or not the pipeline does) | same | same |
| Environment of the processes (`PATH`, `QHOME`, `QPATH`, `T2S_HDB_DIR`, `T2S_TMP_DIR`) | `~/.config/t2s/t2s.env` | same; edit it there |
| Lingering, so the timers run without a login | systemd | `sudo loginctl enable-linger philippe` (once) |
| Permission to set the clock | `/etc/sudoers.d/t2s-hwclock` | you, see **Clock** |
| Windows task that boots WSL at startup | Task Scheduler | not installed; optional, see **Optional: always on** |

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

The timers run whether or not the pipeline does, as long as WSL is up. With
the pipeline stopped, the end-of-day check reports `NOT RUN` or `PENDING`
(see **Day to day**), retention works on the logs as usual, and the status
run records `PIPELINE: stopped`.

The three daily timers have `Persistent=true`: a run missed while the laptop
was off or asleep happens as soon as the timer is active again. The times are
written in UTC in the units themselves, so nothing depends on the system time
zone (the crontab they replace needed a conversion to Singapore time).
A daily job that fails leaves its unit `failed`; `./status.sh` reports it.

## What happens when

| Event | What happens | What you do |
|---|---|---|
| A process crashes or is killed | systemd restarts it within seconds. TP: handlers resend, WDB replays, `missed` stays 0. A trade handler: the trades it did not receive are recorded in `trade_gap` and backfilled. A quote handler: the hole is marked by the handler restart and the books resync. WDB: replays from its checkpoint. | Nothing. `./status.sh` shows the restart count. |
| `wsl --shutdown`, WSL crash, the last WSL window closed while it runs | Everything stops at once, without a flush. Nothing logged by TP is lost. The pipeline stays down. | `./start.sh --markets spot,futures` when you want it back: TP continues its tpSeqNo, WDB replays the log, the trade handlers backfill what was traded meanwhile. |
| Windows restart | The pipeline is not started. | `./start.sh` when you want it. |
| Laptop sleep | See the next section. | Nothing; check `./status.sh` if you are curious. |

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

**The first seconds after wake.** WSL corrects its clock a few seconds after
resuming, not at once. Measured on 2026-10-04 after a 6-minute sleep: 377 spot
quote rows were received in that window, with the clock still reading the
pre-sleep time. That window is too short for the clock timer, so the handlers
deal with it row by row:

- While the exchange event time is more than 2 seconds ahead of the handler's
  clock (`clock_lag_ms` in `config/shared.json`), the row's `time` is the
  exchange event time. `fhRecvTimeUtcNs` keeps the stale clock reading.
- Such a row is identifiable afterwards because its `time` differs from its
  `fhRecvTimeUtcNs`:

  ```q
  \l kdb/utils/hdbUtils.q
  .hdb.use[`:hdb]
  .hdb.clockCorrected[`quote_binance; 2026.10.06]
  ```

- Each handler logs `CLOCK LAG` at the start and the end, and `./status.sh`
  raises `clockLagRows` for the alert window:

  ```
  RECENT (60 min): quote_binance clockLagRows +377 (4 min ago)
         - quote_binance: clockLagRows +377 in the last 60 min
  ```

  It needs no action when it follows a `SLEEP` line. Without a sleep it means
  the clock fell behind while running: look at the `CLOCK` lines and
  `ops/cron/clock.log`.

Rows are partitioned by `time`, so a wake just after midnight UTC no longer
puts rows in the previous day's partition. One side effect remains: in that
case WDB's own clock still reads yesterday while rows dated today arrive. They
go to the right directory, and WDB counts them as `unexpectedDateRows`, which
`./status.sh` also raises for the alert window. It is expected in that one
case.

Measured on 2026-10-05 with a 9-minute lid close (all four handlers):

| Table | Corrected rows | Of which without an event time | Lagging rows left uncorrected |
|---|---|---|---|
| `quote_binance` | 212 | 3 (invalid rows) | 0 |
| `quote_binance_fut` | 307 | 3 (invalid rows) | 0 |
| `trade_binance` | 2,738 | 2,000 (backfilled) | 0 |
| `trade_binance_fut` | 9,810 | 5,626 (backfilled) | 0 |

The clock read 02:39:21 to 02:39:26 for all of them, while their exchange
times run from 02:39:23 to 02:48:51, a lag of 2 to 565 seconds. Two things
that run showed:

- The lag starts *before* the sleep, not only after the wake. While the
  machine goes down the clock nearly stops and data still arrives for a couple
  of minutes (exchange times 02:39 to 02:41 against a clock stuck at 02:39:2x).
- A trade handler can log a second, short `CLOCK LAG` right after the first
  ends: a backfill reply that arrived while the clock was stale and was
  published just after it was stepped. Its rows are corrected like the others.

Only a clock that is behind is handled this way. A clock that is ahead, or a
drift under 2 seconds, is left to the `t2s-clock` timer and TP's `clock skew`.

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

## Optional: always on

Not set up. This is what it takes to have the pipeline come back by itself
after a WSL or Windows restart, with no window open, instead of running it on
demand:

1. `systemctl --user enable t2s.target`: the pipeline starts when WSL starts
   (lingering is already on). The target brings up all four handlers.
   `systemctl --user disable t2s.target` goes back to on demand.
2. The Windows task below, which starts WSL at Windows startup and keeps the
   distro alive.

systemd services do not keep a WSL distro alive, and nothing starts the distro
after a Windows restart. `ops/windows-boot-task.ps1` registers a scheduled task
that does both: it runs at system startup under your account, logged on or
not, with a `wsl.exe` command that never exits. The script has not been run on
this machine, so it is untested.

**Without this task the pipeline only lives while a WSL window or VS Code is
open.** This was seen in the live check on 2026-10-05: after
`wsl --shutdown` the distro started at 00:53:06 UTC and the pipeline was up two
seconds later, but 84 seconds on WSL terminated the distro again, abruptly,
because no Windows process was attached to it any more; it came back when VS
Code reconnected. Nothing was lost (WDB replayed, the trade handlers backfilled
the two short holes), but the pipeline was down for 26 seconds for no reason
other than the missing keep-alive.

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
