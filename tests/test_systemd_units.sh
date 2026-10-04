#!/bin/bash
# test_systemd_units.sh - the systemd unit files are well-formed and say
# what the operating notes promise. Nothing is installed or started: the
# units are rendered into the sandbox and checked there.
#
#   1. every unit passes `systemd-analyze --user verify`
#   2. the properties that matter: restart policy and limits, start/stop
#      order, the graceful WDB stop, exit codes that must not be retried,
#      timers at the right UTC times with Persistent=true
#   3. helper scripts: wait-port.sh, clock_check.sh (with a fake RTC)
#
# Exit code 0 on success.

set -u
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=t_lib.sh
source "$SCRIPT_DIR/t_lib.sh"
cd "$T2S_TEST_ROOT"

FAILURES=0
cleanup() { local rc=$?; if [[ $rc -eq 0 ]]; then t2s_sandbox_remove; else echo "  Sandbox preserved at: $T2S_SANDBOX"; fi; exit $rc; }
trap cleanup EXIT INT TERM
fail() { echo "  FAIL: $*"; FAILURES=$((FAILURES + 1)); }
pass() { echo "  PASS: $*"; }

t2s_sandbox_reset
U="$T2S_SANDBOX/units"
ops/systemd/install.sh --render "$U" > /dev/null || { fail "render"; exit 1; }
has() { grep -qE "$2" "$U/$1" && pass "$1: $3" || fail "$1: $3 (pattern: $2)"; }

echo ""
echo "=== 1. rendering and systemd-analyze verify ==="
[[ $(ls "$U" | wc -l) -eq 15 ]] && pass "15 units rendered" || fail "rendered $(ls "$U" | wc -l) units"
grep -rq "@T2S_ROOT@" "$U" && fail "a template placeholder was left in a unit" || pass "no placeholder left"
grep -q "WorkingDirectory=$T2S_TEST_ROOT/kdb/tick" "$U/t2s-tp.service" && pass "paths point at this checkout" || fail "checkout path not substituted"
if command -v systemd-analyze >/dev/null 2>&1 && systemctl --user is-system-running >/dev/null 2>&1; then
    OUT=$(for u in "$U"/*; do systemd-analyze --user verify "$u" 2>&1; done | grep -v "^$" | grep -v "t2s.env")
    [[ -z "$OUT" ]] && pass "systemd-analyze --user verify: no complaint on any unit" || { fail "systemd-analyze verify"; echo "$OUT" | head -10; }
else
    echo "  SKIP: no user systemd here - verify not run"
fi

echo ""
echo "=== 2. what the units promise ==="
for s in t2s-tp t2s-wdb t2s-trade-fh t2s-quote-fh t2s-trade-fh-fut t2s-quote-fh-fut; do
    f="$s.service"
    grep -q "^Restart=on-failure" "$U/$f" && grep -q "^StartLimitBurst=10" "$U/$f" && grep -q "^StartLimitIntervalSec=300" "$U/$f" \
        && grep -q "^PartOf=t2s.target" "$U/$f" && grep -q "ExecStartPre=.*/guard.sh" "$U/$f" \
        || fail "$f: restart policy, limits, PartOf or guard missing"
done
pass "all six services: Restart=on-failure, 10 restarts per 5 min, part of t2s.target, tmux guard"
has t2s-wdb.service "^After=t2s-tp.service$" "starts after TP (so it stops before TP)"
has t2s-wdb.service "^ExecStop=.*/wdb-stop.sh 5011" "graceful stop through the IPC shutdown"
for s in t2s-trade-fh t2s-quote-fh t2s-trade-fh-fut t2s-quote-fh-fut; do
    grep -q "^After=t2s-tp.service t2s-wdb.service$" "$U/$s.service" || fail "$s: not ordered after TP and WDB"
    grep -q "^RestartPreventExitStatus=1 2 3$" "$U/$s.service" || fail "$s: bad-config exits would be retried"
    grep -qE "^(Requires|BindsTo)=" "$U/$s.service" && fail "$s: bound to TP - a TP restart would restart the handler"
done
pass "handlers: after TP and WDB, not bound to TP, exit codes 1/2/3 not retried"
has t2s-trade-fh-fut.service "^ExecStart=$T2S_TEST_ROOT/build/trade_feed_handler_fut$" "runs the futures trade binary"
has t2s.target "^Wants=t2s-tp.service t2s-wdb.service t2s-trade-fh.service t2s-quote-fh.service t2s-trade-fh-fut.service t2s-quote-fh-fut.service$" "wants all six services"
has t2s.target "^WantedBy=default.target$" "enabled = starts with the user manager"
has t2s-check-eod.timer "^OnCalendar=\*-\*-\* 00:30:00 UTC$" "00:30 UTC"
has t2s-retention.timer "^OnCalendar=\*-\*-\* 00:40:00 UTC$" "00:40 UTC"
has t2s-status.timer "^OnCalendar=\*-\*-\* 07:00:00 UTC$" "07:00 UTC"
for t in t2s-check-eod t2s-retention t2s-status; do grep -q "^Persistent=true$" "$U/$t.timer" || fail "$t.timer: not persistent"; done
pass "the three daily timers are Persistent (a run missed while off or asleep happens on wake)"
has t2s-retention.service "logmgr.q -retention -apply" "applies retention"
has t2s-retention.service "^Environment=T2S_LOG_RETENTION_DAYS=7$" "7 days"
has t2s-status.service "^SuccessExitStatus=1$" "attention (exit 1) is a report, not a unit failure"
has t2s-clock.timer "^OnUnitActiveSec=5min$" "every 5 minutes"

echo ""
echo "=== 3. helper scripts ==="
ops/systemd/wait-port.sh "$T2S_PORT_UNREACHABLE" 1 2> /dev/null; [[ $? -eq 1 ]] && pass "wait-port.sh: 1 when nothing listens" || fail "wait-port.sh on a closed port"
( q -q -p "$T2S_PORT_TP" < /dev/null > /dev/null 2>&1 <<< 'system "sleep 3"; exit 0' ) & sleep 1
ops/systemd/wait-port.sh "$T2S_PORT_TP" 3 && pass "wait-port.sh: 0 once the port listens" || fail "wait-port.sh on an open port"
wait 2>/dev/null
RTC="$T2S_SANDBOX/rtc"
date +%s > "$RTC"
OUT=$(T2S_CLOCK_RTC_FILE="$RTC" ops/clock_check.sh --check); rc=$?
[[ $rc -eq 0 && "$OUT" == *"clock: ok"* ]] && pass "clock_check: ok when the clocks agree" || fail "clock_check ok case: rc=$rc $OUT"
echo $(( $(date +%s) + 300 )) > "$RTC"
OUT=$(T2S_CLOCK_RTC_FILE="$RTC" ops/clock_check.sh --check); rc=$?
[[ $rc -eq 1 && "$OUT" == *"DRIFT"*"behind"* ]] && pass "clock_check: reports a system clock 5 min behind (exit 1)" || fail "clock_check drift case: rc=$rc $OUT"
echo $(( $(date +%s) - 300 )) > "$RTC"
OUT=$(T2S_CLOCK_RTC_FILE="$RTC" ops/clock_check.sh --check); rc=$?
[[ $rc -eq 1 && "$OUT" == *"ahead"* ]] && pass "clock_check: reports a system clock ahead" || fail "clock_check ahead case: rc=$rc $OUT"
OUT=$(T2S_CLOCK_RTC_FILE="$T2S_SANDBOX/none" ops/clock_check.sh --check); [[ $? -eq 2 ]] && pass "clock_check: 2 when the RTC cannot be read" || fail "clock_check unreadable"

echo ""; echo "==========================================="
if [[ $FAILURES -eq 0 ]]; then echo "systemd units: all checks passed"; echo "==========================================="; exit 0; fi
echo "systemd units: $FAILURES failure(s)"; echo "==========================================="; exit 1
