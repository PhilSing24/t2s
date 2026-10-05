#!/bin/bash
# status.sh - the pipeline in a few lines: processes, TP and WDB health,
# rows today, the counters that matter, disk, logs, clock, and an
# "attention" list. Exit code 1 when anything needs attention (so it can
# run from cron), 0 otherwise.
#
# Paths and ports come from the environment the processes use
# (T2S_TP_LOG_DIR, T2S_HDB_DIR, T2S_TMP_DIR, T2S_TP_PORT, T2S_WDB_PORT)
# with the same defaults.

set -u
BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SESSION="t2s"
PORT_TP=${T2S_TP_PORT:-5010}
PORT_WDB=${T2S_WDB_PORT:-5011}
LOG_DIR=${T2S_TP_LOG_DIR:-$BASEDIR/kdb/tick/logs}
TMP_DIR=${T2S_TMP_DIR:-$BASEDIR/kdb/}
HDB_DIR=${T2S_HDB_DIR:-$BASEDIR/hdb}
CLOCK_DRIFT_WARN_SEC=2

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
ATTN=()
note() { ATTN+=("$1"); }

echo "t2s status  $(date -u +%Y-%m-%dT%H:%M:%SZ)  ($BASEDIR)"

# ---------------- processes ----------------
listener() { lsof -ti TCP:"$1" -sTCP:LISTEN 2>/dev/null | head -1; }
# The binary as tmux mode starts it (./build/x or build/x) or as its systemd
# unit does ($BASEDIR/build/x). Anchored, so it cannot match a shell or editor.
fh_pid()   { pgrep -f "^(\./|$BASEDIR/)?build/$1( |$)" 2>/dev/null | head -1; }
TP_PID=$(listener "$PORT_TP"); WDB_PID=$(listener "$PORT_WDB")
SPOT=$(fh_pid trade_feed_handler); FUT=$(fh_pid trade_feed_handler_fut)
QUOTE=$(fh_pid quote_feed_handler); QUOTE_FUT=$(fh_pid quote_feed_handler_fut)
up() { if [[ -n "$2" ]]; then echo -n "$1 up(pid $2)  "; else echo -n "$1 DOWN  "; fi; }
echo -n "PROC : "; up tp "$TP_PID"; up wdb "$WDB_PID"; up trade-fh "$SPOT"; up quote-fh "$QUOTE"; up trade-fh-fut "$FUT"; up quote-fh-fut "$QUOTE_FUT"
if tmux has-session -t $SESSION 2>/dev/null; then echo "tmux:$SESSION"; else echo "tmux:none"; fi

# ---------------- systemd units ----------------
# How the pipeline is run and what systemd had to do: state and restart
# count of each unit, when the timers fire next.
if systemctl --user cat t2s-tp.service >/dev/null 2>&1; then
    LINE=""; SD_ACTIVE=0
    for u in tp wdb trade-fh quote-fh trade-fh-fut quote-fh-fut; do
        st=$(systemctl --user show -p ActiveState --value "t2s-$u.service" 2>/dev/null)
        nr=$(systemctl --user show -p NRestarts --value "t2s-$u.service" 2>/dev/null)
        LINE+="$u $st"
        [[ "${nr:-0}" -gt 0 ]] && LINE+="(restarted ${nr}x)"
        LINE+="  "
        [[ "$st" == "active" ]] && SD_ACTIVE=$((SD_ACTIVE + 1))
        [[ "$st" == "failed" ]] && note "systemd unit t2s-$u is FAILED (systemctl --user status t2s-$u; journalctl --user -u t2s-$u)"
    done
    echo "UNITS: $LINE"
    TL=""
    for t in check-eod retention status clock; do
        if [[ "$(systemctl --user is-active "t2s-$t.timer" 2>/dev/null)" == "active" ]]; then
            nx=$(systemctl --user show -p NextElapseUSecRealtime --value "t2s-$t.timer" 2>/dev/null)
            [[ -z "$nx" || "$nx" == "n/a" ]] && nx="periodic" || nx=$(date -u -d "$nx" +%m-%dT%H:%MZ 2>/dev/null || echo "$nx")
            TL+="$t next $nx  "
        else
            TL+="$t OFF  "
            note "timer t2s-$t.timer is not active (systemctl --user enable --now t2s-$t.timer)"
        fi
        res=$(systemctl --user show -p Result --value "t2s-$t.service" 2>/dev/null)
        [[ -n "$res" && "$res" != "success" ]] && note "last run of t2s-$t.service failed ($res): see ops/cron/ and journalctl --user -u t2s-$t"
    done
    echo "TIMER: $TL"
    en=$(systemctl --user is-enabled t2s.target 2>/dev/null)
    lg=$(loginctl show-user "$USER" -p Linger --value 2>/dev/null)
    echo "BOOT : t2s.target ${en:-not installed}; lingering ${lg:-unknown} (both needed to come back by itself after a WSL restart)"
    [[ "$en" == "enabled" && "$lg" != "yes" ]] && note "lingering is off: the pipeline will not start at WSL boot (sudo loginctl enable-linger $USER)"
    if [[ $SD_ACTIVE -gt 0 ]] && tmux has-session -t $SESSION 2>/dev/null; then
        note "systemd units AND a tmux session '$SESSION' exist at the same time - stop both with ./stop.sh"
    fi
else
    echo "UNITS: systemd units not installed (tmux mode; ops/systemd/install.sh installs them)"
fi
[[ -z "$TP_PID" ]]  && note "TP is down"
[[ -z "$WDB_PID" ]] && note "WDB is down"
MARKETS=$(cat "$BASEDIR/run/markets.active" 2>/dev/null || echo "")
if [[ -n "$TP_PID" ]]; then
    [[ "$MARKETS" == *spot* && -z "$SPOT" ]] && note "spot trade handler is down (markets.active=$MARKETS)"
    [[ "$MARKETS" == *spot* && -z "$QUOTE" ]] && note "spot quote handler is down (markets.active=$MARKETS)"
    [[ "$MARKETS" == *futures* && -z "$FUT" ]] && note "futures trade handler is down (markets.active=$MARKETS)"
    [[ "$MARKETS" == *futures* && -z "$QUOTE_FUT" ]] && note "futures quote handler is down (markets.active=$MARKETS)"
fi

# ---------------- TP / WDB internals ----------------
T2S_STATUS_MARKETS="$MARKETS" T2S_TP_PORT=$PORT_TP T2S_WDB_PORT=$PORT_WDB T2S_TMP_DIR="$TMP_DIR" q "$BASEDIR/kdb/utils/status.q" < /dev/null 2>/dev/null
QRC=$?
# status.q prints its own ATTN lines; fold its verdict into ours
[[ $QRC -ne 0 ]] && note "see the items reported by TP/WDB above"

# ---------------- disk, logs, tmp ----------------
if [[ -d "$LOG_DIR" ]]; then
    FREE=$(df -Pk "$LOG_DIR" 2>/dev/null | awk 'NR==2{printf "%.1f", $4/1048576}')
    LOGS=$(du -sh "$LOG_DIR" 2>/dev/null | cut -f1)
    NLOGS=$(ls "$LOG_DIR"/*.log 2>/dev/null | wc -l)
    echo "DISK : ${FREE:-?} GB free on the log filesystem; $LOG_DIR holds $NLOGS log(s), $LOGS"
else
    echo "DISK : log dir $LOG_DIR missing"; note "log dir missing"
fi
PENDING=$(ls -d "$TMP_DIR"/tmp.* 2>/dev/null | xargs -n1 basename 2>/dev/null | tr '\n' ' ')
TODAY=$(date -u +%Y.%m.%d)
OLD_TMP=""
for t in $PENDING; do [[ "$t" != "tmp.$TODAY" ]] && OLD_TMP="$OLD_TMP $t"; done
echo "TMP  : ${PENDING:-none}  (HDB partitions: $(ls -d "$HDB_DIR"/????.??.?? 2>/dev/null | wc -l), latest $(ls -d "$HDB_DIR"/????.??.?? 2>/dev/null | tail -1 | xargs -n1 basename 2>/dev/null))"
[[ -n "$OLD_TMP" ]] && note "past-date tmp dir(s) not rolled:$OLD_TMP"

# ---------------- clock (WSL vs Windows host) ----------------
if command -v powershell.exe >/dev/null 2>&1; then
    WIN=$(powershell.exe -NoProfile -Command "[DateTimeOffset]::UtcNow.ToUnixTimeSeconds()" 2>/dev/null | tr -d '\r\n ')
    LIN=$(date -u +%s)
    if [[ "$WIN" =~ ^[0-9]+$ ]]; then
        DRIFT=$(( LIN - WIN ))
        ADRIFT=${DRIFT#-}
        if (( ADRIFT > CLOCK_DRIFT_WARN_SEC )); then
            echo -e "CLOCK: ${RED}WSL clock is ${DRIFT}s off the Windows clock${NC}"
            echo "       fix: sudo hwclock -s   (then ./status.sh again; partitions are dated by this clock)"
            note "WSL clock drift ${DRIFT}s - run: sudo hwclock -s"
        else
            echo "CLOCK: WSL vs Windows ${DRIFT}s (ok)"
        fi
    else
        echo "CLOCK: could not read the Windows clock through interop"
    fi
else
    echo "CLOCK: Windows clock not reachable from here (no interop, e.g. when run by a systemd timer); see the hardware clock below"
fi
# The same check the t2s-clock timer runs: WSL clock vs the VM's hardware clock
if [[ -x "$BASEDIR/ops/clock_check.sh" ]]; then
    RTC_LINE=$("$BASEDIR/ops/clock_check.sh" --check 2>&1); RTC_RC=$?
    echo "       hardware clock: ${RTC_LINE#* clock: }"
    [[ $RTC_RC -eq 1 ]] && note "WSL clock differs from the hardware clock (${RTC_LINE#* clock: }) - run: sudo hwclock -s"
fi

# ---------------- verdict ----------------
if [[ ${#ATTN[@]} -eq 0 ]]; then
    echo -e "${GREEN}OK   : nothing needs attention${NC}"
    exit 0
fi
echo -e "${YELLOW}ATTN : ${#ATTN[@]} item(s)${NC}"
for a in "${ATTN[@]}"; do echo "       - $a"; done
exit 1
