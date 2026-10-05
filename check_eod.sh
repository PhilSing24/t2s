#!/bin/bash
# check_eod.sh - after midnight UTC, confirm that yesterday's day is safely
# in the HDB: the partition exists, every row logged by TP for that day is
# in it (exact tpSeqNo check, neighbouring partitions included for the
# midnight straddle), and no tmp.<date> dir for that day is left behind.
#
# Usage:
#   ./check_eod.sh            # yesterday (UTC), and any day left pending earlier
#   ./check_eod.sh 2026.10.02 # a specific date
#
# Exit code 0 when complete, 1 otherwise, 2 on usage error. Suitable for a
# timer; prints a few lines either way.
#
# The pipeline is run on demand, so two cases are not failures (exit 0):
#   NOT RUN  nothing was recorded for the day: no log, no tmp dir, no partition
#   PENDING  the day's rows are in tmp.<date>, there is no partition yet, and
#            the pipeline is stopped (ops/pipeline_state.sh). WDB rolls the day
#            into the HDB when the pipeline is next started. The date is noted
#            in run/eod.pending and checked for real by a later run without
#            arguments, once it has been rolled.
# Everything else is checked as before and fails for a real problem, e.g. a
# day not rolled while the pipeline is running.
#
# Paths and ports come from the same environment variables the processes use
# (T2S_TP_LOG_DIR, T2S_HDB_DIR, T2S_TMP_DIR, T2S_TP_PORT, T2S_WDB_PORT), with
# repo-relative defaults; T2S_RUN_DIR is where eod.pending is kept.

set -u
BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ARG=${1:-}
if [[ -n "$ARG" ]] && ! [[ "$ARG" =~ ^[0-9]{4}\.[0-9]{2}\.[0-9]{2}$ ]]; then
    echo "usage: $0 [YYYY.MM.DD]" >&2
    exit 2
fi

LOG_DIR=${T2S_TP_LOG_DIR:-$BASEDIR/kdb/tick/logs}
HDB_DIR=${T2S_HDB_DIR:-$BASEDIR/hdb}
TMP_DIR=${T2S_TMP_DIR:-$BASEDIR/kdb/}
RUN_DIR=${T2S_RUN_DIR:-$BASEDIR/run}
PENDING_FILE="$RUN_DIR/eod.pending"

set_pending() {   # set_pending DATE yes|no
    mkdir -p "$RUN_DIR"
    local rest; rest=$(grep -vxF "$1" "$PENDING_FILE" 2>/dev/null)
    { [[ -n "$rest" ]] && echo "$rest"; [[ "$2" == "yes" ]] && echo "$1"; } | sort -u > "$PENDING_FILE.new"
    mv "$PENDING_FILE.new" "$PENDING_FILE"
    [[ -s "$PENDING_FILE" ]] || rm -f "$PENDING_FILE"
}

# check_day DATE: 0 complete, not run, or pending; 1 needs attention
check_day() {
    local DATE=$1 RC=0
    echo "=== check_eod $DATE  ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
    local has_log=0 has_tmp=0 has_part=0
    [[ -e "$LOG_DIR/$DATE.log" ]] && has_log=1
    [[ -d "$TMP_DIR/tmp.$DATE" ]] && has_tmp=1
    [[ -d "$HDB_DIR/$DATE" ]] && has_part=1

    if [[ $has_log -eq 0 && $has_tmp -eq 0 && $has_part -eq 0 ]]; then
        echo "LOG:   nothing was recorded on $DATE (no log, no tmp dir, no partition): the pipeline did not run"
        set_pending "$DATE" no
        echo "=== $DATE NOT RUN"
        return 0
    fi
    if [[ $has_tmp -eq 1 && $has_part -eq 0 ]]; then
        local state; state=$("$BASEDIR/ops/pipeline_state.sh" 2>/dev/null)
        if [[ "$state" == "stopped" ]]; then
            echo "LOG:   tmp.$DATE holds the day's rows and the pipeline is stopped: the day is rolled into the HDB at the next start"
            echo "LOG:   noted in $PENDING_FILE; the check runs once the day is rolled (or: ./check_eod.sh $DATE)"
            set_pending "$DATE" yes
            echo "=== $DATE PENDING until the next start"
            return 0
        fi
    fi

    # 1. partition vs log, exact
    if T2S_TP_LOG_DIR="$LOG_DIR" T2S_HDB_DIR="$HDB_DIR" q "$BASEDIR/kdb/utils/logmgr.q" -check-eod "$DATE" < /dev/null; then
        :
    else
        RC=1
    fi

    # 2. nothing left in a tmp dir for that date
    if [[ $has_tmp -eq 1 ]]; then
        echo "LOG:   tmp.$DATE still present under $TMP_DIR - the roll did not complete (or late rows arrived); needs attention"
        RC=1
    else
        echo "LOG:   no tmp.$DATE left behind"
    fi

    # 3. the tables in the partition
    if [[ $has_part -eq 1 ]]; then
        echo "LOG:   partition tables: $(ls "$HDB_DIR/$DATE" | tr '\n' ' ')"
    fi

    if [[ $RC -eq 0 ]]; then set_pending "$DATE" no; echo "=== $DATE OK"; else echo "=== $DATE NEEDS ATTENTION"; fi
    return $RC
}

if [[ -n "$ARG" ]]; then
    check_day "$ARG"; exit $?
fi

# No argument: the days left pending by earlier runs, then yesterday
YESTERDAY=$(date -u -d 'yesterday' +%Y.%m.%d)
RC=0
for d in $(cat "$PENDING_FILE" 2>/dev/null); do
    [[ "$d" == "$YESTERDAY" ]] && continue
    check_day "$d" || RC=1
done
check_day "$YESTERDAY" || RC=1
exit $RC
