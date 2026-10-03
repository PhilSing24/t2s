#!/bin/bash
# check_eod.sh - after midnight UTC, confirm that yesterday's day is safely
# in the HDB: the partition exists, every row logged by TP for that day is
# in it (exact tpSeqNo check, neighbouring partitions included for the
# midnight straddle), and no tmp.<date> dir for that day is left behind.
#
# Usage:
#   ./check_eod.sh            # yesterday (UTC)
#   ./check_eod.sh 2026.10.02 # a specific date
#
# Exit code 0 when complete, 1 otherwise, 2 on usage error. Suitable for
# cron; prints a few lines either way.
#
# Paths come from the same environment variables the processes use
# (T2S_TP_LOG_DIR, T2S_HDB_DIR, T2S_TMP_DIR), with repo-relative defaults.

set -u
BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

DATE=${1:-$(date -u -d 'yesterday' +%Y.%m.%d)}
if ! [[ "$DATE" =~ ^[0-9]{4}\.[0-9]{2}\.[0-9]{2}$ ]]; then
    echo "usage: $0 [YYYY.MM.DD]" >&2
    exit 2
fi

LOG_DIR=${T2S_TP_LOG_DIR:-$BASEDIR/kdb/tick/logs}
HDB_DIR=${T2S_HDB_DIR:-$BASEDIR/hdb}
TMP_DIR=${T2S_TMP_DIR:-$BASEDIR/kdb/}

RC=0
echo "=== check_eod $DATE  ($(date -u +%Y-%m-%dT%H:%M:%SZ))"

# 1. partition vs log, exact
if T2S_TP_LOG_DIR="$LOG_DIR" T2S_HDB_DIR="$HDB_DIR" q "$BASEDIR/kdb/utils/logmgr.q" -check-eod "$DATE" < /dev/null; then
    :
else
    RC=1
fi

# 2. nothing left in a tmp dir for that date
if [[ -d "$TMP_DIR/tmp.$DATE" ]]; then
    echo "LOG:   tmp.$DATE still present under $TMP_DIR - the roll did not complete (or late rows arrived); needs attention"
    RC=1
else
    echo "LOG:   no tmp.$DATE left behind"
fi

# 3. the tables in the partition
if [[ -d "$HDB_DIR/$DATE" ]]; then
    echo "LOG:   partition tables: $(ls "$HDB_DIR/$DATE" | tr '\n' ' ')"
fi

[[ $RC -eq 0 ]] && echo "=== $DATE OK" || echo "=== $DATE NEEDS ATTENTION"
exit $RC
