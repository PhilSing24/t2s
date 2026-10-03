#!/bin/bash
# isolation_check.sh - prove a test run left production state untouched.
#
# Usage:
#   tests/isolation_check.sh record  <snapshot-file>
#   tests/isolation_check.sh compare <snapshot-file>
#
# record  writes a listing (path, size, mtime) of every file under the
#         production locations below, plus an md5 of every WDB checkpoint
#         file found, to <snapshot-file>.
# compare takes the same listing again and diffs it against <snapshot-file>.
#         Any difference is printed and the script exits 1.
#
# Locations covered (whichever exist):
#   - $T2S_HDB_DIR and $T2S_TMP_DIR if exported, and $T2S_WDB_CHECKPOINT
#   - the repo-root hdb/ and tmp/ directories
#   - the relative fallbacks WDB uses when nothing is exported:
#       kdb/hdb, kdb/tmp.*, kdb/wdb.lastTpSeqNo
#   - the production TP log directory kdb/tick/logs
#
# run_tests.sh calls record before the first test and compare after the
# last. It must NOT be used while the live pipeline is running, since the
# live TP and WDB legitimately write to these locations.

set -u

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
MODE=${1:-}
SNAP=${2:-}

if [[ -z "$MODE" || -z "$SNAP" ]]; then
    echo "usage: $0 record|compare <snapshot-file>" >&2
    exit 2
fi

production_paths() {
    local p
    local -a candidates=(
        "${T2S_HDB_DIR:-}"
        "${T2S_TMP_DIR:-}"
        "${T2S_WDB_CHECKPOINT:-}"
        "$ROOT/hdb"
        "$ROOT/tmp"
        "$ROOT/kdb/hdb"
        "$ROOT/kdb/wdb.lastTpSeqNo"
        "$ROOT/kdb/tick/logs"
    )
    for p in "$ROOT"/kdb/tmp.*; do
        [[ -e "$p" ]] && candidates+=("$p")
    done
    for p in "${candidates[@]}"; do
        [[ -n "$p" && -e "$p" ]] && realpath -m "$p"
    done | sort -u
}

snapshot() {
    local p
    while IFS= read -r p; do
        # size and mtime (seconds, with fraction) for every entry, root included
        find "$p" -printf '%p %s %T@\n' 2>/dev/null
    done < <(production_paths) | sort
    # Content hash of every checkpoint file we can find
    while IFS= read -r p; do
        if [[ -d "$p" ]]; then
            find "$p" -maxdepth 1 -name 'wdb.lastTpSeqNo' -type f -exec md5sum {} \; 2>/dev/null
        elif [[ -f "$p" && "$(basename "$p")" == "wdb.lastTpSeqNo" ]]; then
            md5sum "$p"
        fi
    done < <(production_paths) | sort -u
}

case "$MODE" in
    record)
        snapshot > "$SNAP"
        echo "isolation_check: recorded $(wc -l < "$SNAP") entries covering:"
        production_paths | sed 's/^/  /'
        ;;
    compare)
        if [[ ! -f "$SNAP" ]]; then
            echo "isolation_check: snapshot $SNAP missing" >&2
            exit 1
        fi
        NOW=$(mktemp)
        snapshot > "$NOW"
        if diff -u "$SNAP" "$NOW" > "$NOW.diff"; then
            echo "isolation_check: OK - production state unchanged ($(wc -l < "$NOW") entries)"
            rm -f "$NOW" "$NOW.diff"
            exit 0
        fi
        echo "=================================================================="
        echo "isolation_check: FAIL - production state CHANGED during the test run"
        echo "=================================================================="
        cat "$NOW.diff"
        rm -f "$NOW" "$NOW.diff"
        exit 1
        ;;
    *)
        echo "usage: $0 record|compare <snapshot-file>" >&2
        exit 2
        ;;
esac
