#!/bin/bash
# pipeline_state.sh - is the pipeline running? Prints one word:
#
#   stopped   nothing is running and nothing failed: no TP, no WDB, no feed
#             handler, no active or failed systemd unit, no tmux session.
#             This is the normal resting state when the pipeline is run on
#             demand with ./start.sh and ./stop.sh.
#   running   TP and WDB are both up
#   failed    a systemd unit is in the failed state
#   partial   anything else (some processes up, others not; a unit starting)
#
# Used by status.sh (a stopped pipeline is not a problem) and check_eod.sh (a
# day that was not rolled because the pipeline is stopped is pending, not
# failed). Ports come from T2S_TP_PORT / T2S_WDB_PORT like everywhere else.
#
# T2S_STATE_PORTS_ONLY=1 (tests): judge by the two ports only, ignoring the
# real systemd units, handler processes and tmux session of this machine.

set -u
BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT_TP=${T2S_TP_PORT:-5010}
PORT_WDB=${T2S_WDB_PORT:-5011}
listener() { lsof -ti TCP:"$1" -sTCP:LISTEN 2>/dev/null | head -1; }

TP=$(listener "$PORT_TP"); WDB=$(listener "$PORT_WDB")
OTHER=0; FAILED=0
if [[ "${T2S_STATE_PORTS_ONLY:-0}" != "1" ]]; then
    for b in trade_feed_handler trade_feed_handler_fut quote_feed_handler quote_feed_handler_fut; do
        pgrep -f "^(\./|$BASEDIR/)?build/$b( |\$)" >/dev/null 2>&1 && OTHER=1
    done
    if systemctl --user cat t2s-tp.service >/dev/null 2>&1; then
        for u in tp wdb trade-fh quote-fh trade-fh-fut quote-fh-fut; do
            st=$(systemctl --user show -p ActiveState --value "t2s-$u.service" 2>/dev/null)
            case "$st" in
                failed) FAILED=1 ;;
                inactive|"") ;;
                *) OTHER=1 ;;            # active, activating, deactivating, reloading
            esac
        done
    fi
    tmux has-session -t t2s 2>/dev/null && OTHER=1
fi

if   [[ $FAILED -eq 1 ]]; then echo failed
elif [[ -n "$TP" && -n "$WDB" ]]; then echo running
elif [[ -z "$TP" && -z "$WDB" && $OTHER -eq 0 ]]; then echo stopped
else echo partial
fi
