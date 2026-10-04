#!/bin/bash
# install.sh - install the t2s systemd USER units for this checkout.
#
#   ops/systemd/install.sh            render the units into ~/.config/systemd/user
#                                     and reload the user manager; nothing is
#                                     enabled or started
#   ops/systemd/install.sh --enable   also enable t2s.target and the timers
#                                     (they start with the user manager)
#   ops/systemd/install.sh --render DIR   only write the rendered units into DIR
#   ops/systemd/install.sh --uninstall    stop, disable and remove them
#
# No sudo anywhere. For the units to start at WSL boot without a login the
# user needs lingering, once:   sudo loginctl enable-linger $USER
#
# The units are templates: @T2S_ROOT@ becomes this checkout's path. The
# environment the processes need (PATH with q, QHOME, QPATH, T2S_HDB_DIR,
# T2S_TMP_DIR) is written to ~/.config/t2s/t2s.env from the shell that runs
# this script, if that file does not exist yet; edit it there afterwards.

set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
UNIT_DIR="$HOME/.config/systemd/user"
ENV_FILE="$HOME/.config/t2s/t2s.env"
UNITS=(t2s.target t2s-tp.service t2s-wdb.service t2s-trade-fh.service t2s-quote-fh.service
       t2s-trade-fh-fut.service t2s-quote-fh-fut.service
       t2s-check-eod.service t2s-check-eod.timer t2s-retention.service t2s-retention.timer
       t2s-status.service t2s-status.timer t2s-clock.service t2s-clock.timer)
TIMERS=(t2s-check-eod.timer t2s-retention.timer t2s-status.timer t2s-clock.timer)

render() {  # target dir
    mkdir -p "$1"
    for u in "${UNITS[@]}"; do sed "s|@T2S_ROOT@|$ROOT|g" "$HERE/$u" > "$1/$u"; done
}

case "${1:-}" in
    --render)
        [[ -n "${2:-}" ]] || { echo "usage: $0 --render DIR"; exit 2; }
        render "$2"; echo "rendered ${#UNITS[@]} units into $2"; exit 0 ;;
    --uninstall)
        systemctl --user stop t2s.target "${TIMERS[@]}" 2>/dev/null
        systemctl --user disable t2s.target "${TIMERS[@]}" 2>/dev/null
        for u in "${UNITS[@]}"; do rm -f "$UNIT_DIR/$u"; done
        systemctl --user daemon-reload
        echo "t2s units removed from $UNIT_DIR ($ENV_FILE left in place)"; exit 0 ;;
    ""|--enable) ;;
    *) echo "usage: $0 [--enable | --render DIR | --uninstall]"; exit 2 ;;
esac

if [[ ! -f "$ENV_FILE" ]]; then
    command -v q >/dev/null 2>&1 || { echo "q is not on PATH in this shell - cannot write $ENV_FILE"; exit 1; }
    mkdir -p "$(dirname "$ENV_FILE")"
    {
        echo "# Environment of the t2s systemd units. Written by ops/systemd/install.sh"
        echo "# on $(date -u +%Y-%m-%d) from the installing shell; edit here, then"
        echo "#   systemctl --user daemon-reload && systemctl --user restart t2s.target"
        echo "PATH=$(dirname "$(command -v q)"):/usr/local/bin:/usr/bin:/bin"
        [[ -n "${QHOME:-}" ]] && echo "QHOME=$QHOME"
        [[ -n "${QPATH:-}" ]] && echo "QPATH=$QPATH"
        [[ -n "${QLIC:-}" ]]  && echo "QLIC=$QLIC"
        echo "T2S_HDB_DIR=${T2S_HDB_DIR:-$ROOT/hdb}"
        echo "T2S_TMP_DIR=${T2S_TMP_DIR:-$ROOT/tmp/}"
    } > "$ENV_FILE"
    echo "wrote $ENV_FILE:"; sed 's/^/    /' "$ENV_FILE"
else
    echo "keeping existing $ENV_FILE"
fi

mkdir -p "$ROOT/ops/cron" "$ROOT/run"
render "$UNIT_DIR"
systemctl --user daemon-reload || { echo "systemctl --user daemon-reload failed"; exit 1; }
echo "installed ${#UNITS[@]} units into $UNIT_DIR for $ROOT"

if [[ "${1:-}" == "--enable" ]]; then
    systemctl --user enable t2s.target t2s-tp.service t2s-wdb.service t2s-trade-fh.service t2s-quote-fh.service \
        t2s-trade-fh-fut.service t2s-quote-fh-fut.service "${TIMERS[@]}" || exit 1
    systemctl --user start "${TIMERS[@]}" || exit 1
    echo "enabled: t2s.target (starts with the user manager) and the timers (started now)"
fi
linger=$(loginctl show-user "$USER" -p Linger 2>/dev/null | cut -d= -f2)
[[ "$linger" == "yes" ]] || echo "NOTE: lingering is off - the units will not start at WSL boot until you run: sudo loginctl enable-linger $USER"
exit 0
