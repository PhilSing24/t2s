"""
t2s MCP server — real-time volatility query surface.

Exposes the running RTE process (kdb/analytics/rte.q, IPC port 5015) to an MCP
host (e.g. Claude Desktop) as a single read-only tool: get_volatility.

Design notes
------------
- Sidecar pattern: this is a separate process that connects to the RTE as an
  ordinary kdb+ IPC client. It is NOT in the tick hot path.
- The volatility itself is NOT recomputed here. We call the existing q function
  .rte.getVol[], which returns one row per symbol:
      sym | annualizedVol | returnCount | isValid
  computed over a trailing 60-minute rolling window (~120 returns at steady
  state; one return sampled every 30s).
- We surface isValid / returnCount so the host can tell a steady-state reading
  from one that is still warming up (the first ~60 minutes after startup, or
  after an EOD reset / reconnect).
"""

from __future__ import annotations

import os
import sys
import contextlib

# IMPORTANT: PyKX prints a startup banner to stdout on import (the KDB-X
# Community Edition welcome message). MCP communicates over stdio and expects
# ONLY clean JSON on stdout, so any banner would corrupt the protocol when the
# server is launched by a host such as Claude Desktop.
#
# Two layers of defence:
#   1. Ask PyKX to stay quiet via env vars (must be set BEFORE importing pykx).
#   2. Redirect stdout -> stderr for the duration of the import, so anything
#      printed regardless of the flags lands on stderr (which the host ignores)
#      instead of stdout.
os.environ.setdefault("PYKX_SUPPRESS_WARNINGS", "true")

from typing import Any

with contextlib.redirect_stdout(sys.stderr):
    import pykx as kx

from mcp.server.fastmcp import FastMCP

# --- Configuration -----------------------------------------------------------
# Host/port of the running RTE process. Defaults to localhost:5015 (rte.q's
# .rte.cfg.port). Override via env vars if the RTE runs elsewhere.
RTE_HOST = os.environ.get("RTE_HOST", "127.0.0.1")
RTE_PORT = int(os.environ.get("RTE_PORT", "5015"))

# Steady-state target (rte.q .rte.cfg.volTargetCount = 60*60/30 = 120).
# Used only to phrase how "loaded" a warming-up reading is.
VOL_TARGET_COUNT = 120

mcp = FastMCP("t2s-volatility")


def _get_conn() -> kx.SyncQConnection:
    """Open a short-lived IPC connection to the RTE.

    A fresh connection per call keeps the server stateless and resilient: if the
    RTE restarts, the next call simply reconnects rather than holding a dead
    handle. Volatility queries are infrequent (driven by chat), so the connect
    cost is irrelevant here.
    """
    return kx.SyncQConnection(host=RTE_HOST, port=RTE_PORT)


def _fetch_vol_rows() -> list[dict[str, Any]]:
    """Call .rte.getVol[] and return its rows as a list of plain dicts."""
    with _get_conn() as conn:
        # .rte.getVol takes no args; pass the q identity (::) to invoke it.
        tbl = conn(".rte.getVol", kx.q("::"))
    df = tbl.pd()  # -> pandas DataFrame: sym, annualizedVol, returnCount, isValid

    rows: list[dict[str, Any]] = []
    for _, r in df.iterrows():
        rows.append(
            {
                # sym comes back as a kdb symbol -> ensure plain str
                "symbol": str(r["sym"]),
                "annualizedVol": float(r["annualizedVol"]),
                "returnCount": int(r["returnCount"]),
                "isValid": bool(r["isValid"]),
            }
        )
    return rows


@mcp.tool()
def get_volatility(symbol: str | None = None) -> dict[str, Any]:
    """Get the current annualized realized volatility for one or all symbols.

    The value is computed by the real-time engine over a trailing 60-minute
    rolling window (volatility is sampled every 30 seconds; ~120 samples at
    steady state). Each call uses the same method, so repeated readings are
    directly comparable.

    Args:
        symbol: Optional ticker, e.g. "BTCUSDT". Case-insensitive. If omitted,
            volatility for ALL tracked symbols is returned.

    Returns:
        A dict with a "results" list; each entry has:
          - symbol:        the ticker
          - annualizedVol: annualized volatility in percent (e.g. 58.3)
          - returnCount:   number of returns the estimate is based on
          - isValid:       True once the 60-minute window is fully loaded
                           (returnCount >= 120); False while still warming up
          - note:          human-readable status (steady-state vs warming up)
        Plus a top-level "message" when no data matches.
    """
    try:
        rows = _fetch_vol_rows()
    except Exception as exc:  # connection refused, RTE down, etc.
        return {
            "results": [],
            "message": (
                f"Could not reach the real-time engine at {RTE_HOST}:{RTE_PORT} "
                f"({exc}). Is the RTE process running?"
            ),
        }

    # No symbols reported at all -> nothing has crossed the minimum-returns
    # threshold yet (every symbol still under ~10 returns / ~5 minutes).
    if not rows:
        return {
            "results": [],
            "message": (
                "No volatility available yet. The engine needs a minimum number "
                "of return samples (~5 minutes of trading) before reporting any "
                "value."
            ),
        }

    # Optional symbol filter (case-insensitive).
    if symbol is not None:
        want = symbol.strip().upper()
        rows = [r for r in rows if r["symbol"].upper() == want]
        if not rows:
            return {
                "results": [],
                "message": (
                    f"No volatility for '{symbol}'. Either it isn't subscribed, "
                    f"or it's still warming up (under the minimum return count)."
                ),
            }

    # Annotate each row with a readable status note.
    for r in rows:
        if r["isValid"]:
            r["note"] = "steady-state (60-minute window fully loaded)"
        else:
            r["note"] = (
                f"warming up: {r['returnCount']} of {VOL_TARGET_COUNT} returns "
                f"(~{r['returnCount'] * 30 // 60} min of data) — treat as preliminary"
            )

    return {"results": rows}


if __name__ == "__main__":
    # Runs over stdio, which is what Claude Desktop launches and speaks.
    mcp.run()
