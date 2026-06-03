# t2s MCP server — volatility query surface

A minimal [MCP](https://modelcontextprotocol.io) server that exposes the running
**RTE** process (`kdb/analytics/rte.q`, IPC port **5015**) to an MCP host such as
Claude Desktop. It provides one read-only tool, `get_volatility`, which calls the
existing q function `.rte.getVol[]` — no analytics are recomputed in Python.

## What it does

`get_volatility(symbol?)`

- With a `symbol` (e.g. `"BTCUSDT"`, case-insensitive): returns that symbol's
  current annualized realized volatility.
- Without a symbol: returns volatility for **all** tracked symbols.

Each result includes `annualizedVol`, `returnCount`, `isValid`, and a readable
`note`. `isValid` is `True` only once the 60-minute rolling window is fully
loaded (~120 returns, sampled every 30s). For the first ~60 minutes after
startup (or after an EOD reset / reconnect) readings are reported but flagged as
warming up.

## Prerequisites

- The RTE process running and reachable at `127.0.0.1:5015` (in the same WSL
  environment as this server).
- Python 3.10+ with `pykx` (KDB-X integration) and `mcp` installed.

## Install

```bash
cd ~/t2s
source .venv/bin/activate
pip install -r mcp/requirements.txt   # mcp; pykx already present
```

If the RTE runs on a different host/port, override via env vars:

```bash
export RTE_HOST=127.0.0.1
export RTE_PORT=5015
```

## Test it without Claude (recommended first)

Use the MCP Inspector — it talks to the server directly so you can confirm the
IPC + tool work before wiring up a host. Needs Node.js:

```bash
npx @modelcontextprotocol/inspector \
  ~/t2s/.venv/bin/python ~/t2s/mcp/server.py
```

In the Inspector UI: open the **Tools** tab, call `get_volatility` with no args
(all symbols) and with `symbol=BTCUSDT`. Make sure the RTE is up first.

## Wire into Claude Desktop (Windows host + WSL server)

Claude Desktop runs on Windows; the server runs in WSL. Launch it through
`wsl.exe`. Edit Claude Desktop's config:

`%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "t2s-volatility": {
      "command": "wsl.exe",
      "args": [
        "-d", "Ubuntu-22.04",
        "--",
        "/home/philippe/t2s/.venv/bin/python",
        "/home/philippe/t2s/mcp/server.py"
      ]
    }
  }
}
```

Adjust `-d Ubuntu-22.04` to your WSL distro name (`wsl.exe -l -q` to list) and
the two paths to your actual home. Restart Claude Desktop; you should see the
tool available. Then ask, e.g. *"what's the annualized vol for BTC right now?"*

## Notes

- Read-only. It can only call `.rte.getVol[]`.
- A fresh IPC connection is opened per call, so an RTE restart is handled
  transparently on the next query.
- This server is a **sidecar** — it is not in the tick hot path and does not
  affect pipeline throughput.
