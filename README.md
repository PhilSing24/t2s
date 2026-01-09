# Tick to Signal

A production-grade, real-time Binance market data pipeline built with C++ and kdb+/KDB-X. Features dual feed handlers (trades + L5 order book), comprehensive telemetry, real-time analytics, and log replay for crash recovery.

Inspired by *Building Real-Time Event-Driven KDB-X Systems* by Data Intellect.

## Architecture
```
Binance Trade Stream ──WebSocket──► Trade FH ──┬──IPC──► TP:5010 ──┬──► RDB:5011 (storage)
                                               │                   │
Binance Depth Stream ──WebSocket──► Quote FH ──┘                   ├──► RTE:5012 (analytics)
         │                                                         │
         └──REST (snapshot)                                        ├──► TEL:5013 (telemetry)
                                                                   │
                                                                   └──► LOG:5014 (log manager)
```

| Component | Port | Role |
|-----------|------|------|
| Trade FH | - | Trade feed handler (C++) |
| Quote FH | - | Quote feed handler with L5 order book (C++) |
| TP | 5010 | Tickerplant - pub/sub hub with logging |
| RDB | 5011 | Real-time database with log replay |
| RTE | 5012 | Real-time engine - VWAP & order book imbalance |
| TEL | 5013 | Telemetry aggregation |
| LOG | 5014 | Log manager |
| CTL | 5000 | Control process |

## Prerequisites

- kdb+ 4.x
- C++17 compiler
- CMake 3.16+
- Boost (Beast, Asio), OpenSSL, RapidJSON, spdlog

## Build
```bash
cmake -S . -B build
cmake --build build
```

## Run
```bash
# Start all (tmux)
./start.sh

# Start all (background)
./start_bg.sh

# Stop all
./stop.sh
```

## Query Interfaces
```q
// RDB (port 5011)
select from trade_binance where sym=`BTCUSDT

// RTE (port 5012)
.rte.getVwap[`BTCUSDT; 5]
.rte.getImbalance[`BTCUSDT]

// TEL (port 5013)
.tel.vsFhStatus[]
.tel.vsSystemResources[]
```

## Project Structure
```
tick-to-signal/
├── cpp/                    # C++ feed handlers
│   ├── src/
│   ├── include/
│   └── CMakeLists.txt
├── kdb/                    # kdb+ processes
│   ├── tp.q                # Tickerplant
│   ├── rdb.q               # Real-time database
│   ├── rte.q               # Real-time engine
│   ├── tel.q               # Telemetry
│   ├── logmgr.q            # Log manager
│   ├── ctl.q               # Control
│   └── u.q                 # Pub/sub utilities
├── config/                 # JSON configs for feed handlers
├── logs/                   # Runtime logs
├── start.sh
├── start_bg.sh
├── stop.sh
└── CMakeLists.txt
```

## Documentation

Architecture Decision Records and white paper available at [tick-to-signal-docs](https://github.com/PhilSing24/tick-to-signal-docs).

## License

Private repository.
