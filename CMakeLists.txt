cmake_minimum_required(VERSION 3.16)

project(binance_feed_handlers LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Boost REQUIRED COMPONENTS system)
find_package(spdlog REQUIRED)

# Include directories
set(INCLUDE_DIR ${PROJECT_SOURCE_DIR}/cpp/include)
set(KDB_DIR ${PROJECT_SOURCE_DIR}/cpp/third_party/kdb)
set(CATCH2_DIR ${PROJECT_SOURCE_DIR}/cpp/third_party/catch2)

# Common libraries linked by every binary that uses the FH classes
set(COMMON_LIBS
    ${KDB_DIR}/c.o
    Boost::system
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
    spdlog::spdlog
)

# ---------------------------------------------------------------------------
# Static library: feed handler classes
# ---------------------------------------------------------------------------
add_library(t2s_fh STATIC
    cpp/src/trade_feed_handler.cpp
    cpp/src/quote_feed_handler.cpp
)

target_include_directories(t2s_fh PUBLIC
    ${INCLUDE_DIR}
    ${KDB_DIR}
)

target_link_libraries(t2s_fh PUBLIC ${COMMON_LIBS})

# ---------------------------------------------------------------------------
# Executable: spot trade feed handler
# ---------------------------------------------------------------------------
add_executable(trade_feed_handler
    cpp/src/trade_fh_main.cpp
)

target_link_libraries(trade_feed_handler PRIVATE t2s_fh)

# ---------------------------------------------------------------------------
# Executable: futures (USDT-M aggTrade) trade feed handler
# Both spot and futures binaries link the same parameterized
# TradeFeedHandler from t2s_fh; they differ only in default config path.
# ---------------------------------------------------------------------------
add_executable(trade_feed_handler_fut
    cpp/src/trade_fh_fut_main.cpp
)

target_link_libraries(trade_feed_handler_fut PRIVATE t2s_fh)

# ---------------------------------------------------------------------------
# Executable: L5 quote feed handler
# ---------------------------------------------------------------------------
add_executable(quote_feed_handler
    cpp/src/quote_fh_main.cpp
)

target_link_libraries(quote_feed_handler PRIVATE t2s_fh)

# ============================================================
# Executable: USD-M futures L5 quote feed handler
# Same QuoteFeedHandler class as the spot binary; the market
# wiring comes from config/quote_feed_handler_fut.json.
# ============================================================
add_executable(quote_feed_handler_fut
    cpp/src/quote_fh_fut_main.cpp
)
target_link_libraries(quote_feed_handler_fut PRIVATE t2s_fh)

# ---------------------------------------------------------------------------
# Test binaries
#
# Built only when Catch2 amalgamation files are present at
# cpp/third_party/catch2/. See tests/README or the project README for
# download instructions.
# ---------------------------------------------------------------------------
# ============================================================
# Test publisher: synthetic trade rows through the real TpPublisher.
# Used by the sandbox tests; not part of the pipeline.
# ============================================================
add_executable(sim_trade_publisher
    cpp/tools/sim_trade_publisher.cpp
)
target_link_libraries(sim_trade_publisher PRIVATE t2s_fh)

if(EXISTS ${CATCH2_DIR}/catch_amalgamated.cpp)

    # OrderBookManager is header-only, so this test does not link the
    # t2s_fh lib - it just includes the header directly.
    add_executable(test_order_book
        tests/test_order_book.cpp
        ${CATCH2_DIR}/catch_amalgamated.cpp
    )

    target_include_directories(test_order_book PRIVATE
        ${INCLUDE_DIR}
        ${CATCH2_DIR}
    )

    add_executable(test_snapshot_worker
        tests/test_snapshot_worker.cpp
        ${CATCH2_DIR}/catch_amalgamated.cpp
    )

    target_include_directories(test_snapshot_worker PRIVATE
        ${INCLUDE_DIR}
        ${CATCH2_DIR}
    )

    target_link_libraries(test_snapshot_worker PRIVATE
        Threads::Threads
        spdlog::spdlog
        Boost::system
        OpenSSL::SSL
        OpenSSL::Crypto
    )

    # JsonReader is a header-only, rapidjson-only utility, so this test
    # doesn't need any of the FH/network/kdb link surface - just Catch2
    # and the standard library (rapidjson is header-only).
    add_executable(test_snapshot_scheduler
        tests/test_snapshot_scheduler.cpp
        ${CATCH2_DIR}/catch_amalgamated.cpp
    )
    target_include_directories(test_snapshot_scheduler PRIVATE
        ${INCLUDE_DIR}
        ${CATCH2_DIR}
    )

    add_executable(test_quote_row
        tests/test_quote_row.cpp
        ${CATCH2_DIR}/catch_amalgamated.cpp
    )
    target_include_directories(test_quote_row PRIVATE
        ${INCLUDE_DIR}
        ${KDB_DIR}
        ${CATCH2_DIR}
    )
    target_link_libraries(test_quote_row PRIVATE
        ${KDB_DIR}/c.o
        Threads::Threads
    )

    add_executable(test_resend_ring
        tests/test_resend_ring.cpp
        ${CATCH2_DIR}/catch_amalgamated.cpp
    )
    target_include_directories(test_resend_ring PRIVATE
        ${INCLUDE_DIR}
        ${KDB_DIR}
        ${CATCH2_DIR}
    )
    target_link_libraries(test_resend_ring PRIVATE
        ${KDB_DIR}/c.o
        Threads::Threads
        spdlog::spdlog
    )

    add_executable(test_trade_gap
        tests/test_trade_gap.cpp
        ${CATCH2_DIR}/catch_amalgamated.cpp
    )
    target_include_directories(test_trade_gap PRIVATE
        ${INCLUDE_DIR}
        ${KDB_DIR}
        ${CATCH2_DIR}
    )
    target_link_libraries(test_trade_gap PRIVATE
        ${KDB_DIR}/c.o
        Threads::Threads
        spdlog::spdlog
    )

    add_executable(test_trade_backfill
        tests/test_trade_backfill.cpp
        ${CATCH2_DIR}/catch_amalgamated.cpp
    )
    target_include_directories(test_trade_backfill PRIVATE
        ${INCLUDE_DIR}
        ${KDB_DIR}
        ${CATCH2_DIR}
    )
    target_link_libraries(test_trade_backfill PRIVATE
        ${KDB_DIR}/c.o
        Threads::Threads
        spdlog::spdlog
        Boost::system
        OpenSSL::SSL
        OpenSSL::Crypto
    )

    add_executable(test_json_reader
        tests/test_json_reader.cpp
        ${CATCH2_DIR}/catch_amalgamated.cpp
    )

    target_include_directories(test_json_reader PRIVATE
        ${INCLUDE_DIR}
        ${CATCH2_DIR}
    )

    # Trade FH row construction regression test (ADR-013 step 2 + step 5).
    # Covers both buildTradeRow (spot) and buildAggTradeRow (futures).
    # Locks both row layouts against silent drift.
    # Links c.o directly (needs knk/kj/ks/etc.) but does NOT link t2s_fh -
    # the row builders are self-contained in trade_row.hpp.
    add_executable(test_trade_fh_row_construction
        tests/test_trade_fh_row_construction.cpp
        ${CATCH2_DIR}/catch_amalgamated.cpp
    )

    target_include_directories(test_trade_fh_row_construction PRIVATE
        ${INCLUDE_DIR}
        ${KDB_DIR}
        ${CATCH2_DIR}
    )

    target_link_libraries(test_trade_fh_row_construction PRIVATE
        ${KDB_DIR}/c.o
        Threads::Threads
    )

    # Stream path builder test (ADR-013 step 3).
    # buildStreamPath is a pure string function; no kdb/Boost link needed.
    add_executable(test_stream_path
        tests/test_stream_path.cpp
        ${CATCH2_DIR}/catch_amalgamated.cpp
    )

    target_include_directories(test_stream_path PRIVATE
        ${INCLUDE_DIR}
        ${CATCH2_DIR}
    )

    # Futures aggTrade JSON parse test (ADR-013 step 5).
    # Covers the futures schema extraction path mirror of processMessage.
    # Pure JsonReader/rapidjson test; no kdb link needed.
    add_executable(test_aggtrade_parse
        tests/test_aggtrade_parse.cpp
        ${CATCH2_DIR}/catch_amalgamated.cpp
    )

    target_include_directories(test_aggtrade_parse PRIVATE
        ${INCLUDE_DIR}
        ${CATCH2_DIR}
    )

else()
    message(STATUS "Catch2 not found at ${CATCH2_DIR} - skipping test targets")
    message(STATUS "  Download catch_amalgamated.hpp/.cpp from")
    message(STATUS "  https://github.com/catchorg/Catch2/releases (v3.x)")
endif()
