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
# Executable: trade feed handler
# ---------------------------------------------------------------------------
add_executable(trade_feed_handler
    cpp/src/trade_fh_main.cpp
)

target_link_libraries(trade_feed_handler PRIVATE t2s_fh)

# ---------------------------------------------------------------------------
# Executable: L5 quote feed handler
# ---------------------------------------------------------------------------
add_executable(quote_feed_handler
    cpp/src/quote_fh_main.cpp
)

target_link_libraries(quote_feed_handler PRIVATE t2s_fh)

# ---------------------------------------------------------------------------
# Test binary: order book unit tests
#
# Built only when Catch2 amalgamation files are present at
# cpp/third_party/catch2/. See tests/README or the project README for
# download instructions.
#
# OrderBookManager is header-only, so this test does not link the
# t2s_fh lib - it just includes the header directly.
# ---------------------------------------------------------------------------
if(EXISTS ${CATCH2_DIR}/catch_amalgamated.cpp)
    add_executable(test_order_book
        tests/test_order_book.cpp
        ${CATCH2_DIR}/catch_amalgamated.cpp
    )

    target_include_directories(test_order_book PRIVATE
        ${INCLUDE_DIR}
        ${CATCH2_DIR}
    )
else()
    message(STATUS "Catch2 not found at ${CATCH2_DIR} - skipping test_order_book target")
    message(STATUS "  Download catch_amalgamated.hpp/.cpp from")
    message(STATUS "  https://github.com/catchorg/Catch2/releases (v3.x)")
endif()
