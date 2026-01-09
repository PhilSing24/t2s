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

# Common libraries for both handlers
set(COMMON_LIBS
    ${KDB_DIR}/c.o
    Boost::system
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
    spdlog::spdlog
)

# Trade feed handler
add_executable(trade_feed_handler
    cpp/src/trade_feed_handler.cpp
)

target_include_directories(trade_feed_handler PRIVATE
    ${INCLUDE_DIR}
    ${KDB_DIR}
)

target_link_libraries(trade_feed_handler ${COMMON_LIBS})

# Quote feed handler
add_executable(quote_feed_handler
    cpp/src/quote_feed_handler.cpp
)

target_include_directories(quote_feed_handler PRIVATE
    ${INCLUDE_DIR}
    ${KDB_DIR}
)

target_link_libraries(quote_feed_handler ${COMMON_LIBS})