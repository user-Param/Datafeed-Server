cmake_minimum_required(VERSION 3.10)
project(datafeed)

set(CMAKE_CXX_STANDARD 17)

# Find packages
find_package(Boost 1.74 REQUIRED COMPONENTS
    thread
    chrono
    date_time
    atomic
)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)
find_package(nlohmann_json REQUIRED)

# Standard discovery for PostgreSQL
find_package(PostgreSQL REQUIRED)

# Discovery for libpqxx
find_library(PQXX_LIB pqxx)
find_path(PQXX_INCLUDE_DIR pqxx/pqxx)

if(NOT PQXX_LIB OR NOT PQXX_INCLUDE_DIR)
    message(FATAL_ERROR "libpqxx not found. Please install libpqxx-dev (Ubuntu) or libpqxx (macOS)")
endif()

include_directories(
    ${PostgreSQL_INCLUDE_DIRS}
    ${PQXX_INCLUDE_DIR}
    ${CMAKE_SOURCE_DIR}/../broker/include
)

add_executable(datafeed 
    server/server.h
    server/server.cpp
    session/session_manager.cpp
    live_source/live_source.cpp
    backtest_source/backtest_source.cpp

    ../broker/src/adapter/eadapter.cpp
    ../broker/src/exchange/exchange1.cpp
    ../broker/src/exchange/exchange2.cpp
    ../broker/src/exchange/exchange3.cpp
    )

add_executable(dadapter
    ../database/Dadapter.cpp
)

target_link_libraries(datafeed 
    PRIVATE
    ${Boost_LIBRARIES}
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
    PostgreSQL::PostgreSQL
)

target_link_libraries(dadapter 
    PRIVATE
    ${Boost_LIBRARIES}
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
    ${PQXX_LIB}
    PostgreSQL::PostgreSQL
)

target_include_directories(datafeed PRIVATE 
    ${Boost_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}
    nlohmann_json::nlohmann_json
    )

target_include_directories(dadapter PRIVATE 
    ${Boost_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}
    )
