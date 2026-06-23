cmake_minimum_required(VERSION 3.10)
project(datafeed)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

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

# Broker library
add_library(broker STATIC
    ${CMAKE_SOURCE_DIR}/broker/src/adapter/eadapter.cpp
    ${CMAKE_SOURCE_DIR}/broker/src/exchange/exchange1.cpp
    ${CMAKE_SOURCE_DIR}/broker/src/exchange/exchange2.cpp
    ${CMAKE_SOURCE_DIR}/broker/src/exchange/exchange3.cpp
    ${CMAKE_SOURCE_DIR}/broker/src/exchange/exchange4.cpp
)

# EAdapter executable (exchange adapter)
add_executable(eadapter
    ${CMAKE_SOURCE_DIR}/main.cpp
)

# Datafeed executable (main server application)
add_executable(datafeed
    ${CMAKE_SOURCE_DIR}/server/server.cpp
    ${CMAKE_SOURCE_DIR}/session/session_manager.cpp
    ${CMAKE_SOURCE_DIR}/live_source/live_source.cpp
    ${CMAKE_SOURCE_DIR}/backtest_source/backtest_source.cpp
    ${CMAKE_SOURCE_DIR}/utils/error.cpp
)

# Include directories
target_include_directories(broker
    PRIVATE
    ${CMAKE_SOURCE_DIR}/broker/include
    ${PostgreSQL_INCLUDE_DIRS}
    ${PQXX_INCLUDE_DIR}
)

target_include_directories(eadapter
    PRIVATE
    ${CMAKE_SOURCE_DIR}/broker/include
    ${Boost_INCLUDE_DIRS}
    ${CMAKE_SOURCE_DIR}
    nlohmann_json::nlohmann_json
)

target_include_directories(datafeed
    PRIVATE
    ${Boost_INCLUDE_DIRS}
    ${CMAKE_SOURCE_DIR}
    nlohmann_json::nlohmann_json
    ${PostgreSQL_INCLUDE_DIRS}
    ${PQXX_INCLUDE_DIR}
)

# Link libraries
target_link_libraries(broker
    PRIVATE
    ${Boost_LIBRARIES}
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
    nlohmann_json::nlohmann_json
)

target_link_libraries(eadapter
    PRIVATE
    broker
    ${Boost_LIBRARIES}
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
    ${PQXX_LIB}
    PostgreSQL::PostgreSQL
)

target_link_libraries(datafeed
    PRIVATE
    ${Boost_LIBRARIES}
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
    PostgreSQL::PostgreSQL
    broker
)