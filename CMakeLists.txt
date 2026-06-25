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
find_package(PkgConfig REQUIRED)
pkg_check_modules(PQXX REQUIRED libpqxx)

# ─────────────────────────────────────────────
# Broker static library (exchange adapters)
# ─────────────────────────────────────────────
add_library(broker STATIC
    ${CMAKE_SOURCE_DIR}/broker/src/adapter/eadapter.cpp
    ${CMAKE_SOURCE_DIR}/broker/src/exchange/exchange1.cpp
    ${CMAKE_SOURCE_DIR}/broker/src/exchange/exchange2.cpp
    ${CMAKE_SOURCE_DIR}/broker/src/exchange/exchange3.cpp
)

target_include_directories(broker
    PUBLIC
    ${CMAKE_SOURCE_DIR}/broker/include
    PRIVATE
    ${Boost_INCLUDE_DIRS}
    ${PostgreSQL_INCLUDE_DIRS}
    ${PQXX_INCLUDE_DIR}
)

target_link_libraries(broker
    PUBLIC
    ${Boost_LIBRARIES}
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
    nlohmann_json::nlohmann_json
)

# ─────────────────────────────────────────────
# eadapter CLI (standalone broker test binary)
# ─────────────────────────────────────────────
add_executable(eadapter
    ${CMAKE_SOURCE_DIR}/main.cpp
)

target_include_directories(eadapter
    PRIVATE
    ${CMAKE_SOURCE_DIR}/broker/include
    ${CMAKE_SOURCE_DIR}
    ${Boost_INCLUDE_DIRS}
)

target_link_libraries(eadapter
    PRIVATE
    broker
    ${Boost_LIBRARIES}
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
)

# ─────────────────────────────────────────────
# datafeed server (main application)
# ─────────────────────────────────────────────
add_executable(datafeed
    # Server core
    ${CMAKE_SOURCE_DIR}/server/server.cpp
    ${CMAKE_SOURCE_DIR}/session/session_manager.cpp
    ${CMAKE_SOURCE_DIR}/utils/error.cpp

    # Data sources
    ${CMAKE_SOURCE_DIR}/live_source/live_source.cpp
    ${CMAKE_SOURCE_DIR}/backtest_source/backtest_source.cpp

    # Database ORM adapter
    ${CMAKE_SOURCE_DIR}/sadapter.cpp

    # Database history adapter (used by backtest_source)
    ${CMAKE_SOURCE_DIR}/adapters/dadapter/dadapter.cpp

    # REST API layer
    ${CMAKE_SOURCE_DIR}/api/router/router.cpp
    ${CMAKE_SOURCE_DIR}/api/controllers/FeedController.cpp
    ${CMAKE_SOURCE_DIR}/api/services/FeedService.cpp
    ${CMAKE_SOURCE_DIR}/api/services/ClientService.cpp
)

target_include_directories(datafeed
    PRIVATE
    ${PQXX_INCLUDE_DIRS}
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/broker/include
    ${Boost_INCLUDE_DIRS}
    ${PostgreSQL_INCLUDE_DIRS}
)

# nlohmann_json via target (adds include dirs transitively)
target_link_libraries(datafeed
    PRIVATE
    broker
    ${Boost_LIBRARIES}
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
    ${PQXX_LIBRARIES}
    PostgreSQL::PostgreSQL
    nlohmann_json::nlohmann_json
)
