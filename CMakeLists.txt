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


include_directories(broker/include)

# Standard discovery for PostgreSQL
find_package(PostgreSQL REQUIRED)

# Discovery for libpqxx
find_library(PQXX_LIB pqxx)
find_path(PQXX_INCLUDE_DIR pqxx/pqxx)

add_library(broker STATIC
    broker/src/adapter/eadapter.cpp
    broker/src/exchange/exchange1.cpp
    broker/src/exchange/exchange2.cpp
    broker/src/exchange/exchange3.cpp
)

if(NOT PQXX_LIB OR NOT PQXX_INCLUDE_DIR)
    message(FATAL_ERROR "libpqxx not found. Please install libpqxx-dev (Ubuntu) or libpqxx (macOS)")
endif()

include_directories(
    ${PostgreSQL_INCLUDE_DIRS}
    ${PQXX_INCLUDE_DIR}
    ${CMAKE_SOURCE_DIR}/broker/include
)

add_executable(eadapter main.cpp)

add_executable(datafeed 
    server/server.cpp
    session/session_manager.cpp
    live_source/live_source.cpp
    backtest_source/backtest_source.cpp

    broker/src/adapter/eadapter.cpp
    sadapter.cpp

    api/router/router.cpp
    api/controllers/FeedController.cpp
    api/controllers/DashboardController.cpp
    api/controllers/MonitorController.cpp
    api/controllers/AlertsController.cpp
    api/controllers/ConfigController.cpp
    api/controllers/SearchController.cpp
    api/services/FeedService.cpp
    api/services/ClientService.cpp
    api/services/MonitorService.cpp
    api/services/AlertsService.cpp
    api/services/ConfigService.cpp
    api/services/SearchService.cpp

    adapters/dadapter/dadapter.cpp

    metrics/metrics_collector.cpp
    metrics/latency_tracker.cpp
    metrics/throughput_tracker.cpp
    metrics/system_monitor.cpp
    metrics/exchange_monitor.cpp
    metrics/queue_monitor.cpp
    metrics/feed_health_monitor.cpp
    metrics/session_monitor.cpp
    metrics/network_monitor.cpp
    metrics/database_monitor.cpp
    metrics/snapshot_scheduler.cpp
    metrics/snapshot_converter.cpp
    metrics/metric_aggregator.cpp
    metrics/state_manager.cpp
    metrics/weekly_rollup.cpp

    broker/src/exchange/exchange1.cpp
    broker/src/exchange/exchange2.cpp
    broker/src/exchange/exchange3.cpp
    )



target_link_libraries(datafeed 
    PRIVATE
    ${Boost_LIBRARIES}
    ${PQXX_LIB}
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
    PostgreSQL::PostgreSQL
)

# target_link_libraries(dadapter 
#     PRIVATE
#     ${Boost_LIBRARIES}
#     OpenSSL::SSL
#     OpenSSL::Crypto
#     Threads::Threads
#     ${PQXX_LIB}
#     PostgreSQL::PostgreSQL
# )

target_link_libraries(broker
    ${Boost_LIBRARIES}
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
    nlohmann_json::nlohmann_json

)

target_link_libraries(eadapter
    broker
    ${Boost_LIBRARIES}
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
    nlohmann_json::nlohmann_json

)

target_include_directories(datafeed PRIVATE 
    ${Boost_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/metrics
    nlohmann_json::nlohmann_json
    )

# target_include_directories(dadapter PRIVATE 
#     ${Boost_INCLUDE_DIRS}
#     ${CMAKE_CURRENT_SOURCE_DIR}
#     )
