#include "dadapter.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <stdexcept>
#include <sstream>

DAdapter::DAdapter(const std::string& connection_string)
    : connection_string_(connection_string) {}

DAdapter::~DAdapter() {
    stop();
}

void DAdapter::set_symbols(const std::vector<std::string>& symbols) {
    symbols_ = symbols;
}

void DAdapter::set_date_range(const std::string& start_date, const std::string& end_date) {
    start_date_ = start_date;
    end_date_ = end_date;
}

void DAdapter::set_timeframe(const std::string& timeframe) {
    timeframe_ = timeframe;
}

void DAdapter::set_callback(ExternalCallback cb) {
    external_cb_ = std::move(cb);
}

void DAdapter::connect_to_database() {
    try {
        conn_ = std::make_unique<pqxx::connection>(connection_string_);
        if (conn_->is_open()) {
            std::cout << "[DAdapter] Connected to PostgreSQL database" << std::endl;
        } else {
            throw std::runtime_error("Failed to connect to PostgreSQL database");
        }
    } catch (const std::exception& e) {
        std::cerr << "[DAdapter] Database connection error: " << e.what() << std::endl;
        throw;
    }
}

void DAdapter::subscribe_symbols() {
    // For database adapter, we don't subscribe in the traditional sense
    // We just store the symbols for querying
    std::cout << "[DAdapter] Prepared to query data for " << symbols_.size() << " symbols" << std::endl;
}

void DAdapter::on_update() {
    // Setup the callback for when data is ready
    // This is called before starting the main loop
}

void DAdapter::query_historical_data() {
    if (!conn_ || !conn_->is_open()) {
        std::cerr << "[DAdapter] Database not connected" << std::endl;
        return;
    }

    try {
        // Start a non-transactional query
        pqxx::work txn(*conn_);

        // Build the query based on timeframe
        std::string query = "SELECT symbol, price, bid, ask, quantity, timestamp FROM market_data ";
        query += "WHERE symbol = ANY($1) AND timestamp >= $2 AND timestamp <= $3 ";

        // Add timeframe filtering if needed
        if (!timeframe_.empty()) {
            // For simplicity, we'll assume the data is already aggregated by timeframe
            // In a real implementation, you might need to aggregate raw data
            query += "AND timeframe = $4 ";
        }
        query += "ORDER BY timestamp ASC";

        // Prepare the parameters
        pqxx::result result;

        if (!timeframe_.empty()) {
            result = txn.exec_params(query,
                                   pqxx::array_parse(symbols_),
                                   start_date_,
                                   end_date_,
                                   timeframe_);
        } else {
            result = txn.exec_params(query,
                                   pqxx::array_parse(symbols_),
                                   start_date_,
                                   end_date_);
        }

        txn.commit();

        // Process the results and send to callback
        for (const auto& row : result) {
            MarketData data;
            data.symbol = row["symbol"].as<std::string>();
            data.price = row["price"].as<double>();
            data.bid = row["bid"].as<double>();
            data.ask = row["ask"].as<double>();
            data.quantity = row["quantity"].as<int>();
            data.timestamp = row["timestamp"].as<uint64_t>();

            if (external_cb_) {
                external_cb_(data);
            }

            // Small delay to simulate streaming
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        std::cout << "[DAdapter] Finished querying historical data. Retrieved "
                  << result.size() << " records." << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "[DAdapter] Error querying historical data: " << e.what() << std::endl;
    }
}

void DAdapter::run() {
    std::cout << "[DAdapter] Starting historical data query..." << std::endl;

    // Setup callback for data updates
    on_update();

    // Connect to database
    connect_to_database();

    // Prepare symbols (subscribe)
    subscribe_symbols();

    running_ = true;

    // Main loop - query historical data and send via callback
    while (running_) {
        query_historical_data();

        // For historical data, we typically query once and then stop
        // Unless we want to continuously stream new data
        // For now, we'll break after one query
        break;

        // If we wanted to continuously poll for new data:
        // std::this_thread::sleep_for(std::chrono::seconds(5));
    }
}

void DAdapter::stop() {
    running_ = false;
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
    if (conn_ && conn_->is_open()) {
        conn_->disconnect();
    }
    std::cout << "[DAdapter] Stopped" << std::endl;
}

// Private helpers
void DAdapter::_connect() { connect_to_database(); }
void DAdapter::_subscribe(const std::vector<std::string>& syms) { symbols_ = syms; subscribe_symbols(); }
void DAdapter::_set_callback() { on_update(); }