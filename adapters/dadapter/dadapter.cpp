#include "dadapter.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <stdexcept>
#include <sstream>
#include <iomanip>
#include <fstream>

namespace {

uint64_t date_to_epoch_ms(const std::string& date) {
    std::tm tm = {};
    std::istringstream iss(date);
    iss >> std::get_time(&tm, "%Y-%m-%d");
    if (iss.fail()) {
        throw std::runtime_error("Invalid date format (expected YYYY-MM-DD): " + date);
    }
    tm.tm_hour = 0;
    tm.tm_min = 0;
    tm.tm_sec = 0;
#if defined(_WIN32)
    time_t t = _mkgmtime(&tm);
#else
    time_t t = timegm(&tm);
#endif
    return static_cast<uint64_t>(t) * 1000ULL;
}

} // namespace

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
    std::cout << "[DAdapter] Prepared to query data for " << symbols_.size() << " symbols" << std::endl;
}

void DAdapter::on_update() {}

void DAdapter::query_historical_data() {
    if (!conn_ || !conn_->is_open()) {
        std::cerr << "[DAdapter] Database not connected" << std::endl;
        return;
    }

    if (symbols_.empty()) {
        std::cerr << "[DAdapter] No symbols configured" << std::endl;
        return;
    }

    try {
        const uint64_t start_ts = date_to_epoch_ms(start_date_);
        const uint64_t end_ts   = date_to_epoch_ms(end_date_) + 86400000ULL - 1ULL;

        pqxx::work txn(*conn_);
        pqxx::params p;
        p.append(start_ts);
        p.append(end_ts);

        std::ostringstream query;
        query << "SELECT symbol, price, bid, ask, quantity, timestamp FROM market_data "
              << "WHERE timestamp >= $1 AND timestamp <= $2 AND symbol IN (";
        for (std::size_t i = 0; i < symbols_.size(); ++i) {
            if (i > 0) query << ',';
            query << '$' << (i + 3);
            p.append(symbols_[i]);
        }
        query << ") ";
        if (!timeframe_.empty()) {
            query << "AND timeframe = $" << (symbols_.size() + 3) << ' ';
            p.append(timeframe_);
        }
        query << "ORDER BY timestamp ASC";

        auto result = txn.exec(query.str(), p);
        txn.commit();

        // #region agent log
        {
            std::ofstream dbg("/Users/param/Documents/datafeed/.cursor/debug-627934.log", std::ios::app);
            dbg << "{\"sessionId\":\"627934\",\"hypothesisId\":\"E\",\"location\":\"dadapter.cpp:query_historical_data\","
                << "\"message\":\"historical query result\",\"data\":{\"rows\":" << result.size()
                << ",\"start_ts\":" << start_ts << ",\"end_ts\":" << end_ts
                << ",\"symbols\":" << symbols_.size() << "},\"timestamp\":"
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch()).count()
                << "}\n";
        }
        // #endregion

        for (const auto& row : result) {
            MarketData data;
            data.symbol    = row["symbol"].as<std::string>();
            data.price     = row["price"].as<double>();
            data.bid       = row["bid"].as<double>();
            data.ask       = row["ask"].as<double>();
            data.quantity  = row["quantity"].as<int>();
            data.timestamp = row["timestamp"].as<uint64_t>();

            if (external_cb_) {
                external_cb_(data);
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        std::cout << "[DAdapter] Finished querying historical data. Retrieved "
                  << result.size() << " records." << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "[DAdapter] Error querying historical data: " << e.what() << std::endl;
        // #region agent log
        {
            std::ofstream dbg("/Users/param/Documents/datafeed/.cursor/debug-627934.log", std::ios::app);
            dbg << "{\"sessionId\":\"627934\",\"hypothesisId\":\"E\",\"location\":\"dadapter.cpp:query_historical_data\","
                << "\"message\":\"query error\",\"data\":{\"error\":\"" << e.what() << "\"},\"timestamp\":"
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch()).count()
                << "}\n";
        }
        // #endregion
    }
}

void DAdapter::run() {
    std::cout << "[DAdapter] Starting historical data query..." << std::endl;
    on_update();
    connect_to_database();
    subscribe_symbols();

    running_ = true;
    while (running_) {
        query_historical_data();
        break;
    }
}

void DAdapter::stop() {
    running_ = false;
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
    if (conn_ && conn_->is_open()) {
        conn_->close();
    }
    std::cout << "[DAdapter] Stopped" << std::endl;
}

void DAdapter::_connect() { connect_to_database(); }
void DAdapter::_subscribe(const std::vector<std::string>& syms) { symbols_ = syms; subscribe_symbols(); }
void DAdapter::_set_callback() { on_update(); }
