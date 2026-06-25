#include "backtest_source.h"
#include <iostream>
#include <sstream>
#include <cstdlib>
#include <thread>
#include <fstream>
#include <chrono>
#include <nlohmann/json.hpp>

backtest_source::backtest_source(std::shared_ptr<session_manager> manager)
    : manager_(manager) {}

void backtest_source::set_db_adapter(std::shared_ptr<datafeed::SAdapter> adapter,
                                     const std::string& instance_id) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    db_ = std::move(adapter);
    instance_id_ = instance_id;
}

std::string backtest_source::db_connection_string() {
    const char* env = std::getenv("DATABASE_URL");
    if (env && *env) return std::string(env);

    const char* host = std::getenv("PGHOST");
    const char* port = std::getenv("PGPORT");
    const char* db   = std::getenv("PGDATABASE");
    const char* user = std::getenv("PGUSER");
    const char* pass = std::getenv("PGPASSWORD");

    std::ostringstream oss;
    oss << "host="     << (host ? host : "localhost")
        << " port="    << (port ? port : "5432")
        << " dbname="  << (db   ? db   : "datafeed")
        << " user="    << (user ? user : "datafeed")
        << " password="<< (pass ? pass : "datafeed");
    return oss.str();
}

void backtest_source::set_feed_status(const std::string& status) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    if (!db_ || !db_->is_connected() || instance_id_.empty()) return;

    datafeed::FeedInstance fi;
    fi.instance_id = instance_id_;
    fi.exchange = "DATABASE";
    fi.adapter_type = "database";
    fi.feed_status = status;
    fi.reconnect_attempts = 0;
    fi.last_tick_at = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    auto existing = db_->get_feed_instance_by_id(instance_id_);
    if (existing) {
        db_->update_feed_instance(fi);
    } else {
        db_->create_feed_instance(fi);
    }
}

void backtest_source::start_replay(const std::string& date_range,
                                   const std::vector<std::string>& symbols,
                                   const std::string& timeframe) {
    std::cout << "[BacktestSource] Starting replay for range: " << date_range << std::endl;

    std::string start_date, end_date;
    auto comma = date_range.find(',');
    if (comma != std::string::npos) {
        start_date = date_range.substr(0, comma);
        end_date   = date_range.substr(comma + 1);
    } else {
        start_date = date_range;
        end_date   = date_range;
    }

    std::vector<std::string> syms = symbols.empty()
        ? std::vector<std::string>{"BTCUSDT", "ETHUSDT", "SOLUSDT"}
        : symbols;

    stop_replay();
    set_feed_status("connected");

    // #region agent log
    {
        std::ofstream dbg("/Users/param/Documents/datafeed/.cursor/debug-627934.log", std::ios::app);
        dbg << "{\"sessionId\":\"627934\",\"hypothesisId\":\"C\",\"location\":\"backtest_source.cpp:start_replay\","
            << "\"message\":\"backtest replay starting\",\"data\":{\"date_range\":\"" << date_range
            << "\",\"symbols\":" << syms.size() << "},\"timestamp\":"
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch()).count()
            << "}\n";
    }
    // #endregion

    try {
        adapter_ = std::make_unique<DAdapter>(db_connection_string());
        adapter_->set_symbols(syms);
        adapter_->set_date_range(start_date, end_date);
        if (!timeframe.empty()) {
            adapter_->set_timeframe(timeframe);
        }
        adapter_->set_callback([this](const MarketData& data) {
            this->on_market_data(data);
        });

        std::thread([this]() {
            try {
                adapter_->run();
                set_feed_status("disconnected");
                nlohmann::json done;
                done["topic"]  = "backtest_complete";
                done["status"] = "complete";
                manager_->broadcast_to_topic("backtest_complete", done.dump());
                std::cout << "[BacktestSource] Replay complete." << std::endl;
            } catch (const std::exception& e) {
                set_feed_status("disconnected");
                std::cerr << "[BacktestSource] Replay error: " << e.what() << std::endl;
                nlohmann::json err;
                err["topic"] = "backtest_complete";
                err["status"] = "error";
                err["message"] = e.what();
                manager_->broadcast_to_topic("backtest_complete", err.dump());
            }
        }).detach();

    } catch (const std::exception& e) {
        set_feed_status("disconnected");
        std::cerr << "[BacktestSource] Failed to initialise DAdapter: " << e.what() << std::endl;
    }
}

void backtest_source::stop_replay() {
    if (adapter_) {
        adapter_->stop();
        adapter_.reset();
    }
}

void backtest_source::on_market_data(const MarketData& data) {
    nlohmann::json j;
    j["topic"]     = "backtest_price_";
    j["symbol"]    = data.symbol;
    j["price"]     = data.price;
    j["bid"]       = data.bid;
    j["ask"]       = data.ask;
    j["quantity"]  = data.quantity;
    j["timestamp"] = data.timestamp;

    manager_->broadcast_to_topic("backtest_price_", j.dump());
}
