#include "live_source.h"
#include <iostream>
#include <fstream>
#include <chrono>
#include <nlohmann/json.hpp>

live_source::live_source(std::shared_ptr<session_manager> manager)
    : manager_(manager) {}

void live_source::set_db_adapter(std::shared_ptr<datafeed::SAdapter> adapter,
                                 const std::string& instance_id) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    db_ = std::move(adapter);
    instance_id_ = instance_id;
}

void live_source::register_feed_instance(const std::string& exchange) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    if (!db_ || !db_->is_connected() || instance_id_.empty()) return;

    datafeed::FeedInstance fi;
    fi.instance_id = instance_id_;
    fi.exchange = exchange;
    fi.adapter_type = "exchange";
    fi.feed_status = "connected";
    fi.reconnect_attempts = 0;
    fi.last_tick_at = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    auto existing = db_->get_feed_instance_by_id(instance_id_);
    if (existing) {
        db_->update_feed_instance(fi);
    } else {
        db_->create_feed_instance(fi);
    }

    // #region agent log
    // Disabled hardcoded debug log for deployment compatibility
    // {
    //     std::ofstream dbg("/Users/param/Documents/datafeed/.cursor/debug-627934.log", std::ios::app);
    //     dbg << "{\"sessionId\":\"627934\",\"hypothesisId\":\"B\",\"location\":\"live_source.cpp:register_feed_instance\","
    //         << "\"message\":\"feed instance registered\",\"data\":{\"instance_id\":\"" << instance_id_
    //         << "\",\"exchange\":\"" << exchange << "\"},\"timestamp\":"
    //         << std::chrono::duration_cast<std::chrono::milliseconds>(
    //                std::chrono::system_clock::now().time_since_epoch()).count()
    //         << "}\n";
    // }
    // #endregion
}

void live_source::touch_feed_instance(uint64_t tick_ts) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    if (!db_ || !db_->is_connected() || instance_id_.empty()) return;
    if (++tick_count_ % 50 != 0) return;

    datafeed::FeedInstance fi;
    fi.instance_id = instance_id_;
    fi.exchange = current_exchange_;
    fi.adapter_type = "exchange";
    fi.feed_status = "connected";
    fi.reconnect_attempts = 0;
    fi.last_tick_at = tick_ts;
    db_->update_feed_instance(fi);
}

void live_source::start() {
    std::cout << "[LiveSource] Starting..." << std::endl;

    const char* ex_env = std::getenv("EXCHANGE");
    current_exchange_ = (ex_env && *ex_env) ? ex_env : "BINANCE";

    register_feed_instance(current_exchange_);

    ExchangeType exType = ExchangeType::BINANCE;
    std::string exUpper = current_exchange_;
    for (auto& c : exUpper) c = toupper(c);
    if (exUpper == "JUPITER")      exType = ExchangeType::JUPITER;
    else if (exUpper == "BIRDEYE") exType = ExchangeType::BIRDEYE;

    std::vector<std::string> symbols;
    if (exType == ExchangeType::BINANCE)
        symbols = {"BTCUSDT", "ETHUSDT", "SOLUSDT"};
    else
        symbols = {"BTC", "ETH", "SOL"};

    std::lock_guard<std::mutex> lock(adapter_mutex_);
    adapter_ = std::make_shared<EAdapter>(exType);
    adapter_->set_symbols(symbols);

    adapter_->set_callback([this](const std::string& symbol, double price, double bid, double ask, long ts) {
        MarketData data;
        data.symbol = symbol;
        data.price = price;
        data.bid = bid;
        data.ask = ask;
        data.timestamp = static_cast<uint64_t>(ts);
        this->on_market_data(data);
    });

    std::thread([this]() {
        std::shared_ptr<EAdapter> a;
        {
            std::lock_guard<std::mutex> lock(adapter_mutex_);
            a = adapter_;
        }
        if (a) a->run();
    }).detach();
}

void live_source::stop() {
    std::lock_guard<std::mutex> lock(adapter_mutex_);
    if (adapter_) {
        adapter_->stop();
        adapter_.reset();
    }
}

void live_source::on_market_data(const MarketData& data) {
    touch_feed_instance(data.timestamp);

    nlohmann::json j;
    j["topic"] = "ticker_";
    j["symbol"] = data.symbol;
    j["price"] = data.price;
    j["bid"] = data.bid;
    j["ask"] = data.ask;
    j["timestamp"] = data.timestamp;

    manager_->broadcast_to_topic("ticker_", j.dump());
}

void live_source::switch_exchange(ExchangeType type, const std::vector<std::string>& symbols) {
    current_exchange_ = (type == ExchangeType::BINANCE ? "BINANCE" :
                         type == ExchangeType::JUPITER ? "JUPITER" : "BIRDEYE");
    register_feed_instance(current_exchange_);

    std::cout << "[LiveSource] Switching exchange to " << current_exchange_ << std::endl;

    std::thread([this, type, symbols]() {
        try {
            std::shared_ptr<EAdapter> old_adapter;
            {
                std::lock_guard<std::mutex> lock(adapter_mutex_);
                old_adapter = adapter_;
                adapter_.reset();
            }
            if (old_adapter) {
                old_adapter->stop();
            }

            auto new_adapter = std::make_shared<EAdapter>(type);
            new_adapter->set_symbols(symbols);
            new_adapter->set_callback([this](const std::string& symbol, double price,
                                            double bid, double ask, long ts) {
                MarketData data;
                data.symbol    = symbol;
                data.price     = price;
                data.bid       = bid;
                data.ask       = ask;
                data.timestamp = static_cast<uint64_t>(ts);
                this->on_market_data(data);
            });

            {
                std::lock_guard<std::mutex> lock(adapter_mutex_);
                adapter_ = new_adapter;
            }
            new_adapter->run();
        } catch (const std::exception& e) {
            std::cerr << "[LiveSource] Switch error: " << e.what() << std::endl;
        }
    }).detach();
}
