#include "live_source.h"
#include <iostream>
#include "../session/session_manager.h"
#include <nlohmann/json.hpp>
#include "../server/server.h"

std::shared_ptr<EAdapter> adapter_;

live_source::live_source(std::shared_ptr<session_manager> manager)
    : manager_(manager) {}

void live_source::start() {
    std::cout << "[LiveSource] Starting..." << std::endl;

    // Default to JUPITER as requested
    adapter_ = std::make_shared<EAdapter>(ExchangeType::JUPITER);

    // Initial symbols for Jupiter/Solana
    adapter_->set_symbols({"SOL", "BTC", "ETH"});

    adapter_->set_callback([this](const std::string& symbol, double price, double bid, double ask, long ts) {
        MarketData data;
        data.symbol = symbol;
        data.price = price;
        data.bid = bid;
        data.ask = ask;
        data.timestamp = ts;
        this->on_market_data(data);
    });

    std::thread([this]() {
        adapter_->run();
    }).detach();
}

void live_source::on_market_data(const MarketData& data) {
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
    std::cout << "[LiveSource] Switching exchange to " 
              << (type == ExchangeType::BINANCE ? "BINANCE" :
                  type == ExchangeType::JUPITER ? "JUPITER" : "BIRDEYE") << std::endl;

    std::thread([this, type, symbols]() {
        try {
            if (adapter_) {
                adapter_->stop();
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
                data.timestamp = ts;
                this->on_market_data(data);
            });

            adapter_ = new_adapter;
            adapter_->run();
        } catch (const std::exception& e) {
            std::cerr << "[LiveSource] Switch error: " << e.what() << std::endl;
        }
    }).detach();
}