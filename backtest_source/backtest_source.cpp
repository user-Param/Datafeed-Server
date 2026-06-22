#include "backtest_source.h"
#include "../session/session_manager.h"
#include <iostream>
#include "../server/server.h"
#include <nlohmann/json.hpp>


class websocket_session;



backtest_source::backtest_source(std::shared_ptr<session_manager> manager)
    : manager_(manager) {}

void backtest_source::start_replay(const std::string& date) {
    (void)date;
    std::cout << "Backtest started for date: " << date << std::endl;
}

void backtest_source::on_market_data(const MarketData& data) {
    nlohmann::json j;
    j["topic"] = "ticker_";
    j["symbol"] = data.symbol;
    j["price"] = data.price;
    j["bid"] = data.bid;
    j["ask"] = data.ask;
    j["timestamp"] = data.timestamp;

    manager_->broadcast_to_topic("ticker_", j.dump());
}