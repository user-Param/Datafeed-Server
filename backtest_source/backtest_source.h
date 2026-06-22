#ifndef BACKTEST_SOURCE_H
#define BACKTEST_SOURCE_H

#include <memory>
#include <string>
#include "../market_data.h"
#include "../session/session_manager.h"

class backtest_source {
public:
    backtest_source(std::shared_ptr<session_manager> manager);
    void start_replay(const std::string& date);  // Start replaying historical data
    void on_market_data(const MarketData& data);  // Called when data is read from DB

private:
    std::shared_ptr<session_manager> manager_;
};

#endif