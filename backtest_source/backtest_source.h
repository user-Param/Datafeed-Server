#ifndef BACKTEST_SOURCE_H
#define BACKTEST_SOURCE_H

#include <memory>
#include <string>
#include <vector>
#include <mutex>
#include "../market_data.h"
#include "../session/session_manager.h"
#include "../adapters/dadapter/dadapter.h"
#include "../sadapter.h"

class backtest_source {
public:
    explicit backtest_source(std::shared_ptr<session_manager> manager);

    void set_db_adapter(std::shared_ptr<datafeed::SAdapter> adapter, const std::string& instance_id);

    void start_replay(const std::string& date_range,
                      const std::vector<std::string>& symbols = {},
                      const std::string& timeframe = "");

    void stop_replay();
    void on_market_data(const MarketData& data);

private:
    void set_feed_status(const std::string& status);
    static std::string db_connection_string();

    std::shared_ptr<session_manager> manager_;
    std::shared_ptr<datafeed::SAdapter> db_;
    std::unique_ptr<DAdapter> adapter_;
    std::string instance_id_;
    std::mutex db_mutex_;
};

#endif
