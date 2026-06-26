#ifndef LIVE_SOURCE_H
#define LIVE_SOURCE_H

#include <memory>
#include <string>
#include "../session/session_manager.h"
#include "../market_data.h"
#include "../broker/include/adapter/eadapter.h"
#include "../sadapter.h"
#include <mutex>

class MetricsCollector;

class live_source {
public:
    live_source(std::shared_ptr<session_manager> manager);
    void set_db_adapter(std::shared_ptr<datafeed::SAdapter> adapter, const std::string& instance_id);
    void set_collector(MetricsCollector* collector);
    void start();
    void stop();
    void on_market_data(const MarketData& data);
    void switch_exchange(ExchangeType type, const std::vector<std::string>& symbols);
    std::shared_ptr<EAdapter> adapter_;
    std::mutex adapter_mutex_;

private:
    void register_feed_instance(const std::string& exchange);
    void touch_feed_instance(uint64_t tick_ts);

    std::shared_ptr<session_manager> manager_;
    std::shared_ptr<datafeed::SAdapter> db_;
    MetricsCollector* collector_{nullptr};
    std::string instance_id_;
    std::string current_exchange_{"BINANCE"};
    std::mutex db_mutex_;
    uint64_t tick_count_{0};
};

#endif
