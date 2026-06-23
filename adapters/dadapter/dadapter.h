#ifndef DADAPTER_H
#define DADAPTER_H

#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <functional>
#include "../../market_data.h"
#include <pqxx/pqxx>

class DAdapter {
public:
    explicit DAdapter(const std::string& connection_string);
    ~DAdapter();

    void connect_to_database();
    void subscribe_symbols();
    void on_update();
    void run();
    void stop();

    void set_symbols(const std::vector<std::string>& symbols);
    void set_date_range(const std::string& start_date, const std::string& end_date);
    void set_timeframe(const std::string& timeframe); // e.g., "1m", "5m", "1h", "1d"

    using ExternalCallback = std::function<void(
    const MarketData&
    )>;

    void set_callback(ExternalCallback cb);

private:
    std::string connection_string_;
    std::vector<std::string> symbols_;
    std::string start_date_;
    std::string end_date_;
    std::string timeframe_;

    std::atomic<bool> running_{false};
    std::thread worker_thread_;
    ExternalCallback external_cb_;

    std::unique_ptr<pqxx::connection> conn_;

    void _connect();
    void _subscribe(const std::vector<std::string>& syms);
    void _set_callback();
    void query_historical_data();
};

#endif