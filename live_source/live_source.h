#ifndef LIVE_SOURCE_H
#define LIVE_SOURCE_H

#include <memory>
#include "../session/session_manager.h"
#include "../market_data.h"
#include "../../broker/include/adapter/eadapter.h"

class live_source {
public:
    live_source(std::shared_ptr<session_manager> manager);
    void start();  
    void on_market_data(const MarketData& data);  
    std::shared_ptr<EAdapter> adapter_;
    void switch_exchange(ExchangeType type, const std::vector<std::string>& symbols);
private:
    std::shared_ptr<session_manager> manager_;
};

#endif
//