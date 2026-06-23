#ifndef EADAPTER_H
#define EADAPTER_H

#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <functional>
#include "../exchange/exchange1.h"
#include "../exchange/exchange2.h"
#include "../exchange/exchange3.h"

class Exchange1;
class Exchange2;
class Exchange3;

enum ExchangeType {
    BINANCE,
    JUPITER,
    BIRDEYE
};





class EAdapter {
public:
    explicit EAdapter(ExchangeType type = ExchangeType::BINANCE);
    ~EAdapter();

    void connect_to_exchange();     
    void subscribe_symbols();       
    void on_update();         
    void run();                     
    void stop();                    
    
    void set_symbols(const std::vector<std::string>& symbols);

    void set_exchange(ExchangeType type);
    ExchangeType get_exchange() const { return exchange_type_; }


    using ExternalCallback = std::function<void(
    const std::string&,
    double,
    double,
    double,
    long
    )>;
    
    void set_callback(ExternalCallback cb);

    


    
private:
    ExchangeType exchange_type_;
    std::unique_ptr<Exchange1> exchange1_;
    std::unique_ptr<Exchange2> exchange2_;
    std::unique_ptr<Exchange3> exchange3_;

    void _connect();
    void _subscribe(const std::vector<std::string>& syms);
    void _set_callback();



    std::vector<std::string> symbols_;
    std::atomic<bool> running_{false};
    std::thread worker_thread_;
    ExternalCallback external_cb_;
};

#endif