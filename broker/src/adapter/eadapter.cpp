#include "adapter/eadapter.h"
#include "exchange/exchange1.h"
#include "exchange/exchange2.h"
#include "exchange/exchange3.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <stdexcept>


EAdapter::EAdapter(ExchangeType type) : exchange_type_(type) {
    set_exchange(type);
}

EAdapter::~EAdapter() {
    stop();
}

void EAdapter::set_exchange(ExchangeType type) {
    exchange_type_ = type;
 
    if (type == ExchangeType::BINANCE) {
        exchange2_.reset(); exchange3_.reset();
        exchange1_ = std::make_unique<Exchange1>();
        std::cout << "[EAdapter] Exchange set to BINANCE" << std::endl;
    } else if (type == ExchangeType::JUPITER) {
        exchange1_.reset(); exchange3_.reset();
        exchange2_ = std::make_unique<Exchange2>();
        std::cout << "[EAdapter] Exchange set to JUPITER" << std::endl;
    } else {
        exchange1_.reset(); exchange2_.reset();
        exchange3_ = std::make_unique<Exchange3>();
        std::cout << "[EAdapter] Exchange set to BIRDEYE" << std::endl;
    }

    if (running_) {
        // Re-initialize if already running
        on_update();
        connect_to_exchange();
        subscribe_symbols();
    }
}

void EAdapter::set_symbols(const std::vector<std::string>& symbols) {
    symbols_ = symbols;
    if (running_) {
        subscribe_symbols();
    }
}

void EAdapter::set_callback(ExternalCallback cb) {
    external_cb_ = std::move(cb);
    if (running_) {
        on_update();
    }
}

void EAdapter::connect_to_exchange() {
    if (exchange_type_ == ExchangeType::BINANCE) {
        if (exchange1_) exchange1_->connect();
    } else if (exchange_type_ == ExchangeType::JUPITER) {
        if (exchange2_) exchange2_->connect();
    } else {
        if (exchange3_) exchange3_->connect();
    }
    std::cout << "[EAdapter] Connected to exchange" << std::endl;
}

void EAdapter::subscribe_symbols() {
    if (exchange_type_ == ExchangeType::BINANCE) {
        if (exchange1_) exchange1_->subscribe(symbols_);
    } else if (exchange_type_ == ExchangeType::JUPITER) {
        if (exchange2_) exchange2_->subscribe(symbols_);
    } else {
        if (exchange3_) exchange3_->subscribe(symbols_);
    }
    std::cout << "[EAdapter] Subscribed to " << symbols_.size() << " symbols" << std::endl;
}

void EAdapter::on_update() {
    auto cb = [this](const std::string& symbol, double price,
                     double bid, double ask, long ts) {
        if (external_cb_) external_cb_(symbol, price, bid, ask, ts);
    };

    //std::cout << "Eadapter : " << cb << std::endl;
 
    if (exchange_type_ == ExchangeType::BINANCE) {
        if (exchange1_) exchange1_->set_callback(cb);
    } else if (exchange_type_ == ExchangeType::JUPITER) {
        if (exchange2_) exchange2_->set_callback(cb);
    } else {
        if (exchange3_) exchange3_->set_callback(cb);
    }
}

void EAdapter::run() {
    std::cout << "[EAdapter] Starting ("
              << (exchange_type_ == ExchangeType::BINANCE ? "BINANCE" :
                  exchange_type_ == ExchangeType::JUPITER ? "JUPITER" : "BIRDEYE")
              << ")..." << std::endl;
    
    // Setup callback for price updates
    on_update();
    
    // Connect to exchange
    connect_to_exchange();
    
    // Subscribe to symbols
    subscribe_symbols();
    
    running_ = true;
    
    // Main loop - just keep running, price updates come via callback
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

void EAdapter::stop() {
    running_ = false;
    // Signal exchange internals to stop before their destructors join threads
    if (exchange1_) { /* Exchange1 destructor handles its own cleanup */ }
    if (exchange2_) exchange2_->stop();
    if (exchange3_) exchange3_->stop();
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
    std::cout << "EAdapter stopped" << std::endl;
}

// Private helpers from header
void EAdapter::_connect() { connect_to_exchange(); }
void EAdapter::_subscribe(const std::vector<std::string>& syms) { symbols_ = syms; subscribe_symbols(); }
void EAdapter::_set_callback() { on_update(); }
