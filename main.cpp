#include "adapter/eadapter.h"
#include <iostream>
#include <thread>
#include <csignal>
#include <cstdlib>
#include <string>
#include <atomic>

std::atomic<bool> adapterRunning{true};
std::unique_ptr<EAdapter> adapter;

void signal_handler(int) {
adapterRunning = false;
    if (adapter) adapter->stop();
}

static ExchangeType parse_exchange(const std::string& name) {
    if (name == "JUPITER" || name == "jupiter" || name == "2")
        return ExchangeType::JUPITER;
    return ExchangeType::BINANCE;   // default
}

int main(int argc, char* argv[]) {
    
    signal(SIGINT, signal_handler);

    std::string exchange_name = "BINANCE";

    if (argc >= 2) exchange_name = argv[1];
    else if (const char* env = std::getenv("EXCHANGE")) exchange_name = env;


    ExchangeType type = parse_exchange(exchange_name);
    adapter = std::make_unique<EAdapter>(type);

    if (type == ExchangeType::BINANCE)
        adapter->set_symbols({"BTCUSDT", "ETHUSDT", "SOLUSDT"});
    else
        adapter->set_symbols({"BTC", "ETH", "SOL"});




    std::thread adapterThread([&]() {
        adapter->run();
    });

    std::cout << "[Broker] Running. Type 'BINANCE' or 'JUPITER' to switch.\n";



    std::string line;
 
    while (adapterRunning && std::getline(std::cin, line)) {
        if (line == "BINANCE" || line == "JUPITER") {
            ExchangeType newType = parse_exchange(line);
            if (newType != adapter->get_exchange()) {
                adapter->stop();                 
                adapterThread.join();            

                adapter = std::make_unique<EAdapter>(newType);
                if (newType == ExchangeType::BINANCE)
                    adapter->set_symbols({"BTCUSDT", "ETHUSDT", "SOLUSDT"});
                else
                    adapter->set_symbols({"BTC", "ETH", "SOL"});

                adapterThread = std::thread([&]() {
                    adapter->run();
                });
                std::cout << "[Broker] Switched to " << line << "\n";
            }
        } else if (line == "quit") {
            break;
        }
    }

    adapterRunning = false;
    adapter->stop();
    if (adapterThread.joinable()) adapterThread.join();
    return 0;
}