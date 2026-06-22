#ifndef MARKET_DATA_H  // ADD THIS
#define MARKET_DATA_H


#include "string"





struct MarketData {
    std::string symbol;
    double price;
    double bid;
    double ask;
    int quantity;
    uint64_t timestamp;  
    
    
    std::string to_topic_string(const std::string& topic) const {
        if (topic == "price_") {
            return "{\"price\": " + std::to_string(price) + "}";
        }
        else if (topic == "bid_") {
            return "{\"bid\": " + std::to_string(bid) + "}";
        }
        else if (topic == "all_") {
            return "{\"price\": " + std::to_string(price) + 
                   ", \"bid\": " + std::to_string(bid) + 
                   ", \"ask\": " + std::to_string(ask) + "}";
        }
        return "";
    }
};

#endif