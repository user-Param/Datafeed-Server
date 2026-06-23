#include "exchange/exchange1.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <cctype>


Exchange1::Exchange1() : connected_(false) {
    ctx_.set_verify_mode(ssl::verify_none);
}

Exchange1::~Exchange1() {
     running_ = false;
    if (ws_.is_open()) {
        beast::error_code ec;
        ws_.close(websocket::close_code::normal, ec);
    }
    if (io_thread_.joinable()) io_thread_.join();
    if (reader_thread_.joinable()) reader_thread_.join();
}

void Exchange1::connect() {
     if (connected_) return;
    
    running_ = true;
    io_thread_ = std::thread(&Exchange1::run_io_context, this);
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    try {
        tcp::resolver resolver(ioc_);
        auto const results = resolver.resolve("stream.binance.com", "9443");
        
        net::connect(ws_.next_layer().next_layer(), results.begin(), results.end());
        ws_.next_layer().handshake(ssl::stream_base::client);
        
        ws_.handshake("stream.binance.com", "/stream");
        
        connected_ = true;
        std::cout << "[Exchange1] Connected to Binance WebSocket" << std::endl;
        reader_thread_ = std::thread(&Exchange1::read_loop, this);
    }
    catch (std::exception const& e) {
        std::cerr << "[Exchange1] Connection error: " << e.what() << std::endl;
        connected_ = false;
        running_ = false;
    }
}

void Exchange1::subscribe(const std::vector<std::string>& symbols) {
    if (!connected_) {
        std::cerr << "[Exchange1] Not connected. Call connect() first." << std::endl;
        return;
    }
    send_subscribe(symbols);
}

void Exchange1::set_callback(PriceCallback callback) {
    callback_ = std::move(callback);
}

void Exchange1::run_io_context() {
    while (running_) {
        try {
            ioc_.run();
            break;
        }
        catch (std::exception const& e) {
            std::cerr << "[Exchange1] IO context error: " << e.what() << std::endl;
            if (running_) {
                // For simplicity, stop on error; a real implementation would attempt reconnect
                running_ = false;
            }
        }
    }
}

void Exchange1::read_loop() {
    beast::flat_buffer buffer;
    while (running_ && connected_) {
        try {
            buffer.consume(buffer.size());
            ws_.read(buffer);
            std::string msg = beast::buffers_to_string(buffer.data());
            
            auto j = nlohmann::json::parse(msg);
            // Binance combined stream format: {"stream":"btcusdt@ticker","data":{...}}
            if (j.contains("data")) {
                auto& data = j["data"];
                std::string symbol = data["s"].get<std::string>();
                double price = std::stod(data["c"].get<std::string>());
                double bid   = std::stod(data["b"].get<std::string>());
                double ask   = std::stod(data["a"].get<std::string>());
                long timestamp = data["E"].get<long>();
                
                if (callback_) {
                    callback_(symbol, price, bid, ask, timestamp);
                }
            }
        }
        catch (std::exception const& e) {
            std::cerr << "[Exchange1] Read error: " << e.what() << std::endl;
            if (running_) {
                break;  // You could attempt reconnection here
            }
        }
    }
    std::cout << "[Exchange1] Read loop ended" << std::endl;
}

void Exchange1::send_subscribe(const std::vector<std::string>& symbols) {
    nlohmann::json sub;
    sub["method"] = "SUBSCRIBE";
    sub["id"] = 1;
    std::vector<std::string> params;
    for (const auto& sym : symbols) {
        std::string lower = sym;
        for (char& c : lower) c = std::tolower(static_cast<unsigned char>(c));
        params.push_back(lower + "@ticker");
    }
    sub["params"] = params;
    
    std::string msg = sub.dump();
    ws_.write(net::buffer(msg));
    std::cout << "[Exchange1] Subscribed to tickers for: ";
    for (const auto& s : symbols) std::cout << s << " ";
    std::cout << std::endl;
}