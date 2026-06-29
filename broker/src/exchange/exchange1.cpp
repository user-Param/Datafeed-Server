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
     connected_ = false;
    if (ws_.is_open()) {
        beast::error_code ec;
        ws_.close(websocket::close_code::normal, ec);
    }
    if (io_thread_.joinable()) io_thread_.join();
    if (reader_thread_.joinable()) reader_thread_.join();
}

void Exchange1::perform_connect() {
    try {
        std::cout << "[Exchange1] Resolving DNS: stream.binance.com:9443" << std::endl;
        tcp::resolver resolver(ioc_);
        auto const results = resolver.resolve("stream.binance.com", "9443");
        std::cout << "[Exchange1] DNS resolved" << std::endl;

        std::cout << "[Exchange1] TCP connecting..." << std::endl;
        net::connect(ws_.next_layer().next_layer(), results.begin(), results.end());
        std::cout << "[Exchange1] TCP connected" << std::endl;

        std::cout << "[Exchange1] TLS handshake..." << std::endl;
        ws_.next_layer().handshake(ssl::stream_base::client);
        std::cout << "[Exchange1] TLS handshake complete" << std::endl;

        std::cout << "[Exchange1] WebSocket handshake: /stream" << std::endl;
        ws_.handshake("stream.binance.com", "/stream");
        std::cout << "[Exchange1] WebSocket handshake complete" << std::endl;

        connected_ = true;
        std::cout << "[Exchange1] Connected to Binance WebSocket" << std::endl;
    }
    catch (std::exception const& e) {
        std::cerr << "[Exchange1] Connection error: " << e.what() << std::endl;
        connected_ = false;
        throw;
    }
}

void Exchange1::start_reader() {
    if (reader_thread_.joinable()) reader_thread_.join();
    reader_thread_ = std::thread(&Exchange1::read_loop, this);
}

void Exchange1::connect() {
     if (connected_) return;
    
    running_ = true;
    io_thread_ = std::thread(&Exchange1::run_io_context, this);
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    perform_connect();
    start_reader();
}

void Exchange1::subscribe(const std::vector<std::string>& symbols) {
    symbols_ = symbols;
    if (!connected_) {
        std::cerr << "[Exchange1] Not connected. Call connect() first." << std::endl;
        return;
    }
    std::cout << "[Exchange1] Subscribing to " << symbols.size() << " symbols" << std::endl;
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
                running_ = false;
            }
        }
    }
}

void Exchange1::read_loop() {
    beast::flat_buffer buffer;
    int reconnect_delay = 1;
    
    while (running_) {
        if (!connected_) {
            std::cout << "[Exchange1] Attempting reconnection in " << reconnect_delay << "s..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(reconnect_delay));
            reconnect_delay = std::min(reconnect_delay * 2, 30);
            
            // Close existing socket if open
            if (ws_.is_open()) {
                beast::error_code ec;
                ws_.close(websocket::close_code::normal, ec);
            }
            
            // Recreate stream after close
            ws_.~stream();
            new (&ws_) websocket::stream<ssl::stream<tcp::socket>>(ioc_, ctx_);
            
            try {
                perform_connect();
                reconnect_delay = 1;
                if (!symbols_.empty()) send_subscribe(symbols_);
                std::cout << "[Exchange1] Reconnection successful" << std::endl;
            } catch (...) {
                continue;
            }
        }
        
        try {
            buffer.consume(buffer.size());
            ws_.read(buffer);
            std::string msg = beast::buffers_to_string(buffer.data());
            
            auto j = nlohmann::json::parse(msg);
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
            connected_ = false;
            if (running_) continue;
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
