#include "exchange/exchange4.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <cstdlib>
#include <nlohmann/json.hpp>

Exchange4::Exchange4() {
    const char* key = std::getenv("CMC_API_KEY");
    if (key) api_key_ = key;
}

Exchange4::~Exchange4() {
    stop();
}

void Exchange4::connect() {
    connected_ = true;
    running_   = true;
    std::cout << "[CMC] Adapter ready (REST polling)" << std::endl;
}

void Exchange4::subscribe(const std::vector<std::string>& symbols) {
    if (!connected_) return;
    {
        std::lock_guard<std::mutex> lock(symbols_mutex_);
        subscribed_symbols_ = symbols;
    }
    if (!stream_thread_.joinable()) {
        stream_thread_ = std::thread(&Exchange4::stream_loop, this);
    }
}

void Exchange4::set_callback(PriceCallback callback) {
    callback_ = std::move(callback);
}

void Exchange4::stop() {
    running_   = false;
    connected_ = false;
    if (stream_thread_.joinable()) stream_thread_.join();
}

void Exchange4::stream_loop() {
    while (running_ && connected_) {
        if (api_key_.empty()) {
            std::cerr << "[CMC] Error: CMC_API_KEY not found in environment." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(5));
            continue;
        }

        std::vector<std::string> symbols;
        {
            std::lock_guard<std::mutex> lock(symbols_mutex_);
            symbols = subscribed_symbols_;
        }

        if (symbols.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        // Construct comma-separated symbol string for CMC API
        std::string symbol_list;
        for (size_t i = 0; i < symbols.size(); ++i) {
            symbol_list += symbols[i];
            if (i < symbols.size() - 1) symbol_list += ",";
        }

        try {
            net::io_context ioc;
            ssl::context ctx{ssl::context::tlsv12_client};
            ctx.set_verify_mode(ssl::verify_none);

            beast::ssl_stream<beast::tcp_stream> stream(ioc, ctx);
            if (!SSL_set_tlsext_host_name(stream.native_handle(), host_.c_str())) continue;

            beast::get_lowest_layer(stream).expires_after(std::chrono::seconds(5));
            tcp::resolver resolver(ioc);
            auto const results = resolver.resolve(host_, port_);
            beast::get_lowest_layer(stream).connect(results);
            stream.handshake(ssl::stream_base::client);

            std::string target = "/v1/cryptocurrency/quotes/latest?symbol=" + symbol_list;
            http::request<http::empty_body> req{http::verb::get, target, 11};
            req.set(http::field::host, host_);
            req.set(http::field::user_agent, "STATPRO/1.0");
            req.set(http::field::connection, "close");
            req.set("X-CMC_PRO_API_KEY", api_key_);

            http::write(stream, req);

            beast::flat_buffer buf;
            http::response<http::string_body> res;
            http::read(stream, buf, res);

            beast::error_code ec;
            stream.shutdown(ec);

            if (res.result() == http::status::ok) {
                auto j = nlohmann::json::parse(res.body());
                if (j.contains("data")) {
                    auto& data = j["data"];
                    for (const auto& symbol : symbols) {
                        if (data.contains(symbol)) {
                            auto& coin_data = data[symbol];
                            if (coin_data.contains("quote") && coin_data["quote"].contains("USD")) {
                                double price = coin_data["quote"]["USD"].value("price", 0.0);
                                if (price > 0) {
                                    constexpr double FEE = 0.0006;
                                    long ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                                              std::chrono::system_clock::now().time_since_epoch()).count();
                                    if (callback_) {
                                        callback_(symbol, price, price * (1.0 - FEE), price * (1.0 + FEE), ts);
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                std::cerr << "[CMC] HTTP " << res.result_int() << " error for symbols: " << symbol_list << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "[CMC] Request failed: " << e.what() << std::endl;
        }

        // CMC API has rate limits, polling every few seconds is safer
        for (int i = 0; i < 20 && running_ && connected_; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
}
