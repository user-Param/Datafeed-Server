#include "exchange/exchange2.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <string_view>
#include <nlohmann/json.hpp>
#include <cstdlib>

const std::unordered_map<std::string, std::string> Exchange2::SYMBOL_TO_MINT = {
    {"SOL",      "So11111111111111111111111111111111111111112"},
    {"SOLUSDT",  "So11111111111111111111111111111111111111112"},
    {"SOL-PERP", "So11111111111111111111111111111111111111112"},
};

Exchange2::Exchange2() {
    ctx_.set_verify_mode(ssl::verify_none);
}

Exchange2::~Exchange2() {
    running_   = false;
    connected_ = false;
    if (stream_thread_.joinable()) {
        stream_thread_.join();
    }
}

void Exchange2::connect() {
    connected_ = true;
    running_   = true;
    std::cout << "[JUPITER] HFT adapter ready (REST polling v3)" << std::endl;
}

void Exchange2::subscribe(const std::vector<std::string>& symbols) {
    if (!connected_) return;
    {
        std::lock_guard<std::mutex> lock(symbols_mutex_);
        subscribed_symbols_ = symbols;
    }
    if (!stream_thread_.joinable()) {
        stream_thread_ = std::thread(&Exchange2::stream_loop, this);
    }
}

void Exchange2::set_callback(PriceCallback callback) {
    callback_ = std::move(callback);
}

void Exchange2::stop() {
    running_   = false;
    connected_ = false;
}

void Exchange2::stream_loop() {
    const char* api_key_env = std::getenv("JUPITER_API_KEY");
    std::string api_key = api_key_env ? api_key_env : "";

    while (running_ && connected_) {
        std::vector<std::string> symbols;
        {
            std::lock_guard<std::mutex> lock(symbols_mutex_);
            symbols = subscribed_symbols_;
        }

        if (symbols.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        std::string ids;
        std::vector<std::pair<std::string, std::string>> active_mappings;
        for (const auto& sym : symbols) {
            auto it = SYMBOL_TO_MINT.find(sym);
            if (it != SYMBOL_TO_MINT.end()) {
                if (!ids.empty()) ids += ",";
                ids += it->second;
                active_mappings.push_back({it->second, sym});
            }
        }

        if (ids.empty()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
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

            http::request<http::empty_body> req{http::verb::get, "/price/v3?ids=" + ids, 11};
            req.set(http::field::host,       host_);
            req.set(http::field::user_agent,  "STATPRO/1.0");
            req.set(http::field::connection,  "close");
            if (!api_key.empty()) req.set("x-api-key", api_key);

            http::write(stream, req);

            beast::flat_buffer buffer;
            http::response<http::string_body> res;
            http::read(stream, buffer, res);

            beast::error_code ec;
            stream.shutdown(ec);

            if (res.result() == http::status::ok) {
                auto j = nlohmann::json::parse(res.body());
                for (const auto& [mint, symbol] : active_mappings) {
                    if (j.contains(mint)) {
                        double price = 0.0;
                        auto& val = j[mint];
                        if (val.contains("usdPrice")) {
                            if (val["usdPrice"].is_number()) price = val["usdPrice"].get<double>();
                            else if (val["usdPrice"].is_string()) {
                                try { price = std::stod(val["usdPrice"].get<std::string>()); } catch (...) {}
                            }
                        }
                        if (price > 0 && callback_) {
                            constexpr double FEE = 0.0006;
                            long ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                                          std::chrono::system_clock::now().time_since_epoch()).count();
                            callback_(symbol, price, price * (1.0 - FEE), price * (1.0 + FEE), ts);
                        }
                    }
                }
            } else {
                std::cerr << "[JUPITER] HTTP " << res.result_int() << " (API Key might be missing or invalid)" << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "[JUPITER] Request failed: " << e.what() << std::endl;
        }

        for (int i = 0; i < 5 && running_ && connected_; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
}

void Exchange2::fast_parse_and_callback(const std::string& body, const std::string& symbol) {}