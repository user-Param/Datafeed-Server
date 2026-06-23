#include "exchange/exchange3.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <cstdlib>
#include <unordered_map>

// Map standard symbols to Solana mint addresses for Birdeye
static const std::unordered_map<std::string, std::string> BIRDEYE_SYMBOL_TO_MINT = {
    {"SOL",      "So11111111111111111111111111111111111111112"},
    {"BTC",      "3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh"},
    {"ETH",      "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs"},
    {"USDC",     "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"},
    {"USDT",     "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"},
};

Exchange3::Exchange3() {
    const char* key = std::getenv("BIRDEYE_API_KEY");
    if (key) api_key_ = key;
}

Exchange3::~Exchange3() {
    running_   = false;
    connected_ = false;
    if (stream_thread_.joinable()) stream_thread_.join();
}

void Exchange3::connect() {
    connected_ = true;
    running_   = true;
    std::cout << "[BIRDEYE] Adapter ready (REST polling)" << std::endl;
}

void Exchange3::subscribe(const std::vector<std::string>& symbols) {
    if (!connected_) return;
    {
        std::lock_guard<std::mutex> lock(symbols_mutex_);
        subscribed_symbols_ = symbols;
    }
    if (!stream_thread_.joinable()) {
        stream_thread_ = std::thread(&Exchange3::stream_loop, this);
    }
}

void Exchange3::set_callback(PriceCallback callback) {
    callback_ = std::move(callback);
}

void Exchange3::stop() {
    running_   = false;
    connected_ = false;
}

void Exchange3::stream_loop() {
    while (running_ && connected_) {
        if (api_key_.empty()) {
            std::cerr << "[BIRDEYE] Error: BIRDEYE_API_KEY not found in environment." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(5));
            continue;
        }

        std::vector<std::string> symbols;
        {
            std::lock_guard<std::mutex> lock(symbols_mutex_);
            symbols = subscribed_symbols_;
        }

        for (const auto& symbol : symbols) {
            if (!running_ || !connected_) break;
            
            std::string address = symbol;
            auto it = BIRDEYE_SYMBOL_TO_MINT.find(symbol);
            if (it != BIRDEYE_SYMBOL_TO_MINT.end()) address = it->second;

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

                http::request<http::empty_body> req{http::verb::get, "/defi/price?address=" + address, 11};
                req.set(http::field::host,       host_);
                req.set(http::field::user_agent,  "STATPRO/1.0");
                req.set(http::field::connection,  "close");
                req.set("X-API-KEY", api_key_);
                req.set("x-chain", "solana");

                http::write(stream, req);

                beast::flat_buffer buf;
                http::response<http::string_body> res;
                http::read(stream, buf, res);

                beast::error_code ec;
                stream.shutdown(ec);

                if (res.result() == http::status::ok) {
                    auto j = nlohmann::json::parse(res.body());
                    if (j.value("success", false) && j.contains("data") && j["data"].contains("value")) {
                        double price = j["data"]["value"].get<double>();
                        constexpr double FEE = 0.0006;
                        long ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                                      std::chrono::system_clock::now().time_since_epoch()).count();
                        if (callback_) callback_(symbol, price, price * (1.0 - FEE), price * (1.0 + FEE), ts);
                    }
                } else {
                    std::cerr << "[BIRDEYE] HTTP " << res.result_int() << " for " << symbol << std::endl;
                }
            } catch (...) {}
        }

        for (int i = 0; i < 10 && running_ && connected_; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
}