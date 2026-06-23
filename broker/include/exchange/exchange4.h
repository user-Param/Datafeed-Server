#ifndef EXCHANGE4_H
#define EXCHANGE4_H

#include <string>
#include <vector>
#include <functional>
#include <memory>
#include <thread>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>

using PriceCallback = std::function<void(const std::string& symbol, double price, double bid, double ask, long timestamp)>;

namespace beast = boost::beast;
namespace http  = beast::http;
namespace net   = boost::asio;
namespace ssl   = net::ssl;
using tcp       = net::ip::tcp;

class Exchange4 {
public:
    Exchange4();
    ~Exchange4();

    void connect();
    void subscribe(const std::vector<std::string>& symbols);
    void set_callback(PriceCallback callback);
    void stop();

private:
    void stream_loop();

    const std::string host_ = "pro-api.coinmarketcap.com";
    const std::string port_ = "443";
    std::string api_key_;

    PriceCallback callback_;

    std::atomic<bool> connected_{false};
    std::atomic<bool> running_{false};

    std::vector<std::string> subscribed_symbols_;
    std::mutex               symbols_mutex_;

    std::thread stream_thread_;
};

#endif // EXCHANGE4_H
