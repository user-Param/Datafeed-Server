#ifndef EXCHANGE2_H
#define EXCHANGE2_H

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

// Same PriceCallback signature as Exchange1 — EAdapter works with both
using PriceCallback = std::function<void(const std::string& symbol, double price, double bid, double ask, long timestamp)>;

namespace beast = boost::beast;
namespace http  = beast::http;
namespace net   = boost::asio;
namespace ssl   = net::ssl;
using tcp       = net::ip::tcp;

class Exchange2 {
public:
    Exchange2();
    ~Exchange2();

    // ── Same public surface as Exchange1 ──────────────────────────────────
    void connect();
    void subscribe(const std::vector<std::string>& symbols);
    void set_callback(PriceCallback callback);
    void stop();

private:
    // Internal polling loop  (replaces Exchange1's read_loop + io_thread)
    void stream_loop();
    void fast_parse_and_callback(const std::string& body, const std::string& symbol);

    // Jupiter REST endpoint
    const std::string host_ = "api.jup.ag";
    const std::string port_ = "443";

    // SOL symbol  →  on-chain mint address
    static const std::unordered_map<std::string, std::string> SYMBOL_TO_MINT;

    PriceCallback callback_;

    std::atomic<bool> connected_{false};
    std::atomic<bool> running_{false};

    net::io_context               ioc_;
    ssl::context                  ctx_{ssl::context::tlsv12_client};

    // Persistent HTTPS stream (keep-alive, mirrors Exchange1's persistent WS)
    std::unique_ptr<beast::ssl_stream<beast::tcp_stream>> stream_;

    std::vector<std::string> subscribed_symbols_;
    std::mutex               symbols_mutex_;

    std::thread stream_thread_;
};

#endif // EXCHANGE2_H