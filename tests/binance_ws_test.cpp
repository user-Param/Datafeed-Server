// Standalone Binance WebSocket Connectivity Test
// Compile: g++ -std=c++17 binance_ws_test.cpp -o binance_ws_test \
//   -I/opt/homebrew/include -L/opt/homebrew/lib \
//   -lboost_core -lboost_system -lssl -lcrypto -lpthread
//
// Usage: ./binance_ws_test

#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/beast/version.hpp>
#include <nlohmann/json.hpp>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <atomic>
#include <cctype>
#include <openssl/ssl.h>

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

static std::atomic<bool> running{true};

int main() {
    try {
        std::string host = "stream.binance.com";
        std::string port = "9443";
        std::string target = "/ws";

        std::cout << "=== Binance WebSocket Connectivity Test ===" << std::endl;
        std::cout << "Target: wss://" << host << ":" << port << target << std::endl;
        std::cout << std::endl;

        // -------------------------------------------------------
        // Setup I/O
        // -------------------------------------------------------
        net::io_context ioc;
        ssl::context ctx{ssl::context::tlsv12_client};
        ctx.set_verify_mode(ssl::verify_none);

        tcp::resolver resolver(ioc);
        websocket::stream<ssl::stream<tcp::socket>> ws{ioc, ctx};

        // -------------------------------------------------------
        // 1. DNS Resolution
        // -------------------------------------------------------
        std::cout << "[1] Resolving DNS..." << std::endl;
        auto const results = resolver.resolve(host, port);
        std::cout << "    resolved " << results.size() << " endpoint(s):" << std::endl;
        for (auto it = results.begin(); it != results.end(); ++it) {
            auto ep = it->endpoint();
            std::cout << "      " << (ep.address().is_v4() ? "IPv4" : "IPv6")
                      << " " << ep.address().to_string() << ":" << ep.port() << std::endl;
        }
        std::cout << std::endl;

        // -------------------------------------------------------
        // 2. TCP Connect
        // -------------------------------------------------------
        std::cout << "[2] TCP connecting..." << std::endl;
        auto tcp_start = std::chrono::steady_clock::now();
        tcp::endpoint remote_ep = net::connect(ws.next_layer().next_layer(), results);
        auto tcp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - tcp_start).count();
        auto local_ep = ws.next_layer().next_layer().local_endpoint();
        std::cout << "    connected" << std::endl;
        std::cout << "    local:  " << local_ep.address().to_string() << ":" << local_ep.port() << std::endl;
        std::cout << "    remote: " << remote_ep.address().to_string() << ":" << remote_ep.port() << std::endl;
        std::cout << "    latency: " << tcp_ms << "ms" << std::endl;
        std::cout << std::endl;

        // -------------------------------------------------------
        // 3. SNI
        // -------------------------------------------------------
        std::cout << "[3] Setting SNI..." << std::endl;
        SSL* ssl_native = ws.next_layer().native_handle();
        if (ssl_native) {
            if (!SSL_set_tlsext_host_name(ssl_native, host.c_str())) {
                unsigned long err = ERR_get_error();
                char errbuf[256];
                ERR_error_string_n(err, errbuf, sizeof(errbuf));
                std::cerr << "    SNI FAILED: " << errbuf << std::endl;
                return 1;
            }
            std::cout << "    SNI hostname: " << host << std::endl;
        } else {
            std::cerr << "    ERROR: native_handle is null" << std::endl;
            return 1;
        }
        std::cout << std::endl;

        // -------------------------------------------------------
        // 4. TLS Handshake
        // -------------------------------------------------------
        std::cout << "[4] TLS handshake..." << std::endl;
        ws.next_layer().handshake(ssl::stream_base::client);

        const char* tls_ver = SSL_get_version(ssl_native);
        const char* cipher = SSL_get_cipher_name(ssl_native);
        int cipher_bits = SSL_get_cipher_bits(ssl_native, nullptr);
        const char* sni_check = SSL_get_servername(ssl_native, TLSEXT_NAMETYPE_host_name);
        X509* cert = SSL_get_peer_certificate(ssl_native);

        std::cout << "    version: " << (tls_ver ? tls_ver : "?") << std::endl;
        std::cout << "    cipher:  " << (cipher ? cipher : "?") << " (" << cipher_bits << " bits)" << std::endl;
        std::cout << "    SNI:     " << (sni_check ? sni_check : "NONE") << std::endl;
        if (cert) {
            char subject[256], issuer[256];
            X509_NAME_oneline(X509_get_subject_name(cert), subject, sizeof(subject));
            X509_NAME_oneline(X509_get_issuer_name(cert), issuer, sizeof(issuer));
            std::cout << "    cert:    " << subject << std::endl;
            std::cout << "    issuer:  " << issuer << std::endl;
            X509_free(cert);
        }
        std::cout << std::endl;

        // -------------------------------------------------------
        // 5. WebSocket Options
        // -------------------------------------------------------
        std::cout << "[5] Configuring WebSocket options..." << std::endl;

        ws.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));

        ws.set_option(websocket::stream_base::decorator(
            [](websocket::request_type& req) {
                req.set(beast::http::field::user_agent,
                    std::string(BOOST_BEAST_VERSION_STRING) + " BinanceTest/1.0");
            }
        ));

        ws.binary(false);
        std::cout << "    timeout: suggested(role=client)" << std::endl;
        std::cout << "    User-Agent: " << BOOST_BEAST_VERSION_STRING << " BinanceTest/1.0" << std::endl;
        std::cout << "    binary: false" << std::endl;
        std::cout << std::endl;

        // -------------------------------------------------------
        // 6. WebSocket Handshake
        // -------------------------------------------------------
        std::cout << "[6] WebSocket handshake..." << std::endl;
        std::cout << "    Host:   " << host << ":" << port << std::endl;
        std::cout << "    Target: " << target << std::endl;

        ws.handshake(host + ":" + port, target);

        std::cout << "    SUCCESS: HTTP 101 Switching Protocols" << std::endl;
        std::cout << std::endl;

        // -------------------------------------------------------
        // 7. Subscribe
        // -------------------------------------------------------
        std::cout << "[7] Subscribing..." << std::endl;
        {
            nlohmann::json sub;
            sub["method"] = "SUBSCRIBE";
            sub["id"] = 1;
            std::vector<std::string> params = {
                "btcusdt@ticker", "ethusdt@ticker", "solusdt@ticker"
            };
            sub["params"] = params;
            std::string msg = sub.dump();
            std::cout << "    sending: " << msg << std::endl;
            ws.write(net::buffer(msg));
        }

        // Read subscription ack
        {
            beast::flat_buffer buf;
            ws.read(buf);
            std::string ack = beast::buffers_to_string(buf.data());
            std::cout << "    response: " << ack << std::endl;
            auto j = nlohmann::json::parse(ack);
            if (j.contains("result") && j["result"].is_null() && j.contains("id")) {
                std::cout << "    SUBSCRIPTION ACKNOWLEDGED (id=" << j["id"] << ")" << std::endl;
            } else if (j.contains("error")) {
                std::cerr << "    SUBSCRIPTION ERROR: " << j["error"] << std::endl;
                return 1;
            } else {
                std::cout << "    (unexpected response format)" << std::endl;
            }
        }
        std::cout << std::endl;

        // -------------------------------------------------------
        // 8. Read loop
        // -------------------------------------------------------
        std::cout << "[8] Reading market data (10 seconds)..." << std::endl;
        std::cout << std::endl;

        beast::flat_buffer buffer;
        int tick_count = 0;
        int other_count = 0;
        auto start = std::chrono::steady_clock::now();
        auto deadline = start + std::chrono::seconds(10);

        while (std::chrono::steady_clock::now() < deadline) {
            try {
                buffer.consume(buffer.size());
                ws.read(buffer);
                std::string msg = beast::buffers_to_string(buffer.data());

                auto j = nlohmann::json::parse(msg);

                // Subscription ack or result
                if (j.contains("result") || j.contains("id")) {
                    std::cout << "    [CTRL] " << msg << std::endl;
                    continue;
                }

                // Error
                if (j.contains("code") || j.contains("error")) {
                    std::cerr << "    [ERROR] " << msg << std::endl;
                    continue;
                }

                // Ticker data
                if (j.contains("e") && j["e"] == "24hrTicker" &&
                    j.contains("s") && j.contains("c") &&
                    j.contains("b") && j.contains("a"))
                {
                    tick_count++;
                    std::string sym = j["s"].get<std::string>();
                    std::string price = j["c"].get<std::string>();
                    std::string bid = j["b"].get<std::string>();
                    std::string ask = j["a"].get<std::string>();

                    if (tick_count <= 5) {
                        std::cout << "    [TICK] " << sym << "  price=" << price
                                  << "  bid=" << bid << "  ask=" << ask << std::endl;
                    }
                } else {
                    other_count++;
                    if (other_count <= 3) {
                        std::cout << "    [OTHER] " << msg.substr(0, 200) << std::endl;
                    }
                }
            }
            catch (std::exception const& e) {
                std::cerr << "    [READ ERROR] " << e.what() << std::endl;
                break;
            }
        }

        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - start).count();

        std::cout << std::endl;
        std::cout << "=== RESULTS ===" << std::endl;
        std::cout << "  Duration:    " << elapsed << "s" << std::endl;
        std::cout << "  Ticker msgs: " << tick_count << std::endl;
        std::cout << "  Other msgs:  " << other_count << std::endl;
        std::cout << "  Rate:        " << (tick_count / double(elapsed)) << " ticks/sec" << std::endl;

        // -------------------------------------------------------
        // 9. Clean close
        // -------------------------------------------------------
        std::cout << std::endl;
        std::cout << "[9] Closing..." << std::endl;
        beast::error_code ec;
        ws.close(websocket::close_code::normal, ec);
        if (ec) {
            std::cout << "    close error: " << ec.message() << std::endl;
        } else {
            std::cout << "    closed gracefully" << std::endl;
        }

        if (tick_count > 0) {
            std::cout << std::endl;
            std::cout << "*** CONNECTIVITY TEST PASSED ***" << std::endl;
            return 0;
        } else {
            std::cerr << std::endl;
            std::cerr << "*** CONNECTIVITY TEST FAILED: No ticker data received ***" << std::endl;
            return 1;
        }
    }
    catch (beast::system_error const& e) {
        std::cerr << std::endl;
        std::cerr << "*** BEAST ERROR ***" << std::endl;
        std::cerr << "  what:  " << e.what() << std::endl;
        std::cerr << "  code:  " << e.code() << " (" << e.code().message() << ")" << std::endl;
        return 1;
    }
    catch (std::exception const& e) {
        std::cerr << std::endl;
        std::cerr << "*** ERROR ***" << std::endl;
        std::cerr << "  " << e.what() << std::endl;
        return 1;
    }
}
