#include "exchange/exchange1.h"

#include <iostream>
#include <chrono>
#include <thread>
#include <cctype>
#include <iomanip>
#include <sstream>
#include <openssl/ssl.h>
#include <boost/beast/ssl.hpp>
#include <boost/beast/version.hpp>

Exchange1::Exchange1()
    : connected_(false)
{
    ctx_.set_verify_mode(ssl::verify_none);
}

Exchange1::~Exchange1() {
     running_ = false;
     connected_ = false;
    {
        std::lock_guard<std::mutex> lock(ws_mutex_);
        if (ws_.is_open()) {
            beast::error_code ec;
            ws_.close(websocket::close_code::normal, ec);
        }
    }
    if (io_thread_.joinable()) io_thread_.join();
    if (reader_thread_.joinable()) reader_thread_.join();
}

void Exchange1::perform_connect() {
    beast::http::response<beast::http::string_body> handshake_res;
    try {
        std::cout << "[Exchange1] Resolving DNS: " << sni_hostname_ << ":" << ws_port_ << std::endl;
        tcp::resolver resolver(ioc_);
        auto const results = resolver.resolve(sni_hostname_, ws_port_);

        std::cout << "[Exchange1] DNS resolved " << results.size() << " endpoint(s):" << std::endl;
        for (auto it = results.begin(); it != results.end(); ++it) {
            auto ep = it->endpoint();
            std::cout << "  " << (ep.address().is_v4() ? "IPv4" : "IPv6") << " "
                      << ep.address().to_string() << ":" << ep.port() << std::endl;
        }

        std::cout << "[Exchange1] TCP connecting..." << std::endl;
        auto connect_start = std::chrono::steady_clock::now();

        tcp::endpoint remote_ep = net::connect(ws_.next_layer().next_layer(), results);

        auto connect_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - connect_start).count();

        tcp::endpoint local_ep = ws_.next_layer().next_layer().local_endpoint();
        std::cout << "[Exchange1] TCP connected" << std::endl;
        std::cout << "[Exchange1]   local:  " << local_ep.address().to_string() << ":" << local_ep.port() << std::endl;
        std::cout << "[Exchange1]   remote: " << remote_ep.address().to_string() << ":" << remote_ep.port() << std::endl;
        std::cout << "[Exchange1]   latency: " << connect_duration << "ms" << std::endl;

        // -------------------------------------------------------
        // 3. Set SNI before TLS handshake (required for Cloudflare)
        // -------------------------------------------------------
        std::cout << "[Exchange1] Setting SNI hostname: " << sni_hostname_ << std::endl;
        SSL* ssl_native = ws_.next_layer().native_handle();
        if (ssl_native) {
            if (!SSL_set_tlsext_host_name(ssl_native, sni_hostname_.c_str())) {
                unsigned long err = ERR_get_error();
                char errbuf[256];
                ERR_error_string_n(err, errbuf, sizeof(errbuf));
                std::cerr << "[Exchange1] SNI setting failed: " << errbuf << std::endl;
            } else {
                std::cout << "[Exchange1] SNI set successfully" << std::endl;
            }
        } else {
            std::cerr << "[Exchange1] native SSL handle is null, cannot set SNI" << std::endl;
        }

        // -------------------------------------------------------
        // 4. TLS Handshake
        // -------------------------------------------------------
        std::cout << "[Exchange1] TLS handshake..." << std::endl;
        ws_.next_layer().handshake(ssl::stream_base::client);

        if (ssl_native) {
            const char* tls_version = SSL_get_version(ssl_native);
            const char* cipher = SSL_get_cipher_name(ssl_native);
            const char* sni_used = SSL_get_servername(ssl_native, TLSEXT_NAMETYPE_host_name);
            int cipher_bits = SSL_get_cipher_bits(ssl_native, nullptr);
            X509* cert = SSL_get_peer_certificate(ssl_native);

            std::cout << "[Exchange1] TLS handshake complete" << std::endl;
            std::cout << "[Exchange1]   version: " << (tls_version ? tls_version : "?") << std::endl;
            std::cout << "[Exchange1]   cipher:  " << (cipher ? cipher : "?")
                      << " (" << cipher_bits << " bits)" << std::endl;
            std::cout << "[Exchange1]   SNI:     " << (sni_used ? sni_used : "NONE") << std::endl;
            if (cert) {
                char subject[256], issuer[256];
                X509_NAME_oneline(X509_get_subject_name(cert), subject, sizeof(subject));
                X509_NAME_oneline(X509_get_issuer_name(cert), issuer, sizeof(issuer));
                std::cout << "[Exchange1]   cert:    " << subject << std::endl;
                std::cout << "[Exchange1]   issuer:  " << issuer << std::endl;
                X509_free(cert);
            }
        }

        // -------------------------------------------------------
        // 5. WebSocket stream options
        // -------------------------------------------------------
        ws_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));

        ws_.set_option(websocket::stream_base::decorator(
            [this](websocket::request_type& req) {
                req.set(beast::http::field::user_agent,
                    std::string(BOOST_BEAST_VERSION_STRING) + " Exchange1/1.0");
                req.set(beast::http::field::connection, "Upgrade");
                req.set(beast::http::field::upgrade, "websocket");
            }
        ));

        ws_.binary(false);

        // -------------------------------------------------------
        // 6. WebSocket Handshake (HTTP Upgrade)
        // -------------------------------------------------------
        std::cout << "[Exchange1] WebSocket handshake:" << std::endl;
        std::cout << "[Exchange1]   Host:   " << ws_host_ << std::endl;
        std::cout << "[Exchange1]   Target: " << ws_target_ << std::endl;

        ws_.handshake(handshake_res, ws_host_, ws_target_);

        std::cout << "[Exchange1] WebSocket handshake complete (" << handshake_res.result_int() << " " << handshake_res.reason() << ")" << std::endl;
        connected_ = true;
        std::cout << "[Exchange1] Connected to Binance WebSocket" << std::endl;
        std::cout << "[Exchange1]   endpoint: wss://" << ws_host_ << ws_target_ << std::endl;
    }
    catch (beast::system_error const& e) {
        std::cerr << "[Exchange1] Connection error (beast): " << e.what() << std::endl;
        std::cerr << "[Exchange1]   error code: " << e.code() << " (" << e.code().message() << ")" << std::endl;
        if (handshake_res.result_int() > 0) {
            std::cerr << "[Exchange1]   HTTP response:" << std::endl;
            std::cerr << "[Exchange1]     status: " << handshake_res.result_int() << " "
                      << handshake_res.reason() << std::endl;
            for (auto const& field : handshake_res) {
                std::cerr << "[Exchange1]     " << field.name_string() << ": "
                          << field.value() << std::endl;
            }
            std::string const& body = handshake_res.body();
            if (!body.empty()) {
                std::cerr << "[Exchange1]     body: " << body.substr(0, 1024) << std::endl;
            }
        }
        connected_ = false;
        throw;
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
    running_ = true;
    io_thread_ = std::thread(&Exchange1::run_io_context, this);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    try {
        perform_connect();
    } catch (...) {
        std::cerr << "[Exchange1] Initial connect failed, reader will retry" << std::endl;
        connected_ = false;
    }
}

void Exchange1::subscribe(const std::vector<std::string>& symbols) {
    symbols_ = symbols;
    if (!connected_) {
        std::cerr << "[Exchange1] Not connected. Will subscribe on reconnect." << std::endl;
        return;
    }
    std::lock_guard<std::mutex> lock(ws_mutex_);
    std::cout << "[Exchange1] Subscribing to " << symbols.size() << " symbols" << std::endl;
    for (const auto& s : symbols) {
        std::cout << "[Exchange1]   symbol=" << s << std::endl;
    }
    try {
        send_subscribe(symbols);
    } catch (std::exception const& e) {
        std::cerr << "[Exchange1] Subscribe write failed: " << e.what() << std::endl;
    }

    try {
        beast::flat_buffer buffer;
        ws_.read(buffer);
        std::string ack = beast::buffers_to_string(buffer.data());
        std::cout << "[Exchange1] Subscription response: " << ack << std::endl;
        auto j = nlohmann::json::parse(ack);
        if (j.contains("result") && j["result"].is_null() && j.contains("id")) {
            std::cout << "[Exchange1] Subscription acknowledged (id=" << j["id"] << ")" << std::endl;
        } else if (j.contains("error")) {
            std::cerr << "[Exchange1] Subscription error: " << j["error"] << std::endl;
        }
    } catch (beast::system_error const& e) {
        std::cerr << "[Exchange1] Subscription ack read (expected if data came first): " << e.what() << std::endl;
    } catch (std::exception const& e) {
        std::cerr << "[Exchange1] Subscription ack parse error: " << e.what() << std::endl;
    }

    start_reader();
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
        }
    }
}

void Exchange1::read_loop() {
    beast::flat_buffer buffer;
    int reconnect_delay = 1;
    int reconnect_attempt = 0;

    while (running_) {
        if (!connected_) {
            reconnect_attempt++;
            std::cout << "[Exchange1] Reconnect #" << reconnect_attempt
                      << " in " << reconnect_delay << "s..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(reconnect_delay));
            reconnect_delay = std::min(reconnect_delay * 2, 30);

            {
                std::lock_guard<std::mutex> lock(ws_mutex_);
                if (ws_.is_open()) {
                    beast::error_code ec;
                    ws_.close(websocket::close_code::normal, ec);
                }
                ws_.~stream();
                new (&ws_) websocket::stream<ssl::stream<tcp::socket>>(ioc_, ctx_);
            }

            if (ioc_.stopped()) {
                std::cout << "[Exchange1] Restarting io_context" << std::endl;
                ioc_.restart();
            }

            if (io_thread_.joinable()) io_thread_.join();
            running_ = true;
            io_thread_ = std::thread(&Exchange1::run_io_context, this);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            try {
                perform_connect();
                reconnect_delay = 1;
                reconnect_attempt = 0;
                if (!symbols_.empty()) {
                    send_subscribe(symbols_);
                    try {
                        buffer.consume(buffer.size());
                        ws_.read(buffer);
                        std::string ack = beast::buffers_to_string(buffer.data());
                        std::cout << "[Exchange1] Subscription ack (reconnect): " << ack << std::endl;
                    } catch (...) { }
                }
                std::cout << "[Exchange1] Reconnect successful" << std::endl;
            } catch (std::exception const& e) {
                std::cerr << "[Exchange1] Reconnect failed: " << e.what() << std::endl;
                connected_ = false;
                continue;
            }
        }

        try {
            // --- START: Measure exchange latency from message arrival ---
            auto exchange_start = std::chrono::steady_clock::now();

            std::lock_guard<std::mutex> lock(ws_mutex_);
            buffer.consume(buffer.size());
            ws_.read(buffer);
            std::string msg = beast::buffers_to_string(buffer.data());

            auto j = nlohmann::json::parse(msg);

            if (j.contains("result") && j.contains("id")) {
                std::cout << "[Exchange1] Subscription ack: " << msg << std::endl;
                continue;
            }
            if (j.contains("error")) {
                std::cerr << "[Exchange1] Binance error: " << msg << std::endl;
                continue;
            }

            // Raw /ws format — ticker data is at the root
            if (j.contains("e") && j["e"] == "24hrTicker" &&
                j.contains("s") && j.contains("c") &&
                j.contains("b") && j.contains("a") && j.contains("E"))
            {
                std::string symbol = j["s"].get<std::string>();
                double price = std::stod(j["c"].get<std::string>());
                double bid   = std::stod(j["b"].get<std::string>());
                double ask   = std::stod(j["a"].get<std::string>());
                long timestamp = j["E"].get<long>();

                // --- Record exchange latency before calling callback ---
                auto exchange_end = std::chrono::steady_clock::now();
                double exchange_latency_ms = std::chrono::duration<double, std::milli>(exchange_end - exchange_start).count();

                  // --- Record exchange latency ---
                if (collector_) {
                    collector_->latencyTracker().endLatencyMeasurement(
                        exchange_start, LatencyTracker::LatencyCategory::EXCHANGE);
                }

                if (callback_) {
                    callback_(symbol, price, bid, ask, timestamp);
                }
            } else {
                std::cout << "[Exchange1] Unrecognized: " << msg.substr(0, 200) << std::endl;
            }
        }
        catch (websocket::close_reason const& cr) {
            std::cerr << "[Exchange1] Closed by server: code=" << cr.code
                      << " reason=\"" << cr.reason << "\"" << std::endl;
            connected_ = false;
            continue;
        }
        catch (beast::system_error const& e) {
            std::cerr << "[Exchange1] Beast error: " << e.what()
                      << " (code=" << e.code() << ")" << std::endl;
            connected_ = false;
            if (running_) continue;
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
    std::cout << "[Exchange1] Sending SUBSCRIBE: " << msg << std::endl;
    ws_.write(net::buffer(msg));
    std::cout << "[Exchange1] Subscribed to tickers for: ";
    for (const auto& s : symbols) std::cout << s << " ";
    std::cout << std::endl;
}
