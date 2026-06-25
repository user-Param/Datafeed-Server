// sadapter.cpp — pqxx 7.x compatible implementation
// Uses pqxx::params + txn.exec(query, params) throughout.
// Nulls are passed as pqxx::params::append() with no argument.
#include "sadapter.h"
#include <pqxx/pqxx>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <ctime>
#include <optional>

namespace datafeed {

// ─── Helpers ─────────────────────────────────────────────────────────────────

std::string SAdapter::timestamp_to_string(uint64_t ms) {
    auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(ms));
    auto t  = std::chrono::system_clock::to_time_t(tp);
    std::tm tm = *std::gmtime(&t);
    int millis  = static_cast<int>(ms % 1000);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S")
        << '.' << std::setfill('0') << std::setw(3) << millis;
    return oss.str();
}

uint64_t SAdapter::string_to_timestamp(const std::string& str) {
    std::tm tm = {};
    std::istringstream iss(str);
    iss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    if (iss.fail()) throw std::runtime_error("Bad timestamp: " + str);
    auto tp = std::chrono::system_clock::from_time_t(std::mktime(&tm));
    auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch());
    std::string frac; char dot;
    if (iss >> dot && dot == '.' && iss >> frac) {
        frac.resize(3, '0');
        dur += std::chrono::milliseconds(std::stoi(frac));
    }
    return static_cast<uint64_t>(dur.count());
}

std::string SAdapter::interval_to_string(int64_t seconds) {
    return std::to_string(seconds) + " seconds";
}

int64_t SAdapter::string_to_interval(const std::string& str) {
    std::istringstream iss(str); int64_t s; iss >> s; return s;
}

// ─── Connection ──────────────────────────────────────────────────────────────

SAdapter::SAdapter(const std::string& cs) : connection_string_(cs) {}
SAdapter::~SAdapter() { disconnect(); }

bool SAdapter::connect() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    try {
        conn_ = std::make_unique<pqxx::connection>(connection_string_);
        return conn_->is_open();
    } catch (const std::exception& e) {
        std::cerr << "DB connect failed: " << e.what() << "\n"; return false;
    }
}

void SAdapter::disconnect() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (conn_) { try { conn_->close(); } catch (...) {} conn_.reset(); }
}

bool SAdapter::is_connected() const {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    return conn_ && conn_->is_open();
}

// ─── Helper macro: append optional value or NULL ─────────────────────────────
// Usage: APPEND_OPT(p, opt_value)          — appends *opt or null
// Usage: APPEND_OPT_TS(p, opt_ts)          — appends timestamp string or null
// Usage: APPEND_OPT_IV(p, opt_seconds)     — appends interval string or null
#define APPEND_OPT(p, v)    do { if ((v)) p.append(*(v)); else p.append(); } while(0)
#define APPEND_OPT_TS(p,v)  do { if ((v)) p.append(timestamp_to_string(*(v))); else p.append(); } while(0)
#define APPEND_OPT_IV(p,v)  do { if ((v)) p.append(interval_to_string(*(v))); else p.append(); } while(0)

// ─── Client operations ───────────────────────────────────────────────────────

std::optional<Client> SAdapter::create_client(const Client& c) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        p.append(c.client_name); p.append(c.plan); p.append(c.status);
        APPEND_OPT(p, c.auth_subject); APPEND_OPT(p, c.ip_address); APPEND_OPT(p, c.user_agent);
        auto r = txn.exec(
            "INSERT INTO clients(client_name,plan,status,auth_subject,ip_address,user_agent)"
            " VALUES($1,$2,$3,$4,$5,$6) RETURNING tenant_id,created_at,updated_at", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        Client out = c;
        out.tenant_id  = r[0]["tenant_id"].as<std::string>();
        out.created_at = string_to_timestamp(r[0]["created_at"].as<std::string>());
        out.updated_at = string_to_timestamp(r[0]["updated_at"].as<std::string>());
        return out;
    } catch (const std::exception& e) { std::cerr << "create_client: " << e.what() << "\n"; return std::nullopt; }
}

std::optional<Client> SAdapter::get_client_by_id(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("SELECT * FROM clients WHERE tenant_id=$1", p);
        if (r.empty()) return std::nullopt;
        Client c;
        c.tenant_id=r[0]["tenant_id"].as<std::string>(); c.client_name=r[0]["client_name"].as<std::string>();
        c.plan=r[0]["plan"].as<std::string>(); c.status=r[0]["status"].as<std::string>();
        c.created_at=string_to_timestamp(r[0]["created_at"].as<std::string>());
        c.updated_at=string_to_timestamp(r[0]["updated_at"].as<std::string>());
        if (!r[0]["last_seen_at"].is_null()) c.last_seen_at=string_to_timestamp(r[0]["last_seen_at"].as<std::string>());
        if (!r[0]["auth_subject"].is_null()) c.auth_subject=r[0]["auth_subject"].as<std::string>();
        if (!r[0]["ip_address"].is_null())   c.ip_address=r[0]["ip_address"].as<std::string>();
        if (!r[0]["user_agent"].is_null())   c.user_agent=r[0]["user_agent"].as<std::string>();
        return c;
    } catch (const std::exception& e) { std::cerr << "get_client_by_id: " << e.what() << "\n"; return std::nullopt; }
}

std::vector<Client> SAdapter::get_clients_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    std::vector<Client> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*conn_);
        std::string q = "SELECT * FROM clients";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            Client c;
            c.tenant_id=row["tenant_id"].as<std::string>(); c.client_name=row["client_name"].as<std::string>();
            c.plan=row["plan"].as<std::string>(); c.status=row["status"].as<std::string>();
            c.created_at=string_to_timestamp(row["created_at"].as<std::string>());
            c.updated_at=string_to_timestamp(row["updated_at"].as<std::string>());
            if (!row["last_seen_at"].is_null()) c.last_seen_at=string_to_timestamp(row["last_seen_at"].as<std::string>());
            if (!row["auth_subject"].is_null()) c.auth_subject=row["auth_subject"].as<std::string>();
            if (!row["ip_address"].is_null())   c.ip_address=row["ip_address"].as<std::string>();
            if (!row["user_agent"].is_null())   c.user_agent=row["user_agent"].as<std::string>();
            out.push_back(c);
        }
        txn.commit();
    } catch (const std::exception& e) { std::cerr << "get_clients: " << e.what() << "\n"; }
    return out;
}

bool SAdapter::update_client(const Client& c) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || c.tenant_id.empty()) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        p.append(c.client_name); p.append(c.plan); p.append(c.status);
        APPEND_OPT(p, c.auth_subject); APPEND_OPT(p, c.ip_address); APPEND_OPT(p, c.user_agent);
        p.append(c.tenant_id);
        auto r = txn.exec(
            "UPDATE clients SET client_name=$1,plan=$2,status=$3,auth_subject=$4,"
            "ip_address=$5,user_agent=$6,updated_at=NOW() WHERE tenant_id=$7 RETURNING tenant_id", p);
        txn.commit(); return !r.empty();
    } catch (const std::exception& e) { std::cerr << "update_client: " << e.what() << "\n"; return false; }
}

bool SAdapter::delete_client(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || id.empty()) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("DELETE FROM clients WHERE tenant_id=$1", p);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "delete_client: " << e.what() << "\n"; return false; }
}

// ─── Session operations ──────────────────────────────────────────────────────

std::optional<Session> SAdapter::create_session(const Session& s) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        APPEND_OPT(p, s.connection_id);
        p.append(timestamp_to_string(s.connected_at));
        APPEND_OPT_TS(p, s.disconnected_at);
        APPEND_OPT(p, s.disconnect_reason); APPEND_OPT(p, s.auth_status);
        APPEND_OPT(p, s.reconnect_count);
        APPEND_OPT_IV(p, s.heartbeat_interval);
        p.append(s.protocol); p.append(s.instance_id); APPEND_OPT(p, s.tenant_id);
        auto r = txn.exec(
            "INSERT INTO sessions(connection_id,connected_at,disconnected_at,disconnect_reason,"
            "auth_status,reconnect_count,heartbeat_interval,protocol,instance_id,tenant_id)"
            " VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING session_id", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        Session out = s; out.session_id = r[0]["session_id"].as<std::string>(); return out;
    } catch (const std::exception& e) { std::cerr << "create_session: " << e.what() << "\n"; return std::nullopt; }
}

std::optional<Session> SAdapter::get_session_by_id(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("SELECT * FROM sessions WHERE session_id=$1", p);
        if (r.empty()) return std::nullopt;
        Session s;
        s.session_id=r[0]["session_id"].as<std::string>();
        if (!r[0]["connection_id"].is_null()) s.connection_id=r[0]["connection_id"].as<std::string>();
        s.connected_at=string_to_timestamp(r[0]["connected_at"].as<std::string>());
        if (!r[0]["disconnected_at"].is_null()) s.disconnected_at=string_to_timestamp(r[0]["disconnected_at"].as<std::string>());
        if (!r[0]["disconnect_reason"].is_null()) s.disconnect_reason=r[0]["disconnect_reason"].as<std::string>();
        if (!r[0]["auth_status"].is_null()) s.auth_status=r[0]["auth_status"].as<std::string>();
        if (!r[0]["reconnect_count"].is_null()) s.reconnect_count=r[0]["reconnect_count"].as<int64_t>();
        if (!r[0]["heartbeat_interval"].is_null()) s.heartbeat_interval=string_to_interval(r[0]["heartbeat_interval"].as<std::string>());
        s.protocol=r[0]["protocol"].as<std::string>(); s.instance_id=r[0]["instance_id"].as<std::string>();
        if (!r[0]["tenant_id"].is_null()) s.tenant_id=r[0]["tenant_id"].as<std::string>();
        return s;
    } catch (const std::exception& e) { std::cerr << "get_session_by_id: " << e.what() << "\n"; return std::nullopt; }
}

std::vector<Session> SAdapter::get_sessions_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    std::vector<Session> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*conn_);
        std::string q = "SELECT * FROM sessions";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            Session s;
            s.session_id=row["session_id"].as<std::string>();
            if (!row["connection_id"].is_null()) s.connection_id=row["connection_id"].as<std::string>();
            s.connected_at=string_to_timestamp(row["connected_at"].as<std::string>());
            if (!row["disconnected_at"].is_null()) s.disconnected_at=string_to_timestamp(row["disconnected_at"].as<std::string>());
            if (!row["disconnect_reason"].is_null()) s.disconnect_reason=row["disconnect_reason"].as<std::string>();
            if (!row["auth_status"].is_null()) s.auth_status=row["auth_status"].as<std::string>();
            if (!row["reconnect_count"].is_null()) s.reconnect_count=row["reconnect_count"].as<int64_t>();
            if (!row["heartbeat_interval"].is_null()) s.heartbeat_interval=string_to_interval(row["heartbeat_interval"].as<std::string>());
            s.protocol=row["protocol"].as<std::string>(); s.instance_id=row["instance_id"].as<std::string>();
            if (!row["tenant_id"].is_null()) s.tenant_id=row["tenant_id"].as<std::string>();
            out.push_back(s);
        }
        txn.commit();
    } catch (const std::exception& e) { std::cerr << "get_sessions: " << e.what() << "\n"; }
    return out;
}

bool SAdapter::update_session(const Session& s) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || s.session_id.empty()) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        APPEND_OPT(p, s.connection_id);
        p.append(timestamp_to_string(s.connected_at));
        APPEND_OPT_TS(p, s.disconnected_at);
        APPEND_OPT(p, s.disconnect_reason); APPEND_OPT(p, s.auth_status);
        APPEND_OPT(p, s.reconnect_count);
        APPEND_OPT_IV(p, s.heartbeat_interval);
        p.append(s.protocol); p.append(s.instance_id); APPEND_OPT(p, s.tenant_id);
        p.append(s.session_id);
        auto r = txn.exec(
            "UPDATE sessions SET connection_id=$1,connected_at=$2,disconnected_at=$3,"
            "disconnect_reason=$4,auth_status=$5,reconnect_count=$6,heartbeat_interval=$7,"
            "protocol=$8,instance_id=$9,tenant_id=$10 WHERE session_id=$11", p);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "update_session: " << e.what() << "\n"; return false; }
}

bool SAdapter::delete_session(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || id.empty()) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("DELETE FROM sessions WHERE session_id=$1", p);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "delete_session: " << e.what() << "\n"; return false; }
}

// ─── Subscription operations ─────────────────────────────────────────────────

std::optional<Subscription> SAdapter::create_subscription(const Subscription& sub) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        p.append(sub.symbol); p.append(sub.topic);
        APPEND_OPT(p, sub.stream_type); APPEND_OPT(p, sub.mode); APPEND_OPT(p, sub.filters_json);
        APPEND_OPT(p, sub.priority); APPEND_OPT(p, sub.tenant_id); APPEND_OPT(p, sub.session_id);
        auto r = txn.exec(
            "INSERT INTO subscriptions(symbol,topic,stream_type,mode,filters_json,priority,tenant_id,session_id)"
            " VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING subscription_id,created_at", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        Subscription out = sub;
        out.subscription_id=r[0]["subscription_id"].as<std::string>();
        out.created_at=string_to_timestamp(r[0]["created_at"].as<std::string>());
        return out;
    } catch (const std::exception& e) { std::cerr << "create_subscription: " << e.what() << "\n"; return std::nullopt; }
}

std::optional<Subscription> SAdapter::get_subscription_by_id(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("SELECT * FROM subscriptions WHERE subscription_id=$1", p);
        if (r.empty()) return std::nullopt;
        Subscription s;
        s.subscription_id=r[0]["subscription_id"].as<std::string>();
        s.symbol=r[0]["symbol"].as<std::string>(); s.topic=r[0]["topic"].as<std::string>();
        if (!r[0]["stream_type"].is_null())  s.stream_type=r[0]["stream_type"].as<std::string>();
        if (!r[0]["mode"].is_null())         s.mode=r[0]["mode"].as<std::string>();
        if (!r[0]["filters_json"].is_null()) s.filters_json=r[0]["filters_json"].as<std::string>();
        if (!r[0]["priority"].is_null())     s.priority=r[0]["priority"].as<int64_t>();
        s.created_at=string_to_timestamp(r[0]["created_at"].as<std::string>());
        if (!r[0]["removed_at"].is_null())   s.removed_at=string_to_timestamp(r[0]["removed_at"].as<std::string>());
        s.is_active=r[0]["is_active"].as<bool>();
        if (!r[0]["tenant_id"].is_null())    s.tenant_id=r[0]["tenant_id"].as<std::string>();
        if (!r[0]["session_id"].is_null())   s.session_id=r[0]["session_id"].as<std::string>();
        return s;
    } catch (const std::exception& e) { std::cerr << "get_subscription_by_id: " << e.what() << "\n"; return std::nullopt; }
}

std::vector<Subscription> SAdapter::get_subscriptions_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    std::vector<Subscription> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*conn_);
        std::string q = "SELECT * FROM subscriptions";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            Subscription s;
            s.subscription_id=row["subscription_id"].as<std::string>();
            s.symbol=row["symbol"].as<std::string>(); s.topic=row["topic"].as<std::string>();
            if (!row["stream_type"].is_null())  s.stream_type=row["stream_type"].as<std::string>();
            if (!row["mode"].is_null())         s.mode=row["mode"].as<std::string>();
            if (!row["filters_json"].is_null()) s.filters_json=row["filters_json"].as<std::string>();
            if (!row["priority"].is_null())     s.priority=row["priority"].as<int64_t>();
            s.created_at=string_to_timestamp(row["created_at"].as<std::string>());
            if (!row["removed_at"].is_null())   s.removed_at=string_to_timestamp(row["removed_at"].as<std::string>());
            s.is_active=row["is_active"].as<bool>();
            if (!row["tenant_id"].is_null())    s.tenant_id=row["tenant_id"].as<std::string>();
            if (!row["session_id"].is_null())   s.session_id=row["session_id"].as<std::string>();
            out.push_back(s);
        }
        txn.commit();
    } catch (const std::exception& e) { std::cerr << "get_subscriptions: " << e.what() << "\n"; }
    return out;
}

bool SAdapter::update_subscription(const Subscription& sub) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || sub.subscription_id.empty()) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        p.append(sub.symbol); p.append(sub.topic);
        APPEND_OPT(p, sub.stream_type); APPEND_OPT(p, sub.mode); APPEND_OPT(p, sub.filters_json);
        APPEND_OPT(p, sub.priority); APPEND_OPT(p, sub.tenant_id); APPEND_OPT(p, sub.session_id);
        p.append(sub.is_active);
        APPEND_OPT_TS(p, sub.removed_at);
        p.append(sub.subscription_id);
        auto r = txn.exec(
            "UPDATE subscriptions SET symbol=$1,topic=$2,stream_type=$3,mode=$4,filters_json=$5,"
            "priority=$6,tenant_id=$7,session_id=$8,is_active=$9,removed_at=$10"
            " WHERE subscription_id=$11", p);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "update_subscription: " << e.what() << "\n"; return false; }
}

bool SAdapter::delete_subscription(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || id.empty()) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("DELETE FROM subscriptions WHERE subscription_id=$1", p);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "delete_subscription: " << e.what() << "\n"; return false; }
}

// ─── FeedInstance operations ─────────────────────────────────────────────────

static void fill_feed_instance(datafeed::FeedInstance& fi, const pqxx::row& row,
                                std::function<uint64_t(const std::string&)> ts_fn,
                                std::function<int64_t(const std::string&)> iv_fn) {
    fi.instance_id=row["instance_id"].as<std::string>(); fi.exchange=row["exchange"].as<std::string>();
    if (!row["adapter_type"].is_null()) fi.adapter_type=row["adapter_type"].as<std::string>();
    fi.feed_status=row["feed_status"].as<std::string>();
    if (!row["last_tick_at"].is_null()) fi.last_tick_at=ts_fn(row["last_tick_at"].as<std::string>());
    if (!row["stale_seconds"].is_null()) fi.stale_seconds=iv_fn(row["stale_seconds"].as<std::string>());
    fi.reconnect_attempts=row["reconnect_attempts"].as<int64_t>();
    if (!row["message_rate_in"].is_null())    fi.message_rate_in=row["message_rate_in"].as<double>();
    if (!row["message_rate_out"].is_null())   fi.message_rate_out=row["message_rate_out"].as<double>();
    if (!row["queue_depth"].is_null())        fi.queue_depth=row["queue_depth"].as<int64_t>();
    if (!row["backpressure_active"].is_null()) fi.backpressure_active=row["backpressure_active"].as<bool>();
    if (!row["serialization_ms"].is_null())  fi.serialization_ms=row["serialization_ms"].as<double>();
    if (!row["parse_error_count"].is_null()) fi.parse_error_count=row["parse_error_count"].as<int64_t>();
    if (!row["gap_count"].is_null())         fi.gap_count=row["gap_count"].as<int64_t>();
    if (!row["duplicate_count"].is_null())   fi.duplicate_count=row["duplicate_count"].as<int64_t>();
    if (!row["out_of_order_count"].is_null()) fi.out_of_order_count=row["out_of_order_count"].as<int64_t>();
    if (!row["tenant_id"].is_null())         fi.tenant_id=row["tenant_id"].as<std::string>();
}

std::optional<FeedInstance> SAdapter::create_feed_instance(const FeedInstance& fi) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        p.append(fi.instance_id); p.append(fi.exchange);
        APPEND_OPT(p, fi.adapter_type); p.append(fi.feed_status);
        APPEND_OPT_TS(p, fi.last_tick_at); APPEND_OPT_IV(p, fi.stale_seconds);
        p.append(fi.reconnect_attempts);
        APPEND_OPT(p, fi.message_rate_in); APPEND_OPT(p, fi.message_rate_out);
        APPEND_OPT(p, fi.queue_depth); APPEND_OPT(p, fi.backpressure_active);
        APPEND_OPT(p, fi.serialization_ms); APPEND_OPT(p, fi.parse_error_count);
        APPEND_OPT(p, fi.gap_count); APPEND_OPT(p, fi.duplicate_count);
        APPEND_OPT(p, fi.out_of_order_count); APPEND_OPT(p, fi.tenant_id);
        auto r = txn.exec(
            "INSERT INTO feed_instances(instance_id,exchange,adapter_type,feed_status,last_tick_at,"
            "stale_seconds,reconnect_attempts,message_rate_in,message_rate_out,queue_depth,"
            "backpressure_active,serialization_ms,parse_error_count,gap_count,duplicate_count,"
            "out_of_order_count,tenant_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)"
            " RETURNING instance_id", p);
        txn.commit(); if (r.empty()) return std::nullopt; return fi;
    } catch (const std::exception& e) { std::cerr << "create_feed_instance: " << e.what() << "\n"; return std::nullopt; }
}

std::optional<FeedInstance> SAdapter::get_feed_instance_by_id(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("SELECT * FROM feed_instances WHERE instance_id=$1", p);
        if (r.empty()) return std::nullopt;
        FeedInstance fi;
        fill_feed_instance(fi, r[0],
            [this](const std::string& s){ return string_to_timestamp(s); },
            [this](const std::string& s){ return string_to_interval(s); });
        return fi;
    } catch (const std::exception& e) { std::cerr << "get_feed_instance_by_id: " << e.what() << "\n"; return std::nullopt; }
}

std::vector<FeedInstance> SAdapter::get_feed_instances_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    std::vector<FeedInstance> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*conn_);
        std::string q = "SELECT * FROM feed_instances";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            FeedInstance fi;
            fill_feed_instance(fi, row,
                [this](const std::string& s){ return string_to_timestamp(s); },
                [this](const std::string& s){ return string_to_interval(s); });
            out.push_back(fi);
        }
        txn.commit();
    } catch (const std::exception& e) { std::cerr << "get_feed_instances: " << e.what() << "\n"; }
    return out;
}

bool SAdapter::update_feed_instance(const FeedInstance& fi) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || fi.instance_id.empty()) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        p.append(fi.exchange); APPEND_OPT(p, fi.adapter_type); p.append(fi.feed_status);
        APPEND_OPT_TS(p, fi.last_tick_at); APPEND_OPT_IV(p, fi.stale_seconds);
        p.append(fi.reconnect_attempts);
        APPEND_OPT(p, fi.message_rate_in); APPEND_OPT(p, fi.message_rate_out);
        APPEND_OPT(p, fi.queue_depth); APPEND_OPT(p, fi.backpressure_active);
        APPEND_OPT(p, fi.serialization_ms); APPEND_OPT(p, fi.parse_error_count);
        APPEND_OPT(p, fi.gap_count); APPEND_OPT(p, fi.duplicate_count);
        APPEND_OPT(p, fi.out_of_order_count); APPEND_OPT(p, fi.tenant_id);
        p.append(fi.instance_id);
        auto r = txn.exec(
            "UPDATE feed_instances SET exchange=$1,adapter_type=$2,feed_status=$3,last_tick_at=$4,"
            "stale_seconds=$5,reconnect_attempts=$6,message_rate_in=$7,message_rate_out=$8,"
            "queue_depth=$9,backpressure_active=$10,serialization_ms=$11,parse_error_count=$12,"
            "gap_count=$13,duplicate_count=$14,out_of_order_count=$15,tenant_id=$16"
            " WHERE instance_id=$17", p);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "update_feed_instance: " << e.what() << "\n"; return false; }
}

bool SAdapter::delete_feed_instance(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || id.empty()) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("DELETE FROM feed_instances WHERE instance_id=$1", p);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "delete_feed_instance: " << e.what() << "\n"; return false; }
}

// ─── FeedMetricsSnapshot operations ─────────────────────────────────────────

std::optional<FeedMetricsSnapshot> SAdapter::create_feed_metrics_snapshot(const FeedMetricsSnapshot& s) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        p.append(s.instance_id); p.append(timestamp_to_string(s.measured_at));
        APPEND_OPT(p,s.p50_latency_ms); APPEND_OPT(p,s.p95_latency_ms); APPEND_OPT(p,s.p99_latency_ms);
        APPEND_OPT(p,s.avg_latency_ms); APPEND_OPT(p,s.drop_rate); APPEND_OPT(p,s.packet_loss_rate);
        APPEND_OPT(p,s.msgs_sent); APPEND_OPT(p,s.msgs_received);
        APPEND_OPT(p,s.bytes_sent); APPEND_OPT(p,s.bytes_received);
        APPEND_OPT(p,s.cpu_usage); APPEND_OPT(p,s.memory_usage); APPEND_OPT(p,s.thread_count);
        APPEND_OPT(p,s.event_loop_lag_ms); APPEND_OPT(p,s.uptime_seconds);
        auto r = txn.exec(
            "INSERT INTO feed_metrics_snapshots(instance_id,measured_at,p50_latency_ms,p95_latency_ms,"
            "p99_latency_ms,avg_latency_ms,drop_rate,packet_loss_rate,msgs_sent,msgs_received,"
            "bytes_sent,bytes_received,cpu_usage,memory_usage,thread_count,event_loop_lag_ms,uptime_seconds)"
            " VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17) RETURNING id", p);
        txn.commit(); if (r.empty()) return std::nullopt;
        FeedMetricsSnapshot out=s; out.id=r[0]["id"].as<int64_t>(); return out;
    } catch (const std::exception& e) { std::cerr << "create_metrics_snapshot: " << e.what() << "\n"; return std::nullopt; }
}

std::optional<FeedMetricsSnapshot> SAdapter::get_feed_metrics_snapshot_by_id(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("SELECT * FROM feed_metrics_snapshots WHERE id=$1", p);
        if (r.empty()) return std::nullopt;
        FeedMetricsSnapshot s;
        s.id=r[0]["id"].as<int64_t>(); s.instance_id=r[0]["instance_id"].as<std::string>();
        s.measured_at=string_to_timestamp(r[0]["measured_at"].as<std::string>());
        if (!r[0]["p50_latency_ms"].is_null()) s.p50_latency_ms=r[0]["p50_latency_ms"].as<double>();
        if (!r[0]["p95_latency_ms"].is_null()) s.p95_latency_ms=r[0]["p95_latency_ms"].as<double>();
        if (!r[0]["p99_latency_ms"].is_null()) s.p99_latency_ms=r[0]["p99_latency_ms"].as<double>();
        if (!r[0]["avg_latency_ms"].is_null()) s.avg_latency_ms=r[0]["avg_latency_ms"].as<double>();
        if (!r[0]["drop_rate"].is_null())      s.drop_rate=r[0]["drop_rate"].as<double>();
        if (!r[0]["packet_loss_rate"].is_null()) s.packet_loss_rate=r[0]["packet_loss_rate"].as<double>();
        if (!r[0]["msgs_sent"].is_null())      s.msgs_sent=r[0]["msgs_sent"].as<int64_t>();
        if (!r[0]["msgs_received"].is_null())  s.msgs_received=r[0]["msgs_received"].as<int64_t>();
        if (!r[0]["bytes_sent"].is_null())     s.bytes_sent=r[0]["bytes_sent"].as<int64_t>();
        if (!r[0]["bytes_received"].is_null()) s.bytes_received=r[0]["bytes_received"].as<int64_t>();
        if (!r[0]["cpu_usage"].is_null())      s.cpu_usage=r[0]["cpu_usage"].as<double>();
        if (!r[0]["memory_usage"].is_null())   s.memory_usage=r[0]["memory_usage"].as<int64_t>();
        if (!r[0]["thread_count"].is_null())   s.thread_count=r[0]["thread_count"].as<int64_t>();
        if (!r[0]["event_loop_lag_ms"].is_null()) s.event_loop_lag_ms=r[0]["event_loop_lag_ms"].as<double>();
        if (!r[0]["uptime_seconds"].is_null()) s.uptime_seconds=r[0]["uptime_seconds"].as<int64_t>();
        return s;
    } catch (const std::exception& e) { std::cerr << "get_metrics_snapshot: " << e.what() << "\n"; return std::nullopt; }
}

std::vector<FeedMetricsSnapshot> SAdapter::get_feed_metrics_snapshots_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    std::vector<FeedMetricsSnapshot> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*conn_);
        std::string q = "SELECT * FROM feed_metrics_snapshots";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            FeedMetricsSnapshot s;
            s.id=row["id"].as<int64_t>(); s.instance_id=row["instance_id"].as<std::string>();
            s.measured_at=string_to_timestamp(row["measured_at"].as<std::string>());
            if (!row["p50_latency_ms"].is_null()) s.p50_latency_ms=row["p50_latency_ms"].as<double>();
            if (!row["p95_latency_ms"].is_null()) s.p95_latency_ms=row["p95_latency_ms"].as<double>();
            if (!row["p99_latency_ms"].is_null()) s.p99_latency_ms=row["p99_latency_ms"].as<double>();
            if (!row["avg_latency_ms"].is_null()) s.avg_latency_ms=row["avg_latency_ms"].as<double>();
            if (!row["drop_rate"].is_null())      s.drop_rate=row["drop_rate"].as<double>();
            if (!row["packet_loss_rate"].is_null()) s.packet_loss_rate=row["packet_loss_rate"].as<double>();
            if (!row["msgs_sent"].is_null())      s.msgs_sent=row["msgs_sent"].as<int64_t>();
            if (!row["msgs_received"].is_null())  s.msgs_received=row["msgs_received"].as<int64_t>();
            if (!row["bytes_sent"].is_null())     s.bytes_sent=row["bytes_sent"].as<int64_t>();
            if (!row["bytes_received"].is_null()) s.bytes_received=row["bytes_received"].as<int64_t>();
            if (!row["cpu_usage"].is_null())      s.cpu_usage=row["cpu_usage"].as<double>();
            if (!row["memory_usage"].is_null())   s.memory_usage=row["memory_usage"].as<int64_t>();
            if (!row["thread_count"].is_null())   s.thread_count=row["thread_count"].as<int64_t>();
            if (!row["event_loop_lag_ms"].is_null()) s.event_loop_lag_ms=row["event_loop_lag_ms"].as<double>();
            if (!row["uptime_seconds"].is_null()) s.uptime_seconds=row["uptime_seconds"].as<int64_t>();
            out.push_back(s);
        }
        txn.commit();
    } catch (const std::exception& e) { std::cerr << "get_metrics_snapshots: " << e.what() << "\n"; }
    return out;
}

bool SAdapter::update_feed_metrics_snapshot(const FeedMetricsSnapshot& s) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || s.id <= 0) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        p.append(s.instance_id); p.append(timestamp_to_string(s.measured_at));
        APPEND_OPT(p,s.p50_latency_ms); APPEND_OPT(p,s.p95_latency_ms); APPEND_OPT(p,s.p99_latency_ms);
        APPEND_OPT(p,s.avg_latency_ms); APPEND_OPT(p,s.drop_rate); APPEND_OPT(p,s.packet_loss_rate);
        APPEND_OPT(p,s.msgs_sent); APPEND_OPT(p,s.msgs_received);
        APPEND_OPT(p,s.bytes_sent); APPEND_OPT(p,s.bytes_received);
        APPEND_OPT(p,s.cpu_usage); APPEND_OPT(p,s.memory_usage); APPEND_OPT(p,s.thread_count);
        APPEND_OPT(p,s.event_loop_lag_ms); APPEND_OPT(p,s.uptime_seconds);
        p.append(s.id);
        auto r = txn.exec(
            "UPDATE feed_metrics_snapshots SET instance_id=$1,measured_at=$2,p50_latency_ms=$3,"
            "p95_latency_ms=$4,p99_latency_ms=$5,avg_latency_ms=$6,drop_rate=$7,packet_loss_rate=$8,"
            "msgs_sent=$9,msgs_received=$10,bytes_sent=$11,bytes_received=$12,cpu_usage=$13,"
            "memory_usage=$14,thread_count=$15,event_loop_lag_ms=$16,uptime_seconds=$17 WHERE id=$18", p);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "update_metrics_snapshot: " << e.what() << "\n"; return false; }
}

bool SAdapter::delete_feed_metrics_snapshot(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || id <= 0) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("DELETE FROM feed_metrics_snapshots WHERE id=$1", p);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "delete_metrics_snapshot: " << e.what() << "\n"; return false; }
}

// ─── FeedEvent operations ────────────────────────────────────────────────────

static void fill_feed_event(FeedEvent& ev, const pqxx::row& row,
                             std::function<uint64_t(const std::string&)> ts_fn) {
    ev.event_id = row["event_id"].as<std::string>();
    if (!row["actor_type"].is_null())    ev.actor_type = row["actor_type"].as<std::string>();
    if (!row["actor_id"].is_null())      ev.actor_id = row["actor_id"].as<std::string>();
    ev.action_type = row["action_type"].as<std::string>();
    if (!row["target_type"].is_null())   ev.target_type = row["target_type"].as<std::string>();
    if (!row["target_id"].is_null())     ev.target_id = row["target_id"].as<std::string>();
    if (!row["result"].is_null())        ev.result = row["result"].as<std::string>();
    if (!row["error_code"].is_null())    ev.error_code = row["error_code"].as<std::string>();
    if (!row["trace_id"].is_null())      ev.trace_id = row["trace_id"].as<std::string>();
    if (!row["correlation_id"].is_null()) ev.correlation_id = row["correlation_id"].as<std::string>();
    ev.occurred_at = ts_fn(row["occurred_at"].as<std::string>());
    if (!row["metadata"].is_null())      ev.metadata = row["metadata"].as<std::string>();
}

std::optional<FeedEvent> SAdapter::create_feed_event(const FeedEvent& ev) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        APPEND_OPT(p, ev.actor_type); APPEND_OPT(p, ev.actor_id);
        p.append(ev.action_type);
        APPEND_OPT(p, ev.target_type); APPEND_OPT(p, ev.target_id);
        APPEND_OPT(p, ev.result); APPEND_OPT(p, ev.error_code);
        APPEND_OPT(p, ev.trace_id); APPEND_OPT(p, ev.correlation_id);
        APPEND_OPT(p, ev.metadata);
        auto r = txn.exec(
            "INSERT INTO feed_events(actor_type,actor_id,action_type,target_type,target_id,"
            "result,error_code,trace_id,correlation_id,metadata)"
            " VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING event_id,occurred_at", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        FeedEvent out = ev;
        out.event_id = r[0]["event_id"].as<std::string>();
        out.occurred_at = string_to_timestamp(r[0]["occurred_at"].as<std::string>());
        return out;
    } catch (const std::exception& e) { std::cerr << "create_feed_event: " << e.what() << "\n"; return std::nullopt; }
}

std::optional<FeedEvent> SAdapter::get_feed_event_by_id(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("SELECT * FROM feed_events WHERE event_id=$1", p);
        if (r.empty()) return std::nullopt;
        FeedEvent ev;
        fill_feed_event(ev, r[0],
            [this](const std::string& s){ return string_to_timestamp(s); });
        return ev;
    } catch (const std::exception& e) { std::cerr << "get_feed_event_by_id: " << e.what() << "\n"; return std::nullopt; }
}

std::vector<FeedEvent> SAdapter::get_feed_events_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    std::vector<FeedEvent> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*conn_);
        std::string q = "SELECT * FROM feed_events";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            FeedEvent ev;
            fill_feed_event(ev, row,
                [this](const std::string& s){ return string_to_timestamp(s); });
            out.push_back(ev);
        }
        txn.commit();
    } catch (const std::exception& e) { std::cerr << "get_feed_events: " << e.what() << "\n"; }
    return out;
}

bool SAdapter::update_feed_event(const FeedEvent& ev) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || ev.event_id.empty()) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        APPEND_OPT(p, ev.actor_type); APPEND_OPT(p, ev.actor_id);
        p.append(ev.action_type);
        APPEND_OPT(p, ev.target_type); APPEND_OPT(p, ev.target_id);
        APPEND_OPT(p, ev.result); APPEND_OPT(p, ev.error_code);
        APPEND_OPT(p, ev.trace_id); APPEND_OPT(p, ev.correlation_id);
        APPEND_OPT(p, ev.metadata);
        p.append(ev.event_id);
        auto r = txn.exec(
            "UPDATE feed_events SET actor_type=$1,actor_id=$2,action_type=$3,target_type=$4,"
            "target_id=$5,result=$6,error_code=$7,trace_id=$8,correlation_id=$9,metadata=$10"
            " WHERE event_id=$11", p);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "update_feed_event: " << e.what() << "\n"; return false; }
}

bool SAdapter::delete_feed_event(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || id.empty()) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("DELETE FROM feed_events WHERE event_id=$1", p);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "delete_feed_event: " << e.what() << "\n"; return false; }
}

// ─── ApiRequest operations ───────────────────────────────────────────────────

static void fill_api_request(ApiRequest& r, const pqxx::row& row,
                              std::function<uint64_t(const std::string&)> ts_fn) {
    r.request_id = row["request_id"].as<std::string>();
    r.endpoint = row["endpoint"].as<std::string>();
    r.method = row["method"].as<std::string>();
    if (!row["status_code"].is_null())      r.status_code = row["status_code"].as<int64_t>();
    if (!row["latency_ms"].is_null())       r.latency_ms = row["latency_ms"].as<double>();
    if (!row["request_size"].is_null())     r.request_size = row["request_size"].as<int64_t>();
    if (!row["response_size"].is_null())    r.response_size = row["response_size"].as<int64_t>();
    if (!row["client_id"].is_null())        r.client_id = row["client_id"].as<std::string>();
    if (!row["session_id"].is_null())       r.session_id = row["session_id"].as<std::string>();
    if (!row["instance_id"].is_null())      r.instance_id = row["instance_id"].as<std::string>();
    r.timestamp = ts_fn(row["timestamp"].as<std::string>());
}

std::optional<ApiRequest> SAdapter::create_api_request(const ApiRequest& req) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        p.append(req.endpoint); p.append(req.method);
        APPEND_OPT(p, req.status_code); APPEND_OPT(p, req.latency_ms);
        APPEND_OPT(p, req.request_size); APPEND_OPT(p, req.response_size);
        APPEND_OPT(p, req.client_id); APPEND_OPT(p, req.session_id);
        APPEND_OPT(p, req.instance_id);
        auto r = txn.exec(
            "INSERT INTO api_requests(endpoint,method,status_code,latency_ms,request_size,"
            "response_size,client_id,session_id,instance_id)"
            " VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING request_id,timestamp", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        ApiRequest out = req;
        out.request_id = r[0]["request_id"].as<std::string>();
        out.timestamp = string_to_timestamp(r[0]["timestamp"].as<std::string>());
        return out;
    } catch (const std::exception& e) { std::cerr << "create_api_request: " << e.what() << "\n"; return std::nullopt; }
}

std::optional<ApiRequest> SAdapter::get_api_request_by_id(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("SELECT * FROM api_requests WHERE request_id=$1", p);
        if (r.empty()) return std::nullopt;
        ApiRequest ar;
        fill_api_request(ar, r[0],
            [this](const std::string& s){ return string_to_timestamp(s); });
        return ar;
    } catch (const std::exception& e) { std::cerr << "get_api_request_by_id: " << e.what() << "\n"; return std::nullopt; }
}

std::vector<ApiRequest> SAdapter::get_api_requests_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    std::vector<ApiRequest> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*conn_);
        std::string q = "SELECT * FROM api_requests";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            ApiRequest ar;
            fill_api_request(ar, row,
                [this](const std::string& s){ return string_to_timestamp(s); });
            out.push_back(ar);
        }
        txn.commit();
    } catch (const std::exception& e) { std::cerr << "get_api_requests: " << e.what() << "\n"; }
    return out;
}

bool SAdapter::update_api_request(const ApiRequest& req) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || req.request_id.empty()) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        p.append(req.endpoint); p.append(req.method);
        APPEND_OPT(p, req.status_code); APPEND_OPT(p, req.latency_ms);
        APPEND_OPT(p, req.request_size); APPEND_OPT(p, req.response_size);
        APPEND_OPT(p, req.client_id); APPEND_OPT(p, req.session_id);
        APPEND_OPT(p, req.instance_id);
        p.append(req.request_id);
        auto r = txn.exec(
            "UPDATE api_requests SET endpoint=$1,method=$2,status_code=$3,latency_ms=$4,"
            "request_size=$5,response_size=$6,client_id=$7,session_id=$8,instance_id=$9"
            " WHERE request_id=$10", p);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "update_api_request: " << e.what() << "\n"; return false; }
}

bool SAdapter::delete_api_request(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || id.empty()) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("DELETE FROM api_requests WHERE request_id=$1", p);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "delete_api_request: " << e.what() << "\n"; return false; }
}

// ─── ExchangeHealth operations ───────────────────────────────────────────────

static void fill_exchange_health(ExchangeHealth& h, const pqxx::row& row,
                                  std::function<uint64_t(const std::string&)> ts_fn) {
    h.id = row["id"].as<int64_t>();
    h.exchange_name = row["exchange_name"].as<std::string>();
    if (!row["endpoint"].is_null())         h.endpoint = row["endpoint"].as<std::string>();
    h.status = row["status"].as<std::string>();
    if (!row["last_success_at"].is_null())  h.last_success_at = ts_fn(row["last_success_at"].as<std::string>());
    if (!row["last_error_at"].is_null())    h.last_error_at = ts_fn(row["last_error_at"].as<std::string>());
    if (!row["error_count"].is_null())      h.error_count = row["error_count"].as<int64_t>();
    if (!row["rate_limit_hits"].is_null())  h.rate_limit_hits = row["rate_limit_hits"].as<int64_t>();
    if (!row["latency_ms"].is_null())       h.latency_ms = row["latency_ms"].as<double>();
    if (!row["symbols_active"].is_null())   h.symbols_active = row["symbols_active"].as<int64_t>();
    if (!row["feed_lag_ms"].is_null())      h.feed_lag_ms = row["feed_lag_ms"].as<double>();
    h.checked_at = ts_fn(row["checked_at"].as<std::string>());
}

std::optional<ExchangeHealth> SAdapter::create_exchange_health(const ExchangeHealth& h) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        p.append(h.exchange_name); APPEND_OPT(p, h.endpoint); p.append(h.status);
        APPEND_OPT_TS(p, h.last_success_at); APPEND_OPT_TS(p, h.last_error_at);
        APPEND_OPT(p, h.error_count); APPEND_OPT(p, h.rate_limit_hits);
        APPEND_OPT(p, h.latency_ms); APPEND_OPT(p, h.symbols_active);
        APPEND_OPT(p, h.feed_lag_ms);
        auto r = txn.exec(
            "INSERT INTO exchange_health(exchange_name,endpoint,status,last_success_at,"
            "last_error_at,error_count,rate_limit_hits,latency_ms,symbols_active,feed_lag_ms)"
            " VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id,checked_at", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        ExchangeHealth out = h;
        out.id = r[0]["id"].as<int64_t>();
        out.checked_at = string_to_timestamp(r[0]["checked_at"].as<std::string>());
        return out;
    } catch (const std::exception& e) { std::cerr << "create_exchange_health: " << e.what() << "\n"; return std::nullopt; }
}

std::optional<ExchangeHealth> SAdapter::get_exchange_health_by_id(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("SELECT * FROM exchange_health WHERE id=$1", p);
        if (r.empty()) return std::nullopt;
        ExchangeHealth h;
        fill_exchange_health(h, r[0],
            [this](const std::string& s){ return string_to_timestamp(s); });
        return h;
    } catch (const std::exception& e) { std::cerr << "get_exchange_health_by_id: " << e.what() << "\n"; return std::nullopt; }
}

std::vector<ExchangeHealth> SAdapter::get_exchange_healths_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    std::vector<ExchangeHealth> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*conn_);
        std::string q = "SELECT * FROM exchange_health";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            ExchangeHealth h;
            fill_exchange_health(h, row,
                [this](const std::string& s){ return string_to_timestamp(s); });
            out.push_back(h);
        }
        txn.commit();
    } catch (const std::exception& e) { std::cerr << "get_exchange_healths: " << e.what() << "\n"; }
    return out;
}

bool SAdapter::update_exchange_health(const ExchangeHealth& h) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || h.id <= 0) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        p.append(h.exchange_name); APPEND_OPT(p, h.endpoint); p.append(h.status);
        APPEND_OPT_TS(p, h.last_success_at); APPEND_OPT_TS(p, h.last_error_at);
        APPEND_OPT(p, h.error_count); APPEND_OPT(p, h.rate_limit_hits);
        APPEND_OPT(p, h.latency_ms); APPEND_OPT(p, h.symbols_active);
        APPEND_OPT(p, h.feed_lag_ms);
        p.append(h.id);
        auto r = txn.exec(
            "UPDATE exchange_health SET exchange_name=$1,endpoint=$2,status=$3,last_success_at=$4,"
            "last_error_at=$5,error_count=$6,rate_limit_hits=$7,latency_ms=$8,symbols_active=$9,"
            "feed_lag_ms=$10 WHERE id=$11", p);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "update_exchange_health: " << e.what() << "\n"; return false; }
}

bool SAdapter::delete_exchange_health(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || id <= 0) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("DELETE FROM exchange_health WHERE id=$1", p);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "delete_exchange_health: " << e.what() << "\n"; return false; }
}

// ─── BacktestJob operations ──────────────────────────────────────────────────

static void fill_backtest_job(BacktestJob& j, const pqxx::row& row,
                               std::function<uint64_t(const std::string&)> ts_fn) {
    j.job_id = row["job_id"].as<std::string>();
    if (!row["symbol"].is_null())       j.symbol = row["symbol"].as<std::string>();
    if (!row["exchange"].is_null())     j.exchange = row["exchange"].as<std::string>();
    if (!row["start_time"].is_null())   j.start_time = ts_fn(row["start_time"].as<std::string>());
    if (!row["end_time"].is_null())     j.end_time = ts_fn(row["end_time"].as<std::string>());
    if (!row["replay_speed"].is_null()) j.replay_speed = row["replay_speed"].as<int64_t>();
    j.status = row["status"].as<std::string>();
    if (!row["progress"].is_null())     j.progress = row["progress"].as<double>();
    if (!row["created_at"].is_null())   j.created_at = ts_fn(row["created_at"].as<std::string>());
    if (!row["completed_at"].is_null()) j.completed_at = ts_fn(row["completed_at"].as<std::string>());
}

std::optional<BacktestJob> SAdapter::create_backtest_job(const BacktestJob& j) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        APPEND_OPT(p, j.symbol); APPEND_OPT(p, j.exchange);
        APPEND_OPT_TS(p, j.start_time); APPEND_OPT_TS(p, j.end_time);
        APPEND_OPT(p, j.replay_speed); p.append(j.status);
        APPEND_OPT(p, j.progress);
        auto r = txn.exec(
            "INSERT INTO backtest_jobs(symbol,exchange,start_time,end_time,replay_speed,status,progress)"
            " VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING job_id,created_at", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        BacktestJob out = j;
        out.job_id = r[0]["job_id"].as<std::string>();
        out.created_at = string_to_timestamp(r[0]["created_at"].as<std::string>());
        return out;
    } catch (const std::exception& e) { std::cerr << "create_backtest_job: " << e.what() << "\n"; return std::nullopt; }
}

std::optional<BacktestJob> SAdapter::get_backtest_job_by_id(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("SELECT * FROM backtest_jobs WHERE job_id=$1", p);
        if (r.empty()) return std::nullopt;
        BacktestJob j;
        fill_backtest_job(j, r[0],
            [this](const std::string& s){ return string_to_timestamp(s); });
        return j;
    } catch (const std::exception& e) { std::cerr << "get_backtest_job_by_id: " << e.what() << "\n"; return std::nullopt; }
}

std::vector<BacktestJob> SAdapter::get_backtest_jobs_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    std::vector<BacktestJob> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*conn_);
        std::string q = "SELECT * FROM backtest_jobs";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            BacktestJob j;
            fill_backtest_job(j, row,
                [this](const std::string& s){ return string_to_timestamp(s); });
            out.push_back(j);
        }
        txn.commit();
    } catch (const std::exception& e) { std::cerr << "get_backtest_jobs: " << e.what() << "\n"; }
    return out;
}

bool SAdapter::update_backtest_job(const BacktestJob& j) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || j.job_id.empty()) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        APPEND_OPT(p, j.symbol); APPEND_OPT(p, j.exchange);
        APPEND_OPT_TS(p, j.start_time); APPEND_OPT_TS(p, j.end_time);
        APPEND_OPT(p, j.replay_speed); p.append(j.status);
        APPEND_OPT(p, j.progress);
        p.append(j.job_id);
        auto r = txn.exec(
            "UPDATE backtest_jobs SET symbol=$1,exchange=$2,start_time=$3,end_time=$4,"
            "replay_speed=$5,status=$6,progress=$7 WHERE job_id=$8", p);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "update_backtest_job: " << e.what() << "\n"; return false; }
}

bool SAdapter::delete_backtest_job(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || id.empty()) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("DELETE FROM backtest_jobs WHERE job_id=$1", p);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "delete_backtest_job: " << e.what() << "\n"; return false; }
}

// ─── ConfigVersion operations ────────────────────────────────────────────────

static void fill_config_version(ConfigVersion& cv, const pqxx::row& row,
                                 std::function<uint64_t(const std::string&)> ts_fn) {
    cv.id = row["id"].as<int64_t>();
    cv.config_version = row["config_version"].as<std::string>();
    cv.build_sha = row["build_sha"].as<std::string>();
    cv.adapter_version = row["adapter_version"].as<std::string>();
    cv.deployment_id = row["deployment_id"].as<std::string>();
    if (!row["feature_flags"].is_null()) cv.feature_flags = row["feature_flags"].as<std::string>();
    cv.schema_version = row["schema_version"].as<int64_t>();
    cv.applied_at = ts_fn(row["applied_at"].as<std::string>());
}

std::optional<ConfigVersion> SAdapter::create_config_version(const ConfigVersion& cv) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        p.append(cv.config_version); p.append(cv.build_sha);
        p.append(cv.adapter_version); p.append(cv.deployment_id);
        APPEND_OPT(p, cv.feature_flags); p.append(cv.schema_version);
        auto r = txn.exec(
            "INSERT INTO config_versions(config_version,build_sha,adapter_version,deployment_id,"
            "feature_flags,schema_version) VALUES($1,$2,$3,$4,$5,$6) RETURNING id,applied_at", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        ConfigVersion out = cv;
        out.id = r[0]["id"].as<int64_t>();
        out.applied_at = string_to_timestamp(r[0]["applied_at"].as<std::string>());
        return out;
    } catch (const std::exception& e) { std::cerr << "create_config_version: " << e.what() << "\n"; return std::nullopt; }
}

std::optional<ConfigVersion> SAdapter::get_config_version_by_id(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("SELECT * FROM config_versions WHERE id=$1", p);
        if (r.empty()) return std::nullopt;
        ConfigVersion cv;
        fill_config_version(cv, r[0],
            [this](const std::string& s){ return string_to_timestamp(s); });
        return cv;
    } catch (const std::exception& e) { std::cerr << "get_config_version_by_id: " << e.what() << "\n"; return std::nullopt; }
}

std::vector<ConfigVersion> SAdapter::get_config_versions_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    std::vector<ConfigVersion> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*conn_);
        std::string q = "SELECT * FROM config_versions";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            ConfigVersion cv;
            fill_config_version(cv, row,
                [this](const std::string& s){ return string_to_timestamp(s); });
            out.push_back(cv);
        }
        txn.commit();
    } catch (const std::exception& e) { std::cerr << "get_config_versions: " << e.what() << "\n"; }
    return out;
}

bool SAdapter::update_config_version(const ConfigVersion& cv) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || cv.id <= 0) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p;
        p.append(cv.config_version); p.append(cv.build_sha);
        p.append(cv.adapter_version); p.append(cv.deployment_id);
        APPEND_OPT(p, cv.feature_flags); p.append(cv.schema_version);
        p.append(cv.id);
        auto r = txn.exec(
            "UPDATE config_versions SET config_version=$1,build_sha=$2,adapter_version=$3,"
            "deployment_id=$4,feature_flags=$5,schema_version=$6 WHERE id=$7", p);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "update_config_version: " << e.what() << "\n"; return false; }
}

bool SAdapter::delete_config_version(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (!is_connected() || id <= 0) return false;
    try {
        pqxx::work txn(*conn_);
        pqxx::params p; p.append(id);
        auto r = txn.exec("DELETE FROM config_versions WHERE id=$1", p);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "delete_config_version: " << e.what() << "\n"; return false; }
}

} // namespace datafeed