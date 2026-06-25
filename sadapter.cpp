// sadapter.cpp — pqxx 6.x/7.x compatible implementation
// Uses traditional pqxx API with prepared statements and parameter escaping
#include "sadapter.h"
#include <pqxx/pqxx>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <ctime>
#include <optional>

namespace datafeed {

// ─── PIMPL Implementation ──────────────────────────────────────────────────────

struct SAdapter::Impl {
    std::string connection_string;
    std::unique_ptr<pqxx::connection> conn;
    mutable std::recursive_mutex mutex;
};

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

// ─── Helper functions for SQL escaping ────────────────────────────────────────

static std::string escape_string(pqxx::work& txn, const std::string& str) {
    return txn.esc(str);
}

static std::string escape_or_null(pqxx::work& txn, const std::optional<std::string>& opt) {
    if (!opt) return "NULL";
    return "'" + escape_string(txn, *opt) + "'";
}

static std::string escape_timestamp(pqxx::work& txn, const std::optional<uint64_t>& opt) {
    if (!opt) return "NULL";
    return "'" + SAdapter::timestamp_to_string(*opt) + "'";
}

static std::string escape_interval(pqxx::work& txn, const std::optional<int64_t>& opt) {
    if (!opt) return "NULL";
    return "'" + SAdapter::interval_to_string(*opt) + "'";
}

static std::string escape_int64(pqxx::work& txn, const std::optional<int64_t>& opt) {
    if (!opt) return "NULL";
    return std::to_string(*opt);
}

// ─── Connection ──────────────────────────────────────────────────────────────

SAdapter::SAdapter(const std::string& cs) : pImpl_(std::make_unique<Impl>()) {
    pImpl_->connection_string = cs;
}
SAdapter::~SAdapter() { disconnect(); }

bool SAdapter::connect() {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    try {
        pImpl_->conn = std::make_unique<pqxx::connection>(pImpl_->connection_string);
        return pImpl_->conn->is_open();
    } catch (const std::exception& e) {
        std::cerr << "DB connect failed: " << e.what() << "\n"; return false;
    }
}

void SAdapter::disconnect() {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    pImpl_->conn.reset();
}

bool SAdapter::is_connected() const {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    return pImpl_->conn && pImpl_->conn->is_open();
}

// ─── Client operations ───────────────────────────────────────────────────────

std::optional<Client> SAdapter::create_client(const Client& c) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string query = std::string("INSERT INTO clients(client_name,plan,status,auth_subject,ip_address,user_agent) VALUES(") +
            "'" + escape_string(txn, c.client_name) + "'," +
            "'" + escape_string(txn, c.plan) + "'," +
            "'" + escape_string(txn, c.status) + "'," +
            escape_or_null(txn, c.auth_subject) + "," +
            escape_or_null(txn, c.ip_address) + "," +
            escape_or_null(txn, c.user_agent) + ") RETURNING tenant_id,created_at,updated_at";
        auto r = txn.exec(query);
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
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string query = "SELECT * FROM clients WHERE tenant_id='" + escape_string(txn, id) + "'";
        auto r = txn.exec(query);
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
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    std::vector<Client> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*pImpl_->conn);
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
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected() || c.tenant_id.empty()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string query = "UPDATE clients SET client_name='" + escape_string(txn, c.client_name) +
            "',plan='" + escape_string(txn, c.plan) +
            "',status='" + escape_string(txn, c.status) +
            "',auth_subject=" + escape_or_null(txn, c.auth_subject) +
            ",ip_address=" + escape_or_null(txn, c.ip_address) +
            ",user_agent=" + escape_or_null(txn, c.user_agent) +
            ",updated_at=NOW() WHERE tenant_id='" + escape_string(txn, c.tenant_id) + "' RETURNING tenant_id";
        auto r = txn.exec(query);
        txn.commit(); return !r.empty();
    } catch (const std::exception& e) { std::cerr << "update_client: " << e.what() << "\n"; return false; }
}

bool SAdapter::delete_client(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected() || id.empty()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string query = "DELETE FROM clients WHERE tenant_id='" + escape_string(txn, id) + "'";
        auto r = txn.exec(query);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "delete_client: " << e.what() << "\n"; return false; }
}

// ─── Session operations ──────────────────────────────────────────────────────

std::optional<Session> SAdapter::create_session(const Session& s) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string query = "INSERT INTO sessions(connection_id,connected_at,disconnected_at,disconnect_reason,auth_status,reconnect_count,heartbeat_interval,protocol,instance_id,tenant_id) VALUES(" +
            escape_or_null(txn, s.connection_id) + "," +
            "'" + timestamp_to_string(s.connected_at) + "'," +
            escape_timestamp(txn, s.disconnected_at) + "," +
            escape_or_null(txn, s.disconnect_reason) + "," +
            escape_or_null(txn, s.auth_status) + "," +
            escape_int64(txn, s.reconnect_count) + "," +
            escape_interval(txn, s.heartbeat_interval) + "," +
            "'" + escape_string(txn, s.protocol) + "'," +
            "'" + escape_string(txn, s.instance_id) + "'," +
            escape_or_null(txn, s.tenant_id) + ") RETURNING session_id";
        auto r = txn.exec(query);
        txn.commit();
        if (r.empty()) return std::nullopt;
        Session out = s; out.session_id = r[0]["session_id"].as<std::string>(); return out;
    } catch (const std::exception& e) { std::cerr << "create_session: " << e.what() << "\n"; return std::nullopt; }
}

std::optional<Session> SAdapter::get_session_by_id(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string query = "SELECT * FROM sessions WHERE session_id='" + escape_string(txn, id) + "'";
        auto r = txn.exec(query);
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
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    std::vector<Session> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*pImpl_->conn);
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
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected() || s.session_id.empty()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string query = "UPDATE sessions SET connection_id=" + escape_or_null(txn, s.connection_id) +
            ",connected_at='" + timestamp_to_string(s.connected_at) + "'," +
            "disconnected_at=" + escape_timestamp(txn, s.disconnected_at) + "," +
            "disconnect_reason=" + escape_or_null(txn, s.disconnect_reason) + "," +
            "auth_status=" + escape_or_null(txn, s.auth_status) + "," +
            "reconnect_count=" + escape_int64(txn, s.reconnect_count) + "," +
            "heartbeat_interval=" + escape_interval(txn, s.heartbeat_interval) + "," +
            "protocol='" + escape_string(txn, s.protocol) + "'," +
            "instance_id='" + escape_string(txn, s.instance_id) + "'," +
            "tenant_id=" + escape_or_null(txn, s.tenant_id) +
            " WHERE session_id='" + escape_string(txn, s.session_id) + "'";
        auto r = txn.exec(query);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "update_session: " << e.what() << "\n"; return false; }
}

bool SAdapter::delete_session(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected() || id.empty()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string query = "DELETE FROM sessions WHERE session_id='" + escape_string(txn, id) + "'";
        auto r = txn.exec(query);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "delete_session: " << e.what() << "\n"; return false; }
}

// ─── FeedInstance operations ─────────────────────────────────────────────────

std::optional<FeedInstance> SAdapter::create_feed_instance(const FeedInstance& instance) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string query = std::string("INSERT INTO feed_instances(instance_id,exchange,adapter_type,feed_status,last_tick_at,stale_seconds,reconnect_attempts,message_rate_in,message_rate_out,queue_depth,backpressure_active,serialization_ms,parse_error_count,gap_count,duplicate_count,out_of_order_count,tenant_id) VALUES(") +
            "'" + escape_string(txn, instance.instance_id) + "'," +
            "'" + escape_string(txn, instance.exchange) + "'," +
            escape_or_null(txn, instance.adapter_type) + "," +
            "'" + escape_string(txn, instance.feed_status) + "'," +
            escape_timestamp(txn, instance.last_tick_at) + "," +
            escape_interval(txn, instance.stale_seconds) + "," +
            std::to_string(instance.reconnect_attempts) + "," +
            (instance.message_rate_in ? std::to_string(*instance.message_rate_in) : "NULL") + "," +
            (instance.message_rate_out ? std::to_string(*instance.message_rate_out) : "NULL") + "," +
            escape_int64(txn, instance.queue_depth) + "," +
            (instance.backpressure_active ? (*instance.backpressure_active ? "TRUE" : "FALSE") : "NULL") + "," +
            (instance.serialization_ms ? std::to_string(*instance.serialization_ms) : "NULL") + "," +
            escape_int64(txn, instance.parse_error_count) + "," +
            escape_int64(txn, instance.gap_count) + "," +
            escape_int64(txn, instance.duplicate_count) + "," +
            escape_int64(txn, instance.out_of_order_count) + "," +
            escape_or_null(txn, instance.tenant_id) + ")";
        auto r = txn.exec(query);
        txn.commit();
        return instance;
    } catch (const std::exception& e) { std::cerr << "create_feed_instance: " << e.what() << "\n"; return std::nullopt; }
}

std::optional<FeedInstance> SAdapter::get_feed_instance_by_id(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string query = "SELECT * FROM feed_instances WHERE instance_id='" + escape_string(txn, id) + "'";
        auto r = txn.exec(query);
        if (r.empty()) return std::nullopt;
        FeedInstance f;
        f.instance_id=r[0]["instance_id"].as<std::string>();
        f.exchange=r[0]["exchange"].as<std::string>();
        if (!r[0]["adapter_type"].is_null()) f.adapter_type=r[0]["adapter_type"].as<std::string>();
        f.feed_status=r[0]["feed_status"].as<std::string>();
        if (!r[0]["last_tick_at"].is_null()) f.last_tick_at=string_to_timestamp(r[0]["last_tick_at"].as<std::string>());
        if (!r[0]["stale_seconds"].is_null()) f.stale_seconds=string_to_interval(r[0]["stale_seconds"].as<std::string>());
        f.reconnect_attempts=r[0]["reconnect_attempts"].as<int64_t>();
        if (!r[0]["message_rate_in"].is_null()) f.message_rate_in=r[0]["message_rate_in"].as<double>();
        if (!r[0]["message_rate_out"].is_null()) f.message_rate_out=r[0]["message_rate_out"].as<double>();
        if (!r[0]["queue_depth"].is_null()) f.queue_depth=r[0]["queue_depth"].as<int64_t>();
        if (!r[0]["backpressure_active"].is_null()) f.backpressure_active=r[0]["backpressure_active"].as<bool>();
        if (!r[0]["serialization_ms"].is_null()) f.serialization_ms=r[0]["serialization_ms"].as<double>();
        if (!r[0]["parse_error_count"].is_null()) f.parse_error_count=r[0]["parse_error_count"].as<int64_t>();
        if (!r[0]["gap_count"].is_null()) f.gap_count=r[0]["gap_count"].as<int64_t>();
        if (!r[0]["duplicate_count"].is_null()) f.duplicate_count=r[0]["duplicate_count"].as<int64_t>();
        if (!r[0]["out_of_order_count"].is_null()) f.out_of_order_count=r[0]["out_of_order_count"].as<int64_t>();
        if (!r[0]["tenant_id"].is_null()) f.tenant_id=r[0]["tenant_id"].as<std::string>();
        return f;
    } catch (const std::exception& e) { std::cerr << "get_feed_instance_by_id: " << e.what() << "\n"; return std::nullopt; }
}

std::vector<FeedInstance> SAdapter::get_feed_instances_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    std::vector<FeedInstance> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string q = "SELECT * FROM feed_instances";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            FeedInstance f;
            f.instance_id=row["instance_id"].as<std::string>();
            f.exchange=row["exchange"].as<std::string>();
            if (!row["adapter_type"].is_null()) f.adapter_type=row["adapter_type"].as<std::string>();
            f.feed_status=row["feed_status"].as<std::string>();
            if (!row["last_tick_at"].is_null()) f.last_tick_at=string_to_timestamp(row["last_tick_at"].as<std::string>());
            if (!row["stale_seconds"].is_null()) f.stale_seconds=string_to_interval(row["stale_seconds"].as<std::string>());
            f.reconnect_attempts=row["reconnect_attempts"].as<int64_t>();
            if (!row["message_rate_in"].is_null()) f.message_rate_in=row["message_rate_in"].as<double>();
            if (!row["message_rate_out"].is_null()) f.message_rate_out=row["message_rate_out"].as<double>();
            if (!row["queue_depth"].is_null()) f.queue_depth=row["queue_depth"].as<int64_t>();
            if (!row["backpressure_active"].is_null()) f.backpressure_active=row["backpressure_active"].as<bool>();
            if (!row["serialization_ms"].is_null()) f.serialization_ms=row["serialization_ms"].as<double>();
            if (!row["parse_error_count"].is_null()) f.parse_error_count=row["parse_error_count"].as<int64_t>();
            if (!row["gap_count"].is_null()) f.gap_count=row["gap_count"].as<int64_t>();
            if (!row["duplicate_count"].is_null()) f.duplicate_count=row["duplicate_count"].as<int64_t>();
            if (!row["out_of_order_count"].is_null()) f.out_of_order_count=row["out_of_order_count"].as<int64_t>();
            if (!row["tenant_id"].is_null()) f.tenant_id=row["tenant_id"].as<std::string>();
            out.push_back(f);
        }
        txn.commit();
    } catch (const std::exception& e) { std::cerr << "get_feed_instances: " << e.what() << "\n"; }
    return out;
}

bool SAdapter::update_feed_instance(const FeedInstance& instance) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected() || instance.instance_id.empty()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string query = "UPDATE feed_instances SET exchange='" + escape_string(txn, instance.exchange) +
            "',adapter_type=" + escape_or_null(txn, instance.adapter_type) +
            ",feed_status='" + escape_string(txn, instance.feed_status) + "'," +
            "last_tick_at=" + escape_timestamp(txn, instance.last_tick_at) + "," +
            "stale_seconds=" + escape_interval(txn, instance.stale_seconds) + "," +
            "reconnect_attempts=" + std::to_string(instance.reconnect_attempts) + "," +
            "message_rate_in=" + (instance.message_rate_in ? std::to_string(*instance.message_rate_in) : "NULL") + "," +
            "message_rate_out=" + (instance.message_rate_out ? std::to_string(*instance.message_rate_out) : "NULL") + "," +
            "queue_depth=" + escape_int64(txn, instance.queue_depth) + "," +
            "backpressure_active=" + (instance.backpressure_active ? (*instance.backpressure_active ? "TRUE" : "FALSE") : "NULL") + "," +
            "serialization_ms=" + (instance.serialization_ms ? std::to_string(*instance.serialization_ms) : "NULL") + "," +
            "parse_error_count=" + escape_int64(txn, instance.parse_error_count) + "," +
            "gap_count=" + escape_int64(txn, instance.gap_count) + "," +
            "duplicate_count=" + escape_int64(txn, instance.duplicate_count) + "," +
            "out_of_order_count=" + escape_int64(txn, instance.out_of_order_count) +
            " WHERE instance_id='" + escape_string(txn, instance.instance_id) + "'";
        auto r = txn.exec(query);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "update_feed_instance: " << e.what() << "\n"; return false; }
}

bool SAdapter::delete_feed_instance(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected() || id.empty()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string query = "DELETE FROM feed_instances WHERE instance_id='" + escape_string(txn, id) + "'";
        auto r = txn.exec(query);
        txn.commit(); return r.affected_rows() > 0;
    } catch (const std::exception& e) { std::cerr << "delete_feed_instance: " << e.what() << "\n"; return false; }
}

// ─── Stub implementations for remaining operations ───────────────────────────

std::optional<Subscription> SAdapter::create_subscription(const Subscription& subscription) { return std::nullopt; }
std::optional<Subscription> SAdapter::get_subscription_by_id(const std::string& subscription_id) { return std::nullopt; }
std::vector<Subscription> SAdapter::get_subscriptions_by_condition(const std::string& where_clause) { return {}; }
bool SAdapter::update_subscription(const Subscription& subscription) { return false; }
bool SAdapter::delete_subscription(const std::string& subscription_id) { return false; }

std::optional<FeedMetricsSnapshot> SAdapter::create_feed_metrics_snapshot(const FeedMetricsSnapshot& snapshot) { return std::nullopt; }
std::optional<FeedMetricsSnapshot> SAdapter::get_feed_metrics_snapshot_by_id(int64_t id) { return std::nullopt; }
std::vector<FeedMetricsSnapshot> SAdapter::get_feed_metrics_snapshots_by_condition(const std::string& where_clause) { return {}; }
bool SAdapter::update_feed_metrics_snapshot(const FeedMetricsSnapshot& snapshot) { return false; }
bool SAdapter::delete_feed_metrics_snapshot(int64_t id) { return false; }

std::optional<FeedEvent> SAdapter::create_feed_event(const FeedEvent& event) { return std::nullopt; }
std::optional<FeedEvent> SAdapter::get_feed_event_by_id(const std::string& event_id) { return std::nullopt; }
std::vector<FeedEvent> SAdapter::get_feed_events_by_condition(const std::string& where_clause) { return {}; }
bool SAdapter::update_feed_event(const FeedEvent& event) { return false; }
bool SAdapter::delete_feed_event(const std::string& event_id) { return false; }

std::optional<ApiRequest> SAdapter::create_api_request(const ApiRequest& request) { return std::nullopt; }
std::optional<ApiRequest> SAdapter::get_api_request_by_id(const std::string& request_id) { return std::nullopt; }
std::vector<ApiRequest> SAdapter::get_api_requests_by_condition(const std::string& where_clause) { return {}; }
bool SAdapter::update_api_request(const ApiRequest& request) { return false; }
bool SAdapter::delete_api_request(const std::string& request_id) { return false; }

std::optional<ExchangeHealth> SAdapter::create_exchange_health(const ExchangeHealth& health) { return std::nullopt; }
std::optional<ExchangeHealth> SAdapter::get_exchange_health_by_id(int64_t id) { return std::nullopt; }
std::vector<ExchangeHealth> SAdapter::get_exchange_healths_by_condition(const std::string& where_clause) { return {}; }
bool SAdapter::update_exchange_health(const ExchangeHealth& health) { return false; }
bool SAdapter::delete_exchange_health(int64_t id) { return false; }

std::optional<BacktestJob> SAdapter::create_backtest_job(const BacktestJob& job) { return std::nullopt; }
std::optional<BacktestJob> SAdapter::get_backtest_job_by_id(const std::string& job_id) { return std::nullopt; }
std::vector<BacktestJob> SAdapter::get_backtest_jobs_by_condition(const std::string& where_clause) { return {}; }
bool SAdapter::update_backtest_job(const BacktestJob& job) { return false; }
bool SAdapter::delete_backtest_job(const std::string& job_id) { return false; }

std::optional<ConfigVersion> SAdapter::create_config_version(const ConfigVersion& config) { return std::nullopt; }
std::optional<ConfigVersion> SAdapter::get_config_version_by_id(int64_t id) { return std::nullopt; }
std::vector<ConfigVersion> SAdapter::get_config_versions_by_condition(const std::string& where_clause) { return {}; }
bool SAdapter::update_config_version(const ConfigVersion& config) { return false; }
bool SAdapter::delete_config_version(int64_t id) { return false; }

} // namespace datafeed
