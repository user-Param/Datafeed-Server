// sadapter.cpp — pqxx 6.x/7.x compatible implementation
// Uses prepared statements for all monitoring persistence operations.
#include "sadapter.h"
#include <pqxx/pqxx>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <ctime>
#include <optional>
#include <type_traits>

namespace datafeed {

// ─── PIMPL Implementation ──────────────────────────────────────────────────────

struct SAdapter::Impl {
    std::string connection_string;
    std::unique_ptr<pqxx::connection> conn;
    mutable std::recursive_mutex mutex;

    bool prepare_monitoring_statements();
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

// ─── Helper functions for SQL escaping (legacy operations) ────────────────────

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

// ─── Prepared statement helpers for monitoring operations ─────────────────────

// Append an optional double to a pqxx::params array.
// In libpqxx 7, optionals map to SQL NULL when empty.
static void append_opt(pqxx::params& p, const std::optional<double>& v) {
    if (v) p.append(*v); else p.append(std::nullopt);
}
static void append_opt(pqxx::params& p, const std::optional<int64_t>& v) {
    if (v) p.append(*v); else p.append(std::nullopt);
}
static void append_opt(pqxx::params& p, const std::optional<std::string>& v) {
    if (v) p.append(*v); else p.append(std::nullopt);
}
static void append_opt_bool(pqxx::params& p, const std::optional<bool>& v) {
    if (v) p.append(*v ? "true" : "false"); else p.append(std::nullopt);
}
static void append_bool(pqxx::params& p, bool v) {
    p.append(v ? "true" : "false");
}

// ─── Prepare monitoring statements ─────────────────────────────────────────

bool SAdapter::Impl::prepare_monitoring_statements() {
    try {
        // ---- feed_metrics_snapshots ----
        conn->prepare("insert_snapshot", R"(
            INSERT INTO feed_metrics_snapshots (
                instance_id, measured_at,
                p50_latency_ms, p95_latency_ms, p99_latency_ms, avg_latency_ms,
                drop_rate, packet_loss_rate,
                msgs_sent, msgs_received, bytes_sent, bytes_received,
                cpu_usage, memory_usage, thread_count, event_loop_lag_ms, uptime_seconds,
                exchange_p50_ms, exchange_p95_ms, exchange_p99_ms,
                parsing_p50_ms, parsing_p95_ms, parsing_p99_ms,
                normalization_p50_ms, normalization_p95_ms, normalization_p99_ms,
                processing_p50_ms, processing_p95_ms, processing_p99_ms,
                broadcast_p50_ms, broadcast_p95_ms, broadcast_p99_ms,
                serialization_p50_ms, serialization_p95_ms, serialization_p99_ms,
                socket_send_p50_ms, socket_send_p95_ms, socket_send_p99_ms,
                latency_stats_jsonb,
                messages_per_sec, packets_per_sec, bytes_per_sec,
                ticks_per_sec, trades_per_sec, orderbook_updates_per_sec,
                broadcasts_per_sec, subscriptions_per_sec,
                database_writes_per_sec, database_reads_per_sec,
                total_messages, total_packets, total_bytes,
                total_ticks, total_trades, total_orderbook_updates, total_broadcasts,
                incoming_queue_depth, outgoing_queue_depth, serialization_queue_depth,
                max_incoming_queue_depth, max_outgoing_queue_depth, max_serialization_queue_depth,
                queue_overflow_count, queue_wait_time_ms, queue_processing_time_ms, queue_backpressure,
                packet_drops, duplicate_packets, out_of_order_packets,
                sequence_gaps, missing_ticks, invalid_messages, corrupted_packets, parse_failures,
                stale_feed, feed_health_score, feed_health_status,
                active_clients, active_sessions, active_subscriptions,
                total_connections, total_disconnections, reconnect_count, authentication_failures,
                avg_session_duration_ms, longest_session_duration_ms,
                tcp_reconnects, socket_errors, read_errors, write_errors, tls_handshake_failures,
                network_bytes_transmitted, network_bytes_received,
                socket_rtt_ms, network_bandwidth_bps, network_connection_failures,
                db_successful_writes, db_failed_writes, db_insert_latency_ms, db_query_latency_ms,
                db_active_connections, db_connection_failures, db_transaction_count,
                db_writes_per_sec, db_reads_per_sec, db_queue_waiting,
                peak_rss, virtual_memory, heap_usage, memory_growth_rate,
                total_subscriptions, total_database_writes, total_database_reads
            ) VALUES (
                $1,$2, $3,$4,$5,$6, $7,$8, $9,$10,$11,$12,
                $13,$14,$15,$16,$17,
                $18,$19,$20, $21,$22,$23, $24,$25,$26,
                $27,$28,$29, $30,$31,$32, $33,$34,$35,
                $36,$37,$38,
                $39,$40,$41, $42,$43,$44, $45,$46,$47, $48,$49,
                $50,$51,$52, $53,$54,$55,$56,$57, $58,$59,
                $60,$61,$62, $63,$64,$65, $66,$67,$68,$69,
                $70,$71,$72, $73,$74,$75,$76,$77,
                $78,$79,$80,
                $81,$82,$83, $84,$85,$86,$87,
                $88,$89,
                $90,$91,$92,$93,$94, $95,$96, $97,$98,$99,
                $100,$101,$102,$103, $104,$105,$106,
                $107,$108,$109,$110,
                $111,$112,$113,$114,
                $115,$116,$117
            ) RETURNING id
        )");

        // ---- exchange_metrics_history ----
        conn->prepare("insert_exchange_metrics", R"(
            INSERT INTO exchange_metrics_history (
                instance_id, exchange_name, snapshot_time, connected,
                uptime_seconds, reconnect_count, heartbeat_failures,
                websocket_disconnects, messages_received, messages_dropped,
                parse_errors, feed_lag_ms, exchange_latency_ms, stale
            ) VALUES ($1,$2,$3,$4, $5,$6,$7, $8,$9,$10, $11,$12,$13,$14)
            RETURNING id
        )");

        // ---- queue_history ----
        conn->prepare("insert_queue_entry", R"(
            INSERT INTO queue_history (
                instance_id, measured_at,
                incoming_depth, outgoing_depth, serialization_depth,
                max_incoming_depth, max_outgoing_depth, max_serialization_depth,
                overflow_count, backpressure, wait_time_ms, processing_time_ms
            ) VALUES ($1,$2, $3,$4,$5, $6,$7,$8, $9,$10,$11,$12)
            RETURNING id
        )");

        // ---- system_metrics_history ----
        conn->prepare("insert_system_metrics", R"(
            INSERT INTO system_metrics_history (
                instance_id, measured_at,
                cpu_usage_percent, memory_rss, peak_rss, virtual_memory,
                heap_usage, memory_growth_rate, thread_count, uptime_seconds
            ) VALUES ($1,$2, $3,$4,$5,$6, $7,$8,$9,$10)
            RETURNING id
        )");

        // ---- network_metrics_history ----
        conn->prepare("insert_network_metrics", R"(
            INSERT INTO network_metrics_history (
                instance_id, measured_at,
                tcp_reconnects, socket_errors, read_errors, write_errors,
                tls_handshake_failures, bytes_transmitted, bytes_received,
                socket_rtt_ms, bandwidth_bps, connection_failures
            ) VALUES ($1,$2, $3,$4,$5,$6, $7,$8,$9,$10,$11,$12)
            RETURNING id
        )");

        // ---- database_metrics_history ----
        conn->prepare("insert_database_metrics", R"(
            INSERT INTO database_metrics_history (
                instance_id, measured_at,
                successful_writes, failed_writes, insert_latency_ms, query_latency_ms,
                active_connections, connection_failures, transaction_count,
                writes_per_sec, reads_per_sec, queue_waiting
            ) VALUES ($1,$2, $3,$4,$5,$6, $7,$8,$9, $10,$11,$12)
            RETURNING id
        )");

        // ---- alerts ----
        conn->prepare("insert_alert", R"(
            INSERT INTO alerts (
                instance_id, severity, source, metric_name,
                current_value, threshold, message, acknowledged,
                created_at, resolved_at
            ) VALUES ($1,$2,$3,$4, $5,$6,$7,$8, $9,$10)
            RETURNING alert_id
        )");

        conn->prepare("get_alert_by_id", R"(
            SELECT * FROM alerts WHERE alert_id = $1
        )");

        conn->prepare("get_alerts_by_condition", R"(
            SELECT * FROM alerts
        )");  // WHERE clause appended dynamically

        conn->prepare("update_alert", R"(
            UPDATE alerts SET
                instance_id = $1, severity = $2, source = $3,
                metric_name = $4, current_value = $5, threshold = $6,
                message = $7, acknowledged = $8,
                resolved_at = $9
            WHERE alert_id = $10
        )");

        conn->prepare("delete_alert", R"(
            DELETE FROM alerts WHERE alert_id = $1
        )");

        // ---- metric_thresholds ----
        conn->prepare("insert_metric_threshold", R"(
            INSERT INTO metric_thresholds (
                instance_id, metric_name, source,
                warning_threshold, critical_threshold, operator,
                enabled, cooldown_seconds, created_at, updated_at
            ) VALUES ($1,$2,$3, $4,$5,$6, $7,$8, NOW(), NOW())
            RETURNING id
        )");

        conn->prepare("get_metric_threshold_by_id", R"(
            SELECT * FROM metric_thresholds WHERE id = $1
        )");

        conn->prepare("update_metric_threshold", R"(
            UPDATE metric_thresholds SET
                instance_id = $1, metric_name = $2, source = $3,
                warning_threshold = $4, critical_threshold = $5,
                operator = $6, enabled = $7, cooldown_seconds = $8,
                updated_at = NOW()
            WHERE id = $9
        )");

        conn->prepare("delete_metric_threshold", R"(
            DELETE FROM metric_thresholds WHERE id = $1
        )");

        // ---- weekly_metrics_summary ----
        conn->prepare("insert_weekly_summary", R"(
            INSERT INTO weekly_metrics_summary (
                instance_id, week_start, sample_count,
                p50_latency_ms, p95_latency_ms, p99_latency_ms, avg_latency_ms,
                drop_rate, packet_loss_rate,
                msgs_sent, msgs_received, bytes_sent, bytes_received,
                cpu_usage, memory_usage, thread_count, event_loop_lag_ms, uptime_seconds,
                exchange_p50_ms, exchange_p95_ms, exchange_p99_ms,
                parsing_p50_ms, parsing_p95_ms, parsing_p99_ms,
                normalization_p50_ms, normalization_p95_ms, normalization_p99_ms,
                processing_p50_ms, processing_p95_ms, processing_p99_ms,
                broadcast_p50_ms, broadcast_p95_ms, broadcast_p99_ms,
                serialization_p50_ms, serialization_p95_ms, serialization_p99_ms,
                socket_send_p50_ms, socket_send_p95_ms, socket_send_p99_ms,
                latency_stats_jsonb,
                messages_per_sec, packets_per_sec, bytes_per_sec,
                ticks_per_sec, trades_per_sec, orderbook_updates_per_sec,
                broadcasts_per_sec, subscriptions_per_sec,
                database_writes_per_sec, database_reads_per_sec,
                total_messages, total_packets, total_bytes,
                total_ticks, total_trades, total_orderbook_updates, total_broadcasts,
                incoming_queue_depth, outgoing_queue_depth, serialization_queue_depth,
                max_incoming_queue_depth, max_outgoing_queue_depth, max_serialization_queue_depth,
                queue_overflow_count, queue_wait_time_ms, queue_processing_time_ms, queue_backpressure,
                packet_drops, duplicate_packets, out_of_order_packets,
                sequence_gaps, missing_ticks, invalid_messages, corrupted_packets, parse_failures,
                stale_feed, feed_health_score, feed_health_status,
                active_clients, active_sessions, active_subscriptions,
                total_connections, total_disconnections, reconnect_count, authentication_failures,
                avg_session_duration_ms, longest_session_duration_ms,
                tcp_reconnects, socket_errors, read_errors, write_errors, tls_handshake_failures,
                network_bytes_transmitted, network_bytes_received,
                socket_rtt_ms, network_bandwidth_bps, network_connection_failures,
                db_successful_writes, db_failed_writes, db_insert_latency_ms, db_query_latency_ms,
                db_active_connections, db_connection_failures, db_transaction_count,
                db_writes_per_sec, db_reads_per_sec, db_queue_waiting,
                peak_rss, virtual_memory, heap_usage, memory_growth_rate,
                total_subscriptions, total_database_writes, total_database_reads
            ) VALUES (
                $1,$2,$3, $4,$5,$6,$7, $8,$9, $10,$11,$12,$13,
                $14,$15,$16,$17,$18,
                $19,$20,$21, $22,$23,$24, $25,$26,$27,
                $28,$29,$30, $31,$32,$33, $34,$35,$36,
                $37,$38,$39,
                $40,$41,$42, $43,$44,$45, $46,$47,$48, $49,$50,
                $51,$52,$53, $54,$55,$56,$57,$58, $59,$60,
                $61,$62,$63, $64,$65,$66, $67,$68,$69,$70,
                $71,$72,$73, $74,$75,$76,$77,$78,
                $79,$80,$81,
                $82,$83,$84, $85,$86,$87,$88,
                $89,$90,
                $91,$92,$93,$94,$95, $96,$97, $98,$99,$100,
                $101,$102,$103,$104, $105,$106,$107,
                $108,$109,$110,$111,
                $112,$113,$114
            ) RETURNING id
        )");

        conn->prepare("delete_weekly_summary", R"(
            DELETE FROM weekly_metrics_summary WHERE id = $1
        )");

        // ---- subscriptions ----
        conn->prepare("insert_subscription", R"(
            INSERT INTO subscriptions (
                subscription_id, symbol, topic, stream_type, mode,
                filters_json, priority, created_at, removed_at,
                is_active, tenant_id, session_id
            ) VALUES ($1,$2,$3,$4,$5, $6,$7,$8,$9, $10,$11,$12)
            RETURNING subscription_id
        )");

        conn->prepare("get_subscription_by_id", R"(
            SELECT * FROM subscriptions WHERE subscription_id = $1
        )");

        conn->prepare("delete_subscription", R"(
            DELETE FROM subscriptions WHERE subscription_id = $1
        )");

        conn->prepare("update_subscription", R"(
            UPDATE subscriptions SET
                symbol=$1, topic=$2, stream_type=$3, mode=$4,
                filters_json=$5, priority=$6, removed_at=$7,
                is_active=$8, tenant_id=$9, session_id=$10
            WHERE subscription_id=$11
        )");

        // ---- feed_events ----
        conn->prepare("insert_feed_event", R"(
            INSERT INTO feed_events (
                event_id, actor_type, actor_id, action_type,
                target_type, target_id, result, error_code,
                trace_id, correlation_id, occurred_at, metadata
            ) VALUES ($1,$2,$3,$4, $5,$6,$7,$8, $9,$10,$11,$12)
            RETURNING event_id
        )");

        conn->prepare("get_feed_event_by_id", R"(
            SELECT * FROM feed_events WHERE event_id = $1
        )");

        conn->prepare("delete_feed_event", R"(
            DELETE FROM feed_events WHERE event_id = $1
        )");

        // ---- api_requests ----
        conn->prepare("insert_api_request", R"(
            INSERT INTO api_requests (
                request_id, endpoint, method, status_code, latency_ms,
                request_size, response_size, client_id, session_id,
                instance_id, timestamp
            ) VALUES ($1,$2,$3,$4,$5, $6,$7,$8,$9, $10,$11)
            RETURNING request_id
        )");

        conn->prepare("get_api_request_by_id", R"(
            SELECT * FROM api_requests WHERE request_id = $1
        )");

        conn->prepare("delete_api_request", R"(
            DELETE FROM api_requests WHERE request_id = $1
        )");

        // ---- exchange_health ----
        conn->prepare("insert_exchange_health", R"(
            INSERT INTO exchange_health (
                exchange_name, endpoint, status, last_success_at, last_error_at,
                error_count, rate_limit_hits, latency_ms, symbols_active,
                feed_lag_ms, checked_at
            ) VALUES ($1,$2,$3,$4,$5, $6,$7,$8,$9, $10,$11)
            RETURNING id
        )");

        conn->prepare("get_exchange_health_by_id", R"(
            SELECT * FROM exchange_health WHERE id = $1
        )");

        conn->prepare("delete_exchange_health", R"(
            DELETE FROM exchange_health WHERE id = $1
        )");

        // ---- backtest_jobs ----
        conn->prepare("insert_backtest_job", R"(
            INSERT INTO backtest_jobs (
                job_id, symbol, exchange, start_time, end_time,
                replay_speed, status, progress, created_at, completed_at
            ) VALUES ($1,$2,$3,$4,$5, $6,$7,$8,$9,$10)
            RETURNING job_id
        )");

        conn->prepare("get_backtest_job_by_id", R"(
            SELECT * FROM backtest_jobs WHERE job_id = $1
        )");

        conn->prepare("delete_backtest_job", R"(
            DELETE FROM backtest_jobs WHERE job_id = $1
        )");

        // ---- config_versions ----
        conn->prepare("insert_config_version", R"(
            INSERT INTO config_versions (
                config_version, build_sha, adapter_version, deployment_id,
                feature_flags, schema_version, applied_at
            ) VALUES ($1,$2,$3,$4, $5,$6,$7)
            RETURNING id
        )");

        conn->prepare("get_config_version_by_id", R"(
            SELECT * FROM config_versions WHERE id = $1
        )");

        conn->prepare("delete_config_version", R"(
            DELETE FROM config_versions WHERE id = $1
        )");

        return true;
    } catch (const std::exception& e) {
        std::cerr << "prepare_statements failed: " << e.what() << "\n";
        return false;
    }
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
        if (!pImpl_->conn->is_open()) return false;
        return pImpl_->prepare_monitoring_statements();
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

// ─── FeedMetricsSnapshot operations ──────────────────────────────────────────

std::optional<FeedMetricsSnapshot> SAdapter::create_feed_metrics_snapshot(const FeedMetricsSnapshot& s) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);

        pqxx::params p;
        p.append(s.instance_id);
        p.append(timestamp_to_string(s.measured_at));

        append_opt(p, s.p50_latency_ms);
        append_opt(p, s.p95_latency_ms);
        append_opt(p, s.p99_latency_ms);
        append_opt(p, s.avg_latency_ms);
        append_opt(p, s.drop_rate);
        append_opt(p, s.packet_loss_rate);
        append_opt(p, s.msgs_sent);
        append_opt(p, s.msgs_received);
        append_opt(p, s.bytes_sent);
        append_opt(p, s.bytes_received);
        append_opt(p, s.cpu_usage);
        append_opt(p, s.memory_usage);
        append_opt(p, s.thread_count);
        append_opt(p, s.event_loop_lag_ms);
        append_opt(p, s.uptime_seconds);

        // Per-category latency percentiles
        append_opt(p, s.exchange_p50_ms);
        append_opt(p, s.exchange_p95_ms);
        append_opt(p, s.exchange_p99_ms);
        append_opt(p, s.parsing_p50_ms);
        append_opt(p, s.parsing_p95_ms);
        append_opt(p, s.parsing_p99_ms);
        append_opt(p, s.normalization_p50_ms);
        append_opt(p, s.normalization_p95_ms);
        append_opt(p, s.normalization_p99_ms);
        append_opt(p, s.processing_p50_ms);
        append_opt(p, s.processing_p95_ms);
        append_opt(p, s.processing_p99_ms);
        append_opt(p, s.broadcast_p50_ms);
        append_opt(p, s.broadcast_p95_ms);
        append_opt(p, s.broadcast_p99_ms);
        append_opt(p, s.serialization_p50_ms);
        append_opt(p, s.serialization_p95_ms);
        append_opt(p, s.serialization_p99_ms);
        append_opt(p, s.socket_send_p50_ms);
        append_opt(p, s.socket_send_p95_ms);
        append_opt(p, s.socket_send_p99_ms);

        append_opt(p, s.latency_stats_jsonb);

        // Throughput rates
        append_opt(p, s.messages_per_sec);
        append_opt(p, s.packets_per_sec);
        append_opt(p, s.bytes_per_sec);
        append_opt(p, s.ticks_per_sec);
        append_opt(p, s.trades_per_sec);
        append_opt(p, s.orderbook_updates_per_sec);
        append_opt(p, s.broadcasts_per_sec);
        append_opt(p, s.subscriptions_per_sec);
        append_opt(p, s.database_writes_per_sec);
        append_opt(p, s.database_reads_per_sec);

        // Cumulative totals
        append_opt(p, s.total_messages);
        append_opt(p, s.total_packets);
        append_opt(p, s.total_bytes);
        append_opt(p, s.total_ticks);
        append_opt(p, s.total_trades);
        append_opt(p, s.total_orderbook_updates);
        append_opt(p, s.total_broadcasts);

        // Queue metrics
        append_opt(p, s.incoming_queue_depth);
        append_opt(p, s.outgoing_queue_depth);
        append_opt(p, s.serialization_queue_depth);
        append_opt(p, s.max_incoming_queue_depth);
        append_opt(p, s.max_outgoing_queue_depth);
        append_opt(p, s.max_serialization_queue_depth);
        append_opt(p, s.queue_overflow_count);
        append_opt(p, s.queue_wait_time_ms);
        append_opt(p, s.queue_processing_time_ms);
        append_opt_bool(p, s.queue_backpressure);

        // Feed health
        append_opt(p, s.packet_drops);
        append_opt(p, s.duplicate_packets);
        append_opt(p, s.out_of_order_packets);
        append_opt(p, s.sequence_gaps);
        append_opt(p, s.missing_ticks);
        append_opt(p, s.invalid_messages);
        append_opt(p, s.corrupted_packets);
        append_opt(p, s.parse_failures);
        append_opt_bool(p, s.stale_feed);
        append_opt(p, s.feed_health_score);
        append_opt(p, s.feed_health_status);

        // Session metrics
        append_opt(p, s.active_clients);
        append_opt(p, s.active_sessions);
        append_opt(p, s.active_subscriptions);
        append_opt(p, s.total_connections);
        append_opt(p, s.total_disconnections);
        append_opt(p, s.reconnect_count);
        append_opt(p, s.authentication_failures);
        append_opt(p, s.avg_session_duration_ms);
        append_opt(p, s.longest_session_duration_ms);

        // Network metrics
        append_opt(p, s.tcp_reconnects);
        append_opt(p, s.socket_errors);
        append_opt(p, s.read_errors);
        append_opt(p, s.write_errors);
        append_opt(p, s.tls_handshake_failures);
        append_opt(p, s.network_bytes_transmitted);
        append_opt(p, s.network_bytes_received);
        append_opt(p, s.socket_rtt_ms);
        append_opt(p, s.network_bandwidth_bps);
        append_opt(p, s.network_connection_failures);

        // Database metrics
        append_opt(p, s.db_successful_writes);
        append_opt(p, s.db_failed_writes);
        append_opt(p, s.db_insert_latency_ms);
        append_opt(p, s.db_query_latency_ms);
        append_opt(p, s.db_active_connections);
        append_opt(p, s.db_connection_failures);
        append_opt(p, s.db_transaction_count);
        append_opt(p, s.db_writes_per_sec);
        append_opt(p, s.db_reads_per_sec);
        append_opt(p, s.db_queue_waiting);

        // Extended system metrics
        append_opt(p, s.peak_rss);
        append_opt(p, s.virtual_memory);
        append_opt(p, s.heap_usage);
        append_opt(p, s.memory_growth_rate);

        // Missing cumulative counters
        append_opt(p, s.total_subscriptions);
        append_opt(p, s.total_database_writes);
        append_opt(p, s.total_database_reads);

        auto r = txn.exec_prepared("insert_snapshot", p);
        txn.commit();

        if (r.empty()) return std::nullopt;
        FeedMetricsSnapshot out = s;
        out.id = r[0]["id"].as<int64_t>();
        return out;
    } catch (const std::exception& e) {
        std::cerr << "create_feed_metrics_snapshot: " << e.what() << "\n";
        return std::nullopt;
    }
}

std::optional<FeedMetricsSnapshot> SAdapter::get_feed_metrics_snapshot_by_id(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec("SELECT * FROM feed_metrics_snapshots WHERE id=" + std::to_string(id));
        if (r.empty()) return std::nullopt;
        FeedMetricsSnapshot s;
        s.id = r[0]["id"].as<int64_t>();
        s.instance_id = r[0]["instance_id"].as<std::string>();
        s.measured_at = string_to_timestamp(r[0]["measured_at"].as<std::string>());
        // Optional fields not re-read here; caller can use get_by_condition for full data
        return s;
    } catch (const std::exception& e) {
        std::cerr << "get_feed_metrics_snapshot_by_id: " << e.what() << "\n";
        return std::nullopt;
    }
}

std::vector<FeedMetricsSnapshot> SAdapter::get_feed_metrics_snapshots_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    std::vector<FeedMetricsSnapshot> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string q = "SELECT * FROM feed_metrics_snapshots";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            FeedMetricsSnapshot s;
            s.id = row["id"].as<int64_t>();
            s.instance_id = row["instance_id"].as<std::string>();
            s.measured_at = string_to_timestamp(row["measured_at"].as<std::string>());
            out.push_back(std::move(s));
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "get_feed_metrics_snapshots: " << e.what() << "\n";
    }
    return out;
}

bool SAdapter::update_feed_metrics_snapshot(const FeedMetricsSnapshot& /*snapshot*/) {
    std::cerr << "update_feed_metrics_snapshot: not implemented (snapshots are append-only)\n";
    return false;
}

bool SAdapter::delete_feed_metrics_snapshot(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec("DELETE FROM feed_metrics_snapshots WHERE id=" + std::to_string(id));
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "delete_feed_metrics_snapshot: " << e.what() << "\n";
        return false;
    }
}

// ─── Subscription operations ─────────────────────────────────────────────────

std::optional<Subscription> SAdapter::create_subscription(const Subscription& sub) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        pqxx::params p;
        p.append(sub.subscription_id);
        p.append(sub.symbol);
        p.append(sub.topic);
        append_opt(p, sub.stream_type);
        append_opt(p, sub.mode);
        append_opt(p, sub.filters_json);
        append_opt(p, sub.priority);
        p.append(timestamp_to_string(sub.created_at));
        append_opt(p, sub.removed_at.has_value() ? std::optional<std::string>(timestamp_to_string(*sub.removed_at)) : std::nullopt);
        p.append(sub.is_active);
        append_opt(p, sub.tenant_id);
        append_opt(p, sub.session_id);
        auto r = txn.exec_prepared("insert_subscription", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        Subscription out = sub;
        out.subscription_id = r[0]["subscription_id"].as<std::string>();
        return out;
    } catch (const std::exception& e) {
        std::cerr << "create_subscription: " << e.what() << "\n";
        return std::nullopt;
    }
}

std::optional<Subscription> SAdapter::get_subscription_by_id(const std::string& subscription_id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec_prepared("get_subscription_by_id", subscription_id);
        if (r.empty()) return std::nullopt;
        Subscription sub;
        sub.subscription_id = r[0]["subscription_id"].as<std::string>();
        sub.symbol = r[0]["symbol"].as<std::string>();
        sub.topic = r[0]["topic"].as<std::string>();
        if (!r[0]["stream_type"].is_null()) sub.stream_type = r[0]["stream_type"].as<std::string>();
        if (!r[0]["mode"].is_null()) sub.mode = r[0]["mode"].as<std::string>();
        if (!r[0]["filters_json"].is_null()) sub.filters_json = r[0]["filters_json"].as<std::string>();
        if (!r[0]["priority"].is_null()) sub.priority = r[0]["priority"].as<int64_t>();
        sub.created_at = string_to_timestamp(r[0]["created_at"].as<std::string>());
        if (!r[0]["removed_at"].is_null()) sub.removed_at = string_to_timestamp(r[0]["removed_at"].as<std::string>());
        sub.is_active = r[0]["is_active"].as<bool>();
        if (!r[0]["tenant_id"].is_null()) sub.tenant_id = r[0]["tenant_id"].as<std::string>();
        if (!r[0]["session_id"].is_null()) sub.session_id = r[0]["session_id"].as<std::string>();
        return sub;
    } catch (const std::exception& e) {
        std::cerr << "get_subscription_by_id: " << e.what() << "\n";
        return std::nullopt;
    }
}

std::vector<Subscription> SAdapter::get_subscriptions_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    std::vector<Subscription> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string q = "SELECT * FROM subscriptions";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            Subscription sub;
            sub.subscription_id = row["subscription_id"].as<std::string>();
            sub.symbol = row["symbol"].as<std::string>();
            sub.topic = row["topic"].as<std::string>();
            if (!row["stream_type"].is_null()) sub.stream_type = row["stream_type"].as<std::string>();
            if (!row["mode"].is_null()) sub.mode = row["mode"].as<std::string>();
            if (!row["filters_json"].is_null()) sub.filters_json = row["filters_json"].as<std::string>();
            if (!row["priority"].is_null()) sub.priority = row["priority"].as<int64_t>();
            sub.created_at = string_to_timestamp(row["created_at"].as<std::string>());
            if (!row["removed_at"].is_null()) sub.removed_at = string_to_timestamp(row["removed_at"].as<std::string>());
            sub.is_active = row["is_active"].as<bool>();
            if (!row["tenant_id"].is_null()) sub.tenant_id = row["tenant_id"].as<std::string>();
            if (!row["session_id"].is_null()) sub.session_id = row["session_id"].as<std::string>();
            out.push_back(std::move(sub));
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "get_subscriptions: " << e.what() << "\n";
    }
    return out;
}

bool SAdapter::update_subscription(const Subscription& sub) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected() || sub.subscription_id.empty()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        pqxx::params p;
        p.append(sub.symbol);
        p.append(sub.topic);
        append_opt(p, sub.stream_type);
        append_opt(p, sub.mode);
        append_opt(p, sub.filters_json);
        append_opt(p, sub.priority);
        append_opt(p, sub.removed_at.has_value() ? std::optional<std::string>(timestamp_to_string(*sub.removed_at)) : std::nullopt);
        p.append(sub.is_active);
        append_opt(p, sub.tenant_id);
        append_opt(p, sub.session_id);
        p.append(sub.subscription_id);
        auto r = txn.exec_prepared("update_subscription", p);
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "update_subscription: " << e.what() << "\n";
        return false;
    }
}

bool SAdapter::delete_subscription(const std::string& subscription_id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected() || subscription_id.empty()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec_prepared("delete_subscription", subscription_id);
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "delete_subscription: " << e.what() << "\n";
        return false;
    }
}

// ─── FeedEvent operations ────────────────────────────────────────────────────

std::optional<FeedEvent> SAdapter::create_feed_event(const FeedEvent& ev) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        pqxx::params p;
        p.append(ev.event_id);
        append_opt(p, ev.actor_type);
        append_opt(p, ev.actor_id);
        p.append(ev.action_type);
        append_opt(p, ev.target_type);
        append_opt(p, ev.target_id);
        append_opt(p, ev.result);
        append_opt(p, ev.error_code);
        append_opt(p, ev.trace_id);
        append_opt(p, ev.correlation_id);
        p.append(timestamp_to_string(ev.occurred_at));
        append_opt(p, ev.metadata);
        auto r = txn.exec_prepared("insert_feed_event", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        FeedEvent out = ev;
        out.event_id = r[0]["event_id"].as<std::string>();
        return out;
    } catch (const std::exception& e) {
        std::cerr << "create_feed_event: " << e.what() << "\n";
        return std::nullopt;
    }
}

std::optional<FeedEvent> SAdapter::get_feed_event_by_id(const std::string& event_id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec_prepared("get_feed_event_by_id", event_id);
        if (r.empty()) return std::nullopt;
        FeedEvent ev;
        ev.event_id = r[0]["event_id"].as<std::string>();
        if (!r[0]["actor_type"].is_null()) ev.actor_type = r[0]["actor_type"].as<std::string>();
        if (!r[0]["actor_id"].is_null()) ev.actor_id = r[0]["actor_id"].as<std::string>();
        ev.action_type = r[0]["action_type"].as<std::string>();
        if (!r[0]["target_type"].is_null()) ev.target_type = r[0]["target_type"].as<std::string>();
        if (!r[0]["target_id"].is_null()) ev.target_id = r[0]["target_id"].as<std::string>();
        if (!r[0]["result"].is_null()) ev.result = r[0]["result"].as<std::string>();
        if (!r[0]["error_code"].is_null()) ev.error_code = r[0]["error_code"].as<std::string>();
        if (!r[0]["trace_id"].is_null()) ev.trace_id = r[0]["trace_id"].as<std::string>();
        if (!r[0]["correlation_id"].is_null()) ev.correlation_id = r[0]["correlation_id"].as<std::string>();
        ev.occurred_at = string_to_timestamp(r[0]["occurred_at"].as<std::string>());
        if (!r[0]["metadata"].is_null()) ev.metadata = r[0]["metadata"].as<std::string>();
        return ev;
    } catch (const std::exception& e) {
        std::cerr << "get_feed_event_by_id: " << e.what() << "\n";
        return std::nullopt;
    }
}

std::vector<FeedEvent> SAdapter::get_feed_events_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    std::vector<FeedEvent> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string q = "SELECT * FROM feed_events";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            FeedEvent ev;
            ev.event_id = row["event_id"].as<std::string>();
            if (!row["actor_type"].is_null()) ev.actor_type = row["actor_type"].as<std::string>();
            if (!row["actor_id"].is_null()) ev.actor_id = row["actor_id"].as<std::string>();
            ev.action_type = row["action_type"].as<std::string>();
            if (!row["target_type"].is_null()) ev.target_type = row["target_type"].as<std::string>();
            if (!row["target_id"].is_null()) ev.target_id = row["target_id"].as<std::string>();
            if (!row["result"].is_null()) ev.result = row["result"].as<std::string>();
            if (!row["error_code"].is_null()) ev.error_code = row["error_code"].as<std::string>();
            if (!row["trace_id"].is_null()) ev.trace_id = row["trace_id"].as<std::string>();
            if (!row["correlation_id"].is_null()) ev.correlation_id = row["correlation_id"].as<std::string>();
            ev.occurred_at = string_to_timestamp(row["occurred_at"].as<std::string>());
            if (!row["metadata"].is_null()) ev.metadata = row["metadata"].as<std::string>();
            out.push_back(std::move(ev));
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "get_feed_events: " << e.what() << "\n";
    }
    return out;
}

bool SAdapter::update_feed_event(const FeedEvent& /*event*/) {
    std::cerr << "update_feed_event: not implemented (events are append-only)\n";
    return false;
}

bool SAdapter::delete_feed_event(const std::string& event_id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected() || event_id.empty()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec_prepared("delete_feed_event", event_id);
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "delete_feed_event: " << e.what() << "\n";
        return false;
    }
}

// ─── ApiRequest operations ───────────────────────────────────────────────────

std::optional<ApiRequest> SAdapter::create_api_request(const ApiRequest& req) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        pqxx::params p;
        p.append(req.request_id);
        p.append(req.endpoint);
        p.append(req.method);
        append_opt(p, req.status_code);
        append_opt(p, req.latency_ms);
        append_opt(p, req.request_size);
        append_opt(p, req.response_size);
        append_opt(p, req.client_id);
        append_opt(p, req.session_id);
        append_opt(p, req.instance_id);
        p.append(timestamp_to_string(req.timestamp));
        auto r = txn.exec_prepared("insert_api_request", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        ApiRequest out = req;
        out.request_id = r[0]["request_id"].as<std::string>();
        return out;
    } catch (const std::exception& e) {
        std::cerr << "create_api_request: " << e.what() << "\n";
        return std::nullopt;
    }
}

std::optional<ApiRequest> SAdapter::get_api_request_by_id(const std::string& request_id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec_prepared("get_api_request_by_id", request_id);
        if (r.empty()) return std::nullopt;
        ApiRequest req;
        req.request_id = r[0]["request_id"].as<std::string>();
        req.endpoint = r[0]["endpoint"].as<std::string>();
        req.method = r[0]["method"].as<std::string>();
        if (!r[0]["status_code"].is_null()) req.status_code = r[0]["status_code"].as<int64_t>();
        if (!r[0]["latency_ms"].is_null()) req.latency_ms = r[0]["latency_ms"].as<double>();
        if (!r[0]["request_size"].is_null()) req.request_size = r[0]["request_size"].as<int64_t>();
        if (!r[0]["response_size"].is_null()) req.response_size = r[0]["response_size"].as<int64_t>();
        if (!r[0]["client_id"].is_null()) req.client_id = r[0]["client_id"].as<std::string>();
        if (!r[0]["session_id"].is_null()) req.session_id = r[0]["session_id"].as<std::string>();
        if (!r[0]["instance_id"].is_null()) req.instance_id = r[0]["instance_id"].as<std::string>();
        req.timestamp = string_to_timestamp(r[0]["timestamp"].as<std::string>());
        return req;
    } catch (const std::exception& e) {
        std::cerr << "get_api_request_by_id: " << e.what() << "\n";
        return std::nullopt;
    }
}

std::vector<ApiRequest> SAdapter::get_api_requests_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    std::vector<ApiRequest> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string q = "SELECT * FROM api_requests";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            ApiRequest req;
            req.request_id = row["request_id"].as<std::string>();
            req.endpoint = row["endpoint"].as<std::string>();
            req.method = row["method"].as<std::string>();
            if (!row["status_code"].is_null()) req.status_code = row["status_code"].as<int64_t>();
            if (!row["latency_ms"].is_null()) req.latency_ms = row["latency_ms"].as<double>();
            if (!row["request_size"].is_null()) req.request_size = row["request_size"].as<int64_t>();
            if (!row["response_size"].is_null()) req.response_size = row["response_size"].as<int64_t>();
            if (!row["client_id"].is_null()) req.client_id = row["client_id"].as<std::string>();
            if (!row["session_id"].is_null()) req.session_id = row["session_id"].as<std::string>();
            if (!row["instance_id"].is_null()) req.instance_id = row["instance_id"].as<std::string>();
            req.timestamp = string_to_timestamp(row["timestamp"].as<std::string>());
            out.push_back(std::move(req));
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "get_api_requests: " << e.what() << "\n";
    }
    return out;
}

bool SAdapter::update_api_request(const ApiRequest& /*request*/) {
    std::cerr << "update_api_request: not implemented (requests are append-only)\n";
    return false;
}

bool SAdapter::delete_api_request(const std::string& request_id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected() || request_id.empty()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec_prepared("delete_api_request", request_id);
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "delete_api_request: " << e.what() << "\n";
        return false;
    }
}

// ─── ExchangeHealth operations ───────────────────────────────────────────────

std::optional<ExchangeHealth> SAdapter::create_exchange_health(const ExchangeHealth& h) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        pqxx::params p;
        p.append(h.exchange_name);
        append_opt(p, h.endpoint);
        p.append(h.status);
        append_opt(p, h.last_success_at.has_value() ? std::optional<std::string>(timestamp_to_string(*h.last_success_at)) : std::nullopt);
        append_opt(p, h.last_error_at.has_value() ? std::optional<std::string>(timestamp_to_string(*h.last_error_at)) : std::nullopt);
        append_opt(p, h.error_count);
        append_opt(p, h.rate_limit_hits);
        append_opt(p, h.latency_ms);
        append_opt(p, h.symbols_active);
        append_opt(p, h.feed_lag_ms);
        p.append(timestamp_to_string(h.checked_at));
        auto r = txn.exec_prepared("insert_exchange_health", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        ExchangeHealth out = h;
        out.id = r[0]["id"].as<int64_t>();
        return out;
    } catch (const std::exception& e) {
        std::cerr << "create_exchange_health: " << e.what() << "\n";
        return std::nullopt;
    }
}

std::optional<ExchangeHealth> SAdapter::get_exchange_health_by_id(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec_prepared("get_exchange_health_by_id", id);
        if (r.empty()) return std::nullopt;
        ExchangeHealth h;
        h.id = r[0]["id"].as<int64_t>();
        h.exchange_name = r[0]["exchange_name"].as<std::string>();
        if (!r[0]["endpoint"].is_null()) h.endpoint = r[0]["endpoint"].as<std::string>();
        h.status = r[0]["status"].as<std::string>();
        if (!r[0]["last_success_at"].is_null()) h.last_success_at = string_to_timestamp(r[0]["last_success_at"].as<std::string>());
        if (!r[0]["last_error_at"].is_null()) h.last_error_at = string_to_timestamp(r[0]["last_error_at"].as<std::string>());
        if (!r[0]["error_count"].is_null()) h.error_count = r[0]["error_count"].as<int64_t>();
        if (!r[0]["rate_limit_hits"].is_null()) h.rate_limit_hits = r[0]["rate_limit_hits"].as<int64_t>();
        if (!r[0]["latency_ms"].is_null()) h.latency_ms = r[0]["latency_ms"].as<double>();
        if (!r[0]["symbols_active"].is_null()) h.symbols_active = r[0]["symbols_active"].as<int64_t>();
        if (!r[0]["feed_lag_ms"].is_null()) h.feed_lag_ms = r[0]["feed_lag_ms"].as<double>();
        h.checked_at = string_to_timestamp(r[0]["checked_at"].as<std::string>());
        return h;
    } catch (const std::exception& e) {
        std::cerr << "get_exchange_health_by_id: " << e.what() << "\n";
        return std::nullopt;
    }
}

std::vector<ExchangeHealth> SAdapter::get_exchange_healths_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    std::vector<ExchangeHealth> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string q = "SELECT * FROM exchange_health";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            ExchangeHealth h;
            h.id = row["id"].as<int64_t>();
            h.exchange_name = row["exchange_name"].as<std::string>();
            if (!row["endpoint"].is_null()) h.endpoint = row["endpoint"].as<std::string>();
            h.status = row["status"].as<std::string>();
            if (!row["last_success_at"].is_null()) h.last_success_at = string_to_timestamp(row["last_success_at"].as<std::string>());
            if (!row["last_error_at"].is_null()) h.last_error_at = string_to_timestamp(row["last_error_at"].as<std::string>());
            if (!row["error_count"].is_null()) h.error_count = row["error_count"].as<int64_t>();
            if (!row["rate_limit_hits"].is_null()) h.rate_limit_hits = row["rate_limit_hits"].as<int64_t>();
            if (!row["latency_ms"].is_null()) h.latency_ms = row["latency_ms"].as<double>();
            if (!row["symbols_active"].is_null()) h.symbols_active = row["symbols_active"].as<int64_t>();
            if (!row["feed_lag_ms"].is_null()) h.feed_lag_ms = row["feed_lag_ms"].as<double>();
            h.checked_at = string_to_timestamp(row["checked_at"].as<std::string>());
            out.push_back(std::move(h));
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "get_exchange_healths: " << e.what() << "\n";
    }
    return out;
}

bool SAdapter::update_exchange_health(const ExchangeHealth& /*health*/) {
    std::cerr << "update_exchange_health: not implemented\n";
    return false;
}

bool SAdapter::delete_exchange_health(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec_prepared("delete_exchange_health", id);
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "delete_exchange_health: " << e.what() << "\n";
        return false;
    }
}

// ─── BacktestJob operations ──────────────────────────────────────────────────

std::optional<BacktestJob> SAdapter::create_backtest_job(const BacktestJob& job) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        pqxx::params p;
        p.append(job.job_id);
        append_opt(p, job.symbol);
        append_opt(p, job.exchange);
        append_opt(p, job.start_time.has_value() ? std::optional<std::string>(timestamp_to_string(*job.start_time)) : std::nullopt);
        append_opt(p, job.end_time.has_value() ? std::optional<std::string>(timestamp_to_string(*job.end_time)) : std::nullopt);
        append_opt(p, job.replay_speed);
        p.append(job.status);
        append_opt(p, job.progress);
        append_opt(p, job.created_at.has_value() ? std::optional<std::string>(timestamp_to_string(*job.created_at)) : std::nullopt);
        append_opt(p, job.completed_at.has_value() ? std::optional<std::string>(timestamp_to_string(*job.completed_at)) : std::nullopt);
        auto r = txn.exec_prepared("insert_backtest_job", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        BacktestJob out = job;
        out.job_id = r[0]["job_id"].as<std::string>();
        return out;
    } catch (const std::exception& e) {
        std::cerr << "create_backtest_job: " << e.what() << "\n";
        return std::nullopt;
    }
}

std::optional<BacktestJob> SAdapter::get_backtest_job_by_id(const std::string& job_id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec_prepared("get_backtest_job_by_id", job_id);
        if (r.empty()) return std::nullopt;
        BacktestJob job;
        job.job_id = r[0]["job_id"].as<std::string>();
        if (!r[0]["symbol"].is_null()) job.symbol = r[0]["symbol"].as<std::string>();
        if (!r[0]["exchange"].is_null()) job.exchange = r[0]["exchange"].as<std::string>();
        if (!r[0]["start_time"].is_null()) job.start_time = string_to_timestamp(r[0]["start_time"].as<std::string>());
        if (!r[0]["end_time"].is_null()) job.end_time = string_to_timestamp(r[0]["end_time"].as<std::string>());
        if (!r[0]["replay_speed"].is_null()) job.replay_speed = r[0]["replay_speed"].as<int64_t>();
        job.status = r[0]["status"].as<std::string>();
        if (!r[0]["progress"].is_null()) job.progress = r[0]["progress"].as<double>();
        if (!r[0]["created_at"].is_null()) job.created_at = string_to_timestamp(r[0]["created_at"].as<std::string>());
        if (!r[0]["completed_at"].is_null()) job.completed_at = string_to_timestamp(r[0]["completed_at"].as<std::string>());
        return job;
    } catch (const std::exception& e) {
        std::cerr << "get_backtest_job_by_id: " << e.what() << "\n";
        return std::nullopt;
    }
}

std::vector<BacktestJob> SAdapter::get_backtest_jobs_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    std::vector<BacktestJob> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string q = "SELECT * FROM backtest_jobs";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            BacktestJob job;
            job.job_id = row["job_id"].as<std::string>();
            if (!row["symbol"].is_null()) job.symbol = row["symbol"].as<std::string>();
            if (!row["exchange"].is_null()) job.exchange = row["exchange"].as<std::string>();
            if (!row["start_time"].is_null()) job.start_time = string_to_timestamp(row["start_time"].as<std::string>());
            if (!row["end_time"].is_null()) job.end_time = string_to_timestamp(row["end_time"].as<std::string>());
            if (!row["replay_speed"].is_null()) job.replay_speed = row["replay_speed"].as<int64_t>();
            job.status = row["status"].as<std::string>();
            if (!row["progress"].is_null()) job.progress = row["progress"].as<double>();
            if (!row["created_at"].is_null()) job.created_at = string_to_timestamp(row["created_at"].as<std::string>());
            if (!row["completed_at"].is_null()) job.completed_at = string_to_timestamp(row["completed_at"].as<std::string>());
            out.push_back(std::move(job));
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "get_backtest_jobs: " << e.what() << "\n";
    }
    return out;
}

bool SAdapter::update_backtest_job(const BacktestJob& /*job*/) {
    std::cerr << "update_backtest_job: not implemented\n";
    return false;
}

bool SAdapter::delete_backtest_job(const std::string& job_id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected() || job_id.empty()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec_prepared("delete_backtest_job", job_id);
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "delete_backtest_job: " << e.what() << "\n";
        return false;
    }
}

// ─── ConfigVersion operations ────────────────────────────────────────────────

std::optional<ConfigVersion> SAdapter::create_config_version(const ConfigVersion& cfg) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        pqxx::params p;
        p.append(cfg.config_version);
        p.append(cfg.build_sha);
        p.append(cfg.adapter_version);
        p.append(cfg.deployment_id);
        append_opt(p, cfg.feature_flags);
        p.append(cfg.schema_version);
        p.append(timestamp_to_string(cfg.applied_at));
        auto r = txn.exec_prepared("insert_config_version", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        ConfigVersion out = cfg;
        out.id = r[0]["id"].as<int64_t>();
        return out;
    } catch (const std::exception& e) {
        std::cerr << "create_config_version: " << e.what() << "\n";
        return std::nullopt;
    }
}

std::optional<ConfigVersion> SAdapter::get_config_version_by_id(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec_prepared("get_config_version_by_id", id);
        if (r.empty()) return std::nullopt;
        ConfigVersion cfg;
        cfg.id = r[0]["id"].as<int64_t>();
        cfg.config_version = r[0]["config_version"].as<std::string>();
        cfg.build_sha = r[0]["build_sha"].as<std::string>();
        cfg.adapter_version = r[0]["adapter_version"].as<std::string>();
        cfg.deployment_id = r[0]["deployment_id"].as<std::string>();
        if (!r[0]["feature_flags"].is_null()) cfg.feature_flags = r[0]["feature_flags"].as<std::string>();
        cfg.schema_version = r[0]["schema_version"].as<int64_t>();
        cfg.applied_at = string_to_timestamp(r[0]["applied_at"].as<std::string>());
        return cfg;
    } catch (const std::exception& e) {
        std::cerr << "get_config_version_by_id: " << e.what() << "\n";
        return std::nullopt;
    }
}

std::vector<ConfigVersion> SAdapter::get_config_versions_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    std::vector<ConfigVersion> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string q = "SELECT * FROM config_versions";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            ConfigVersion cfg;
            cfg.id = row["id"].as<int64_t>();
            cfg.config_version = row["config_version"].as<std::string>();
            cfg.build_sha = row["build_sha"].as<std::string>();
            cfg.adapter_version = row["adapter_version"].as<std::string>();
            cfg.deployment_id = row["deployment_id"].as<std::string>();
            if (!row["feature_flags"].is_null()) cfg.feature_flags = row["feature_flags"].as<std::string>();
            cfg.schema_version = row["schema_version"].as<int64_t>();
            cfg.applied_at = string_to_timestamp(row["applied_at"].as<std::string>());
            out.push_back(std::move(cfg));
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "get_config_versions: " << e.what() << "\n";
    }
    return out;
}

bool SAdapter::update_config_version(const ConfigVersion& /*config*/) {
    std::cerr << "update_config_version: not implemented\n";
    return false;
}

bool SAdapter::delete_config_version(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec_prepared("delete_config_version", id);
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "delete_config_version: " << e.what() << "\n";
        return false;
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// MONITORING PERSISTENCE — NEW TABLES
// ══════════════════════════════════════════════════════════════════════════════

// ─── ExchangeMetricsHistory ──────────────────────────────────────────────────

std::optional<ExchangeMetricsEntry> SAdapter::create_exchange_metrics_entry(const ExchangeMetricsEntry& e) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        pqxx::params p;
        p.append(e.instance_id);
        p.append(e.exchange_name);
        p.append(timestamp_to_string(e.snapshot_time));
        append_bool(p, e.connected);
        append_opt(p, e.uptime_seconds);
        append_opt(p, e.reconnect_count);
        append_opt(p, e.heartbeat_failures);
        append_opt(p, e.websocket_disconnects);
        append_opt(p, e.messages_received);
        append_opt(p, e.messages_dropped);
        append_opt(p, e.parse_errors);
        append_opt(p, e.feed_lag_ms);
        append_opt(p, e.exchange_latency_ms);
        append_bool(p, e.stale);
        auto r = txn.exec_prepared("insert_exchange_metrics", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        ExchangeMetricsEntry out = e;
        out.id = r[0]["id"].as<int64_t>();
        return out;
    } catch (const std::exception& ex) {
        std::cerr << "create_exchange_metrics_entry: " << ex.what() << "\n";
        return std::nullopt;
    }
}

std::vector<ExchangeMetricsEntry> SAdapter::get_exchange_metrics_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    std::vector<ExchangeMetricsEntry> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string q = "SELECT * FROM exchange_metrics_history";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            ExchangeMetricsEntry e;
            e.id = row["id"].as<int64_t>();
            e.instance_id = row["instance_id"].as<std::string>();
            e.exchange_name = row["exchange_name"].as<std::string>();
            e.snapshot_time = string_to_timestamp(row["snapshot_time"].as<std::string>());
            e.connected = row["connected"].as<bool>();
            if (!row["uptime_seconds"].is_null()) e.uptime_seconds = row["uptime_seconds"].as<double>();
            if (!row["reconnect_count"].is_null()) e.reconnect_count = row["reconnect_count"].as<int64_t>();
            if (!row["heartbeat_failures"].is_null()) e.heartbeat_failures = row["heartbeat_failures"].as<int64_t>();
            if (!row["websocket_disconnects"].is_null()) e.websocket_disconnects = row["websocket_disconnects"].as<int64_t>();
            if (!row["messages_received"].is_null()) e.messages_received = row["messages_received"].as<int64_t>();
            if (!row["messages_dropped"].is_null()) e.messages_dropped = row["messages_dropped"].as<int64_t>();
            if (!row["parse_errors"].is_null()) e.parse_errors = row["parse_errors"].as<int64_t>();
            if (!row["feed_lag_ms"].is_null()) e.feed_lag_ms = row["feed_lag_ms"].as<double>();
            if (!row["exchange_latency_ms"].is_null()) e.exchange_latency_ms = row["exchange_latency_ms"].as<double>();
            e.stale = row["stale"].as<bool>();
            out.push_back(std::move(e));
        }
        txn.commit();
    } catch (const std::exception& ex) {
        std::cerr << "get_exchange_metrics: " << ex.what() << "\n";
    }
    return out;
}

bool SAdapter::delete_exchange_metrics_entry(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec("DELETE FROM exchange_metrics_history WHERE id=" + std::to_string(id));
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "delete_exchange_metrics_entry: " << e.what() << "\n";
        return false;
    }
}

// ─── QueueHistory ────────────────────────────────────────────────────────────

std::optional<QueueEntry> SAdapter::create_queue_entry(const QueueEntry& e) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        pqxx::params p;
        p.append(e.instance_id);
        p.append(timestamp_to_string(e.measured_at));
        p.append(e.incoming_depth);
        p.append(e.outgoing_depth);
        p.append(e.serialization_depth);
        append_opt(p, e.max_incoming_depth);
        append_opt(p, e.max_outgoing_depth);
        append_opt(p, e.max_serialization_depth);
        append_opt(p, e.overflow_count);
        append_bool(p, e.backpressure);
        append_opt(p, e.wait_time_ms);
        append_opt(p, e.processing_time_ms);
        auto r = txn.exec_prepared("insert_queue_entry", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        QueueEntry out = e;
        out.id = r[0]["id"].as<int64_t>();
        return out;
    } catch (const std::exception& ex) {
        std::cerr << "create_queue_entry: " << ex.what() << "\n";
        return std::nullopt;
    }
}

std::vector<QueueEntry> SAdapter::get_queue_entries_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    std::vector<QueueEntry> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string q = "SELECT * FROM queue_history";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            QueueEntry e;
            e.id = row["id"].as<int64_t>();
            e.instance_id = row["instance_id"].as<std::string>();
            e.measured_at = string_to_timestamp(row["measured_at"].as<std::string>());
            e.incoming_depth = row["incoming_depth"].as<int64_t>();
            e.outgoing_depth = row["outgoing_depth"].as<int64_t>();
            e.serialization_depth = row["serialization_depth"].as<int64_t>();
            if (!row["max_incoming_depth"].is_null()) e.max_incoming_depth = row["max_incoming_depth"].as<int64_t>();
            if (!row["max_outgoing_depth"].is_null()) e.max_outgoing_depth = row["max_outgoing_depth"].as<int64_t>();
            if (!row["max_serialization_depth"].is_null()) e.max_serialization_depth = row["max_serialization_depth"].as<int64_t>();
            if (!row["overflow_count"].is_null()) e.overflow_count = row["overflow_count"].as<int64_t>();
            e.backpressure = row["backpressure"].as<bool>();
            if (!row["wait_time_ms"].is_null()) e.wait_time_ms = row["wait_time_ms"].as<double>();
            if (!row["processing_time_ms"].is_null()) e.processing_time_ms = row["processing_time_ms"].as<double>();
            out.push_back(std::move(e));
        }
        txn.commit();
    } catch (const std::exception& ex) {
        std::cerr << "get_queue_entries: " << ex.what() << "\n";
    }
    return out;
}

bool SAdapter::delete_queue_entry(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec("DELETE FROM queue_history WHERE id=" + std::to_string(id));
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "delete_queue_entry: " << e.what() << "\n";
        return false;
    }
}

// ─── SystemMetricsHistory ────────────────────────────────────────────────────

std::optional<SystemMetricsEntry> SAdapter::create_system_metrics_entry(const SystemMetricsEntry& e) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        pqxx::params p;
        p.append(e.instance_id);
        p.append(timestamp_to_string(e.measured_at));
        append_opt(p, e.cpu_usage_percent);
        append_opt(p, e.memory_rss);
        append_opt(p, e.peak_rss);
        append_opt(p, e.virtual_memory);
        append_opt(p, e.heap_usage);
        append_opt(p, e.memory_growth_rate);
        append_opt(p, e.thread_count);
        append_opt(p, e.uptime_seconds);
        auto r = txn.exec_prepared("insert_system_metrics", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        SystemMetricsEntry out = e;
        out.id = r[0]["id"].as<int64_t>();
        return out;
    } catch (const std::exception& ex) {
        std::cerr << "create_system_metrics_entry: " << ex.what() << "\n";
        return std::nullopt;
    }
}

std::vector<SystemMetricsEntry> SAdapter::get_system_metrics_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    std::vector<SystemMetricsEntry> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string q = "SELECT * FROM system_metrics_history";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            SystemMetricsEntry e;
            e.id = row["id"].as<int64_t>();
            e.instance_id = row["instance_id"].as<std::string>();
            e.measured_at = string_to_timestamp(row["measured_at"].as<std::string>());
            if (!row["cpu_usage_percent"].is_null()) e.cpu_usage_percent = row["cpu_usage_percent"].as<double>();
            if (!row["memory_rss"].is_null()) e.memory_rss = row["memory_rss"].as<int64_t>();
            if (!row["peak_rss"].is_null()) e.peak_rss = row["peak_rss"].as<int64_t>();
            if (!row["virtual_memory"].is_null()) e.virtual_memory = row["virtual_memory"].as<int64_t>();
            if (!row["heap_usage"].is_null()) e.heap_usage = row["heap_usage"].as<int64_t>();
            if (!row["memory_growth_rate"].is_null()) e.memory_growth_rate = row["memory_growth_rate"].as<double>();
            if (!row["thread_count"].is_null()) e.thread_count = row["thread_count"].as<int64_t>();
            if (!row["uptime_seconds"].is_null()) e.uptime_seconds = row["uptime_seconds"].as<double>();
            out.push_back(std::move(e));
        }
        txn.commit();
    } catch (const std::exception& ex) {
        std::cerr << "get_system_metrics: " << ex.what() << "\n";
    }
    return out;
}

bool SAdapter::delete_system_metrics_entry(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec("DELETE FROM system_metrics_history WHERE id=" + std::to_string(id));
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "delete_system_metrics_entry: " << e.what() << "\n";
        return false;
    }
}

// ─── NetworkMetricsHistory ───────────────────────────────────────────────────

std::optional<NetworkMetricsEntry> SAdapter::create_network_metrics_entry(const NetworkMetricsEntry& e) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        pqxx::params p;
        p.append(e.instance_id);
        p.append(timestamp_to_string(e.measured_at));
        append_opt(p, e.tcp_reconnects);
        append_opt(p, e.socket_errors);
        append_opt(p, e.read_errors);
        append_opt(p, e.write_errors);
        append_opt(p, e.tls_handshake_failures);
        append_opt(p, e.bytes_transmitted);
        append_opt(p, e.bytes_received);
        append_opt(p, e.socket_rtt_ms);
        append_opt(p, e.bandwidth_bps);
        append_opt(p, e.connection_failures);
        auto r = txn.exec_prepared("insert_network_metrics", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        NetworkMetricsEntry out = e;
        out.id = r[0]["id"].as<int64_t>();
        return out;
    } catch (const std::exception& ex) {
        std::cerr << "create_network_metrics_entry: " << ex.what() << "\n";
        return std::nullopt;
    }
}

std::vector<NetworkMetricsEntry> SAdapter::get_network_metrics_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    std::vector<NetworkMetricsEntry> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string q = "SELECT * FROM network_metrics_history";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            NetworkMetricsEntry e;
            e.id = row["id"].as<int64_t>();
            e.instance_id = row["instance_id"].as<std::string>();
            e.measured_at = string_to_timestamp(row["measured_at"].as<std::string>());
            if (!row["tcp_reconnects"].is_null()) e.tcp_reconnects = row["tcp_reconnects"].as<int64_t>();
            if (!row["socket_errors"].is_null()) e.socket_errors = row["socket_errors"].as<int64_t>();
            if (!row["read_errors"].is_null()) e.read_errors = row["read_errors"].as<int64_t>();
            if (!row["write_errors"].is_null()) e.write_errors = row["write_errors"].as<int64_t>();
            if (!row["tls_handshake_failures"].is_null()) e.tls_handshake_failures = row["tls_handshake_failures"].as<int64_t>();
            if (!row["bytes_transmitted"].is_null()) e.bytes_transmitted = row["bytes_transmitted"].as<int64_t>();
            if (!row["bytes_received"].is_null()) e.bytes_received = row["bytes_received"].as<int64_t>();
            if (!row["socket_rtt_ms"].is_null()) e.socket_rtt_ms = row["socket_rtt_ms"].as<double>();
            if (!row["bandwidth_bps"].is_null()) e.bandwidth_bps = row["bandwidth_bps"].as<double>();
            if (!row["connection_failures"].is_null()) e.connection_failures = row["connection_failures"].as<int64_t>();
            out.push_back(std::move(e));
        }
        txn.commit();
    } catch (const std::exception& ex) {
        std::cerr << "get_network_metrics: " << ex.what() << "\n";
    }
    return out;
}

bool SAdapter::delete_network_metrics_entry(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec("DELETE FROM network_metrics_history WHERE id=" + std::to_string(id));
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "delete_network_metrics_entry: " << e.what() << "\n";
        return false;
    }
}

// ─── DatabaseMetricsHistory ──────────────────────────────────────────────────

std::optional<DatabaseMetricsEntry> SAdapter::create_database_metrics_entry(const DatabaseMetricsEntry& e) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        pqxx::params p;
        p.append(e.instance_id);
        p.append(timestamp_to_string(e.measured_at));
        append_opt(p, e.successful_writes);
        append_opt(p, e.failed_writes);
        append_opt(p, e.insert_latency_ms);
        append_opt(p, e.query_latency_ms);
        append_opt(p, e.active_connections);
        append_opt(p, e.connection_failures);
        append_opt(p, e.transaction_count);
        append_opt(p, e.writes_per_sec);
        append_opt(p, e.reads_per_sec);
        append_opt(p, e.queue_waiting);
        auto r = txn.exec_prepared("insert_database_metrics", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        DatabaseMetricsEntry out = e;
        out.id = r[0]["id"].as<int64_t>();
        return out;
    } catch (const std::exception& ex) {
        std::cerr << "create_database_metrics_entry: " << ex.what() << "\n";
        return std::nullopt;
    }
}

std::vector<DatabaseMetricsEntry> SAdapter::get_database_metrics_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    std::vector<DatabaseMetricsEntry> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string q = "SELECT * FROM database_metrics_history";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            DatabaseMetricsEntry e;
            e.id = row["id"].as<int64_t>();
            e.instance_id = row["instance_id"].as<std::string>();
            e.measured_at = string_to_timestamp(row["measured_at"].as<std::string>());
            if (!row["successful_writes"].is_null()) e.successful_writes = row["successful_writes"].as<int64_t>();
            if (!row["failed_writes"].is_null()) e.failed_writes = row["failed_writes"].as<int64_t>();
            if (!row["insert_latency_ms"].is_null()) e.insert_latency_ms = row["insert_latency_ms"].as<double>();
            if (!row["query_latency_ms"].is_null()) e.query_latency_ms = row["query_latency_ms"].as<double>();
            if (!row["active_connections"].is_null()) e.active_connections = row["active_connections"].as<int64_t>();
            if (!row["connection_failures"].is_null()) e.connection_failures = row["connection_failures"].as<int64_t>();
            if (!row["transaction_count"].is_null()) e.transaction_count = row["transaction_count"].as<int64_t>();
            if (!row["writes_per_sec"].is_null()) e.writes_per_sec = row["writes_per_sec"].as<double>();
            if (!row["reads_per_sec"].is_null()) e.reads_per_sec = row["reads_per_sec"].as<double>();
            if (!row["queue_waiting"].is_null()) e.queue_waiting = row["queue_waiting"].as<int64_t>();
            out.push_back(std::move(e));
        }
        txn.commit();
    } catch (const std::exception& ex) {
        std::cerr << "get_database_metrics: " << ex.what() << "\n";
    }
    return out;
}

bool SAdapter::delete_database_metrics_entry(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec("DELETE FROM database_metrics_history WHERE id=" + std::to_string(id));
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "delete_database_metrics_entry: " << e.what() << "\n";
        return false;
    }
}

// ─── Alerts ──────────────────────────────────────────────────────────────────

std::optional<Alert> SAdapter::create_alert(const Alert& a) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        pqxx::params p;
        append_opt(p, a.instance_id);
        p.append(a.severity);
        p.append(a.source);
        p.append(a.metric_name);
        append_opt(p, a.current_value);
        append_opt(p, a.threshold);
        append_opt(p, a.message);
        p.append(a.acknowledged);
        p.append(timestamp_to_string(a.created_at));
        append_opt(p, a.resolved_at.has_value() ? std::optional<std::string>(timestamp_to_string(*a.resolved_at)) : std::nullopt);
        auto r = txn.exec_prepared("insert_alert", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        Alert out = a;
        out.alert_id = r[0]["alert_id"].as<std::string>();
        return out;
    } catch (const std::exception& e) {
        std::cerr << "create_alert: " << e.what() << "\n";
        return std::nullopt;
    }
}

std::optional<Alert> SAdapter::get_alert_by_id(const std::string& alert_id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec_prepared("get_alert_by_id", alert_id);
        if (r.empty()) return std::nullopt;
        Alert a;
        a.alert_id = r[0]["alert_id"].as<std::string>();
        if (!r[0]["instance_id"].is_null()) a.instance_id = r[0]["instance_id"].as<std::string>();
        a.severity = r[0]["severity"].as<std::string>();
        a.source = r[0]["source"].as<std::string>();
        a.metric_name = r[0]["metric_name"].as<std::string>();
        if (!r[0]["current_value"].is_null()) a.current_value = r[0]["current_value"].as<double>();
        if (!r[0]["threshold"].is_null()) a.threshold = r[0]["threshold"].as<double>();
        if (!r[0]["message"].is_null()) a.message = r[0]["message"].as<std::string>();
        a.acknowledged = r[0]["acknowledged"].as<bool>();
        a.created_at = string_to_timestamp(r[0]["created_at"].as<std::string>());
        if (!r[0]["resolved_at"].is_null()) a.resolved_at = string_to_timestamp(r[0]["resolved_at"].as<std::string>());
        return a;
    } catch (const std::exception& e) {
        std::cerr << "get_alert_by_id: " << e.what() << "\n";
        return std::nullopt;
    }
}

std::vector<Alert> SAdapter::get_alerts_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    std::vector<Alert> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string q = "SELECT * FROM alerts";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            Alert a;
            a.alert_id = row["alert_id"].as<std::string>();
            if (!row["instance_id"].is_null()) a.instance_id = row["instance_id"].as<std::string>();
            a.severity = row["severity"].as<std::string>();
            a.source = row["source"].as<std::string>();
            a.metric_name = row["metric_name"].as<std::string>();
            if (!row["current_value"].is_null()) a.current_value = row["current_value"].as<double>();
            if (!row["threshold"].is_null()) a.threshold = row["threshold"].as<double>();
            if (!row["message"].is_null()) a.message = row["message"].as<std::string>();
            a.acknowledged = row["acknowledged"].as<bool>();
            a.created_at = string_to_timestamp(row["created_at"].as<std::string>());
            if (!row["resolved_at"].is_null()) a.resolved_at = string_to_timestamp(row["resolved_at"].as<std::string>());
            out.push_back(std::move(a));
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "get_alerts: " << e.what() << "\n";
    }
    return out;
}

bool SAdapter::update_alert(const Alert& a) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected() || a.alert_id.empty()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        pqxx::params p;
        append_opt(p, a.instance_id);
        p.append(a.severity);
        p.append(a.source);
        p.append(a.metric_name);
        append_opt(p, a.current_value);
        append_opt(p, a.threshold);
        append_opt(p, a.message);
        p.append(a.acknowledged);
        append_opt(p, a.resolved_at.has_value() ? std::optional<std::string>(timestamp_to_string(*a.resolved_at)) : std::nullopt);
        p.append(a.alert_id);
        auto r = txn.exec_prepared("update_alert", p);
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "update_alert: " << e.what() << "\n";
        return false;
    }
}

bool SAdapter::delete_alert(const std::string& alert_id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected() || alert_id.empty()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec_prepared("delete_alert", alert_id);
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "delete_alert: " << e.what() << "\n";
        return false;
    }
}

// ─── MetricThresholds ────────────────────────────────────────────────────────

std::optional<MetricThreshold> SAdapter::create_metric_threshold(const MetricThreshold& t) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        pqxx::params p;
        p.append(t.instance_id);
        p.append(t.metric_name);
        p.append(t.source);
        append_opt(p, t.warning_threshold);
        append_opt(p, t.critical_threshold);
        p.append(t.op);
        p.append(t.enabled);
        append_opt(p, t.cooldown_seconds);
        auto r = txn.exec_prepared("insert_metric_threshold", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        MetricThreshold out = t;
        out.id = r[0]["id"].as<int64_t>();
        out.created_at = string_to_timestamp(r[0]["created_at"].as<std::string>());
        out.updated_at = string_to_timestamp(r[0]["updated_at"].as<std::string>());
        return out;
    } catch (const std::exception& e) {
        std::cerr << "create_metric_threshold: " << e.what() << "\n";
        return std::nullopt;
    }
}

std::optional<MetricThreshold> SAdapter::get_metric_threshold_by_id(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec_prepared("get_metric_threshold_by_id", id);
        if (r.empty()) return std::nullopt;
        MetricThreshold t;
        t.id = r[0]["id"].as<int64_t>();
        t.instance_id = r[0]["instance_id"].as<std::string>();
        t.metric_name = r[0]["metric_name"].as<std::string>();
        t.source = r[0]["source"].as<std::string>();
        if (!r[0]["warning_threshold"].is_null()) t.warning_threshold = r[0]["warning_threshold"].as<double>();
        if (!r[0]["critical_threshold"].is_null()) t.critical_threshold = r[0]["critical_threshold"].as<double>();
        t.op = r[0]["operator"].as<std::string>();
        t.enabled = r[0]["enabled"].as<bool>();
        if (!r[0]["cooldown_seconds"].is_null()) t.cooldown_seconds = r[0]["cooldown_seconds"].as<int64_t>();
        t.created_at = string_to_timestamp(r[0]["created_at"].as<std::string>());
        t.updated_at = string_to_timestamp(r[0]["updated_at"].as<std::string>());
        return t;
    } catch (const std::exception& e) {
        std::cerr << "get_metric_threshold_by_id: " << e.what() << "\n";
        return std::nullopt;
    }
}

std::vector<MetricThreshold> SAdapter::get_metric_thresholds_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    std::vector<MetricThreshold> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string q = "SELECT * FROM metric_thresholds";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            MetricThreshold t;
            t.id = row["id"].as<int64_t>();
            t.instance_id = row["instance_id"].as<std::string>();
            t.metric_name = row["metric_name"].as<std::string>();
            t.source = row["source"].as<std::string>();
            if (!row["warning_threshold"].is_null()) t.warning_threshold = row["warning_threshold"].as<double>();
            if (!row["critical_threshold"].is_null()) t.critical_threshold = row["critical_threshold"].as<double>();
            t.op = row["operator"].as<std::string>();
            t.enabled = row["enabled"].as<bool>();
            if (!row["cooldown_seconds"].is_null()) t.cooldown_seconds = row["cooldown_seconds"].as<int64_t>();
            t.created_at = string_to_timestamp(row["created_at"].as<std::string>());
            t.updated_at = string_to_timestamp(row["updated_at"].as<std::string>());
            out.push_back(std::move(t));
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "get_metric_thresholds: " << e.what() << "\n";
    }
    return out;
}

bool SAdapter::update_metric_threshold(const MetricThreshold& t) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        pqxx::params p;
        p.append(t.instance_id);
        p.append(t.metric_name);
        p.append(t.source);
        append_opt(p, t.warning_threshold);
        append_opt(p, t.critical_threshold);
        p.append(t.op);
        p.append(t.enabled);
        append_opt(p, t.cooldown_seconds);
        p.append(t.id);
        auto r = txn.exec_prepared("update_metric_threshold", p);
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "update_metric_threshold: " << e.what() << "\n";
        return false;
    }
}

bool SAdapter::delete_metric_threshold(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec_prepared("delete_metric_threshold", id);
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "delete_metric_threshold: " << e.what() << "\n";
        return false;
    }
}

// ─── WeeklyMetricsSummary operations ──────────────────────────────────────────

std::optional<WeeklyMetricsSummary> SAdapter::create_weekly_metrics_summary(const WeeklyMetricsSummary& s) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return std::nullopt;
    try {
        pqxx::work txn(*pImpl_->conn);
        pqxx::params p;
        p.append(s.instance_id);
        p.append(timestamp_to_string(s.week_start));
        p.append(s.sample_count);

        append_opt(p, s.p50_latency_ms); append_opt(p, s.p95_latency_ms);
        append_opt(p, s.p99_latency_ms); append_opt(p, s.avg_latency_ms);
        append_opt(p, s.drop_rate); append_opt(p, s.packet_loss_rate);
        append_opt(p, s.msgs_sent); append_opt(p, s.msgs_received);
        append_opt(p, s.bytes_sent); append_opt(p, s.bytes_received);
        append_opt(p, s.cpu_usage); append_opt(p, s.memory_usage);
        append_opt(p, s.thread_count); append_opt(p, s.event_loop_lag_ms);
        append_opt(p, s.uptime_seconds);

        append_opt(p, s.exchange_p50_ms); append_opt(p, s.exchange_p95_ms); append_opt(p, s.exchange_p99_ms);
        append_opt(p, s.parsing_p50_ms); append_opt(p, s.parsing_p95_ms); append_opt(p, s.parsing_p99_ms);
        append_opt(p, s.normalization_p50_ms); append_opt(p, s.normalization_p95_ms); append_opt(p, s.normalization_p99_ms);
        append_opt(p, s.processing_p50_ms); append_opt(p, s.processing_p95_ms); append_opt(p, s.processing_p99_ms);
        append_opt(p, s.broadcast_p50_ms); append_opt(p, s.broadcast_p95_ms); append_opt(p, s.broadcast_p99_ms);
        append_opt(p, s.serialization_p50_ms); append_opt(p, s.serialization_p95_ms); append_opt(p, s.serialization_p99_ms);
        append_opt(p, s.socket_send_p50_ms); append_opt(p, s.socket_send_p95_ms); append_opt(p, s.socket_send_p99_ms);
        append_opt(p, s.latency_stats_jsonb);

        append_opt(p, s.messages_per_sec); append_opt(p, s.packets_per_sec); append_opt(p, s.bytes_per_sec);
        append_opt(p, s.ticks_per_sec); append_opt(p, s.trades_per_sec); append_opt(p, s.orderbook_updates_per_sec);
        append_opt(p, s.broadcasts_per_sec); append_opt(p, s.subscriptions_per_sec);
        append_opt(p, s.database_writes_per_sec); append_opt(p, s.database_reads_per_sec);

        append_opt(p, s.total_messages); append_opt(p, s.total_packets); append_opt(p, s.total_bytes);
        append_opt(p, s.total_ticks); append_opt(p, s.total_trades); append_opt(p, s.total_orderbook_updates);
        append_opt(p, s.total_broadcasts);

        append_opt(p, s.incoming_queue_depth); append_opt(p, s.outgoing_queue_depth); append_opt(p, s.serialization_queue_depth);
        append_opt(p, s.max_incoming_queue_depth); append_opt(p, s.max_outgoing_queue_depth); append_opt(p, s.max_serialization_queue_depth);
        append_opt(p, s.queue_overflow_count); append_opt(p, s.queue_wait_time_ms); append_opt(p, s.queue_processing_time_ms);
        append_opt_bool(p, s.queue_backpressure);

        append_opt(p, s.packet_drops); append_opt(p, s.duplicate_packets); append_opt(p, s.out_of_order_packets);
        append_opt(p, s.sequence_gaps); append_opt(p, s.missing_ticks); append_opt(p, s.invalid_messages);
        append_opt(p, s.corrupted_packets); append_opt(p, s.parse_failures);
        append_opt_bool(p, s.stale_feed); append_opt(p, s.feed_health_score); append_opt(p, s.feed_health_status);

        append_opt(p, s.active_clients); append_opt(p, s.active_sessions); append_opt(p, s.active_subscriptions);
        append_opt(p, s.total_connections); append_opt(p, s.total_disconnections); append_opt(p, s.reconnect_count);
        append_opt(p, s.authentication_failures);
        append_opt(p, s.avg_session_duration_ms); append_opt(p, s.longest_session_duration_ms);

        append_opt(p, s.tcp_reconnects); append_opt(p, s.socket_errors); append_opt(p, s.read_errors);
        append_opt(p, s.write_errors); append_opt(p, s.tls_handshake_failures);
        append_opt(p, s.network_bytes_transmitted); append_opt(p, s.network_bytes_received);
        append_opt(p, s.socket_rtt_ms); append_opt(p, s.network_bandwidth_bps);
        append_opt(p, s.network_connection_failures);

        append_opt(p, s.db_successful_writes); append_opt(p, s.db_failed_writes);
        append_opt(p, s.db_insert_latency_ms); append_opt(p, s.db_query_latency_ms);
        append_opt(p, s.db_active_connections); append_opt(p, s.db_connection_failures); append_opt(p, s.db_transaction_count);
        append_opt(p, s.db_writes_per_sec); append_opt(p, s.db_reads_per_sec); append_opt(p, s.db_queue_waiting);

        append_opt(p, s.peak_rss); append_opt(p, s.virtual_memory); append_opt(p, s.heap_usage);
        append_opt(p, s.memory_growth_rate);

        append_opt(p, s.total_subscriptions);
        append_opt(p, s.total_database_writes);
        append_opt(p, s.total_database_reads);

        auto r = txn.exec_prepared("insert_weekly_summary", p);
        txn.commit();
        if (r.empty()) return std::nullopt;
        WeeklyMetricsSummary out = s;
        out.id = r[0]["id"].as<int64_t>();
        return out;
    } catch (const std::exception& e) {
        std::cerr << "create_weekly_metrics_summary: " << e.what() << "\n";
        return std::nullopt;
    }
}

std::vector<WeeklyMetricsSummary> SAdapter::get_weekly_summaries_by_condition(const std::string& where_clause) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    std::vector<WeeklyMetricsSummary> out;
    if (!is_connected()) return out;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string q = "SELECT * FROM weekly_metrics_summary";
        if (!where_clause.empty()) q += " WHERE " + where_clause;
        auto r = txn.exec(q);
        for (const auto& row : r) {
            WeeklyMetricsSummary s;
            s.id = row["id"].as<int64_t>();
            s.instance_id = row["instance_id"].as<std::string>();
            s.week_start = string_to_timestamp(row["week_start"].as<std::string>());
            s.sample_count = row["sample_count"].as<int64_t>();

            auto read_double = [&](const char* col) -> std::optional<double> {
                return row[col].is_null() ? std::nullopt : std::optional<double>(row[col].as<double>());
            };
            auto read_int64 = [&](const char* col) -> std::optional<int64_t> {
                return row[col].is_null() ? std::nullopt : std::optional<int64_t>(row[col].as<int64_t>());
            };

            s.p50_latency_ms = read_double("p50_latency_ms");
            s.p95_latency_ms = read_double("p95_latency_ms");
            s.p99_latency_ms = read_double("p99_latency_ms");
            s.avg_latency_ms = read_double("avg_latency_ms");
            s.cpu_usage = read_double("cpu_usage");
            s.memory_usage = read_int64("memory_usage");
            s.thread_count = read_int64("thread_count");
            s.uptime_seconds = read_int64("uptime_seconds");

            s.exchange_p50_ms = read_double("exchange_p50_ms");
            s.exchange_p95_ms = read_double("exchange_p95_ms");
            s.exchange_p99_ms = read_double("exchange_p99_ms");
            s.parsing_p50_ms = read_double("parsing_p50_ms");
            s.parsing_p95_ms = read_double("parsing_p95_ms");
            s.parsing_p99_ms = read_double("parsing_p99_ms");

            // Read remaining latency fields
            s.normalization_p50_ms = read_double("normalization_p50_ms");
            s.normalization_p95_ms = read_double("normalization_p95_ms");
            s.normalization_p99_ms = read_double("normalization_p99_ms");
            s.processing_p50_ms = read_double("processing_p50_ms");
            s.processing_p95_ms = read_double("processing_p95_ms");
            s.processing_p99_ms = read_double("processing_p99_ms");
            s.broadcast_p50_ms = read_double("broadcast_p50_ms");
            s.broadcast_p95_ms = read_double("broadcast_p95_ms");
            s.broadcast_p99_ms = read_double("broadcast_p99_ms");
            s.serialization_p50_ms = read_double("serialization_p50_ms");
            s.serialization_p95_ms = read_double("serialization_p95_ms");
            s.serialization_p99_ms = read_double("serialization_p99_ms");
            s.socket_send_p50_ms = read_double("socket_send_p50_ms");
            s.socket_send_p95_ms = read_double("socket_send_p95_ms");
            s.socket_send_p99_ms = read_double("socket_send_p99_ms");

            if (!row["latency_stats_jsonb"].is_null())
                s.latency_stats_jsonb = row["latency_stats_jsonb"].as<std::string>();
            s.messages_per_sec = read_double("messages_per_sec");
            s.ticks_per_sec = read_double("ticks_per_sec");
            s.trades_per_sec = read_double("trades_per_sec");
            s.cpu_usage = read_double("cpu_usage");

            out.push_back(std::move(s));
        }
        txn.commit();
    } catch (const std::exception& e) {
        std::cerr << "get_weekly_summaries: " << e.what() << "\n";
    }
    return out;
}

bool SAdapter::delete_weekly_metrics_summary(int64_t id) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return false;
    try {
        pqxx::work txn(*pImpl_->conn);
        auto r = txn.exec_prepared("delete_weekly_summary", id);
        txn.commit();
        return r.affected_rows() > 0;
    } catch (const std::exception& e) {
        std::cerr << "delete_weekly_metrics_summary: " << e.what() << "\n";
        return false;
    }
}

int64_t SAdapter::delete_old_snapshots(uint64_t before_epoch_ms) {
    std::lock_guard<std::recursive_mutex> lock(pImpl_->mutex);
    if (!is_connected()) return 0;
    try {
        pqxx::work txn(*pImpl_->conn);
        std::string ts = timestamp_to_string(before_epoch_ms);
        int64_t total = 0;

        auto r1 = txn.exec("DELETE FROM feed_metrics_snapshots WHERE measured_at < '" + ts + "'::timestamptz");
        total += r1.affected_rows();

        auto r2 = txn.exec("DELETE FROM exchange_metrics_history WHERE snapshot_time < '" + ts + "'::timestamptz");
        total += r2.affected_rows();

        auto r3 = txn.exec("DELETE FROM queue_history WHERE measured_at < '" + ts + "'::timestamptz");
        total += r3.affected_rows();

        auto r4 = txn.exec("DELETE FROM system_metrics_history WHERE measured_at < '" + ts + "'::timestamptz");
        total += r4.affected_rows();

        auto r5 = txn.exec("DELETE FROM network_metrics_history WHERE measured_at < '" + ts + "'::timestamptz");
        total += r5.affected_rows();

        auto r6 = txn.exec("DELETE FROM database_metrics_history WHERE measured_at < '" + ts + "'::timestamptz");
        total += r6.affected_rows();

        txn.commit();
        return total;
    } catch (const std::exception& e) {
        std::cerr << "delete_old_snapshots: " << e.what() << "\n";
        return 0;
    }
}

} // namespace datafeed
