#include "ConfigService.hpp"
#include <chrono>

namespace api {
namespace services {

ConfigService::ConfigService(std::shared_ptr<datafeed::SAdapter> sadapter)
    : sadapter_(sadapter) {}

nlohmann::json ConfigService::getConfig()
{
    nlohmann::json cfg;
    cfg["version"] = "1.0.0";
    cfg["adapter_version"] = "1.0.0";
    cfg["deployment_id"] = "live-1";
    cfg["schema_version"] = 1;

    if (sadapter_ && sadapter_->is_connected()) {
        auto versions = sadapter_->get_config_versions_by_condition(
            "ORDER BY applied_at DESC LIMIT 1");
        if (!versions.empty()) {
            const auto& v = versions[0];
            cfg["config_version"] = v.config_version;
            cfg["build_sha"] = v.build_sha;
            cfg["adapter_version"] = v.adapter_version;
            cfg["deployment_id"] = v.deployment_id;
            cfg["feature_flags"] = v.feature_flags.value_or("{}");
            cfg["schema_version"] = v.schema_version;
            cfg["applied_at"] = v.applied_at;
        }
    }

    cfg["runtime"] = nlohmann::json::object();
    cfg["runtime"]["database"] = sadapter_ && sadapter_->is_connected() ? "connected" : "disconnected";

    return cfg;
}

nlohmann::json ConfigService::updateConfig(const nlohmann::json& config)
{
    return {{"status", "not_implemented"}, {"message", "Runtime config updates are not yet supported"}};
}

nlohmann::json ConfigService::getThresholds()
{
    nlohmann::json arr = nlohmann::json::array();
    if (!sadapter_ || !sadapter_->is_connected()) return arr;

    auto thresholds = sadapter_->get_metric_thresholds_by_condition(
        "ORDER BY metric_name ASC");
    for (const auto& t : thresholds) {
        nlohmann::json j;
        j["id"] = t.id;
        j["instance_id"] = t.instance_id;
        j["metric_name"] = t.metric_name;
        j["source"] = t.source;
        j["warning_threshold"] = t.warning_threshold;
        j["critical_threshold"] = t.critical_threshold;
        j["op"] = t.op;
        j["enabled"] = t.enabled;
        j["cooldown_seconds"] = t.cooldown_seconds;
        j["created_at"] = t.created_at;
        j["updated_at"] = t.updated_at;
        arr.push_back(j);
    }
    return arr;
}

nlohmann::json ConfigService::updateThresholds(const nlohmann::json& thresholds)
{
    if (!sadapter_ || !sadapter_->is_connected()) {
        return {{"error", "database not connected"}};
    }

    if (!thresholds.is_array()) {
        return {{"error", "expected array of threshold updates"}};
    }

    int updated = 0;
    for (const auto& item : thresholds) {
        if (!item.contains("id")) continue;
        int64_t id = item["id"].get<int64_t>();
        auto existing = sadapter_->get_metric_threshold_by_id(id);
        if (!existing) continue;

        if (item.contains("warning_threshold") && !item["warning_threshold"].is_null())
            existing->warning_threshold = item["warning_threshold"].get<double>();
        if (item.contains("critical_threshold") && !item["critical_threshold"].is_null())
            existing->critical_threshold = item["critical_threshold"].get<double>();
        if (item.contains("enabled"))
            existing->enabled = item["enabled"].get<bool>();
        if (item.contains("op"))
            existing->op = item["op"].get<std::string>();

        if (sadapter_->update_metric_threshold(*existing)) {
            updated++;
        }
    }

    return {{"status", "ok"}, {"updated", updated}};
}

} // namespace services
} // namespace api
