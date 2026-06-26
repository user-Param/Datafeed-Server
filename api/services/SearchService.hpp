#ifndef API_SERVICES_SEARCH_SERVICE_HPP
#define API_SERVICES_SEARCH_SERVICE_HPP

#include <memory>
#include <string>
#include <nlohmann/json.hpp>
#include "../../sadapter.h"

namespace api {
namespace services {

class SearchService {
public:
    explicit SearchService(std::shared_ptr<datafeed::SAdapter> sadapter);

    nlohmann::json search(const std::string& query);

private:
    std::shared_ptr<datafeed::SAdapter> sadapter_;
};

} // namespace services
} // namespace api

#endif
