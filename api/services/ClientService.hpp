#ifndef API_SERVICES_CLIENT_SERVICE_HPP
#define API_SERVICES_CLIENT_SERVICE_HPP

#include <memory>
#include <vector>
#include <optional>
#include <string>
#include "../../sadapter.h"
#include "../dto/ClientResponseDto.hpp"

namespace api {
namespace services {

class ClientService {
public:
    explicit ClientService(std::shared_ptr<datafeed::SAdapter> adapter);

    // Get a client by tenant_id
    std::optional<dto::ClientResponseDto> getClientById(const std::string& tenant_id);

    // Get all clients
    std::vector<dto::ClientResponseDto> getClients();

    // Create a client
    std::optional<dto::ClientResponseDto> createClient(const dto::ClientResponseDto& client);

    // Update a client
    bool updateClient(const dto::ClientResponseDto& client);

    // Delete a client
    bool deleteClient(const std::string& tenant_id);

private:
    std::shared_ptr<datafeed::SAdapter> adapter_;

    // Helper to convert from datafeed::Client to dto::ClientResponseDto
    dto::ClientResponseDto toDto(const datafeed::Client& client);
};

} // namespace services
} // namespace api

#endif // API_SERVICES_CLIENT_SERVICE_HPP