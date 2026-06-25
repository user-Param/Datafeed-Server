# Implementation Plan for the Datafeed REST API

## Overview
This document outlines the steps to complete the REST API layer on top of the existing datafeed service. The API follows a Clean Architecture pattern with the following layers:
- **Router**: Handles HTTP request routing to controllers.
- **Controllers**: Handle HTTP requests and responses, delegating business logic to services.
- **Services**: Contain business logic and interact with the SAdapter (data access layer).
- **DTOs**: Data Transfer Objects for request/response payloads.
- **Middleware**: Handles cross-cutting concerns like authentication, logging, etc.

## Completed Components
1. **Router**: Basic router implementation in `api/router/` that can register routes and handle requests using regex-based path matching.
2. **Feed Module**:
   - DTO: `api/dto/FeedStatusResponse.hpp`
   - Service: `api/services/FeedService.hpp` and `.cpp` (uses SAdapter to get feed instance status)
   - Controller: `api/controllers/FeedController.hpp` and `.cpp` (handles GET `/api/v1/feed/status`)
3. **Client Module**:
   - DTO: `api/dto/ClientResponseDto.hpp`
   - Service: `api/services/ClientService.hpp` and `.cpp` (CRUD operations for clients)
   - Controller: To be implemented (similar pattern to FeedController)

## Steps to Complete the Implementation

### 1. Set Up Include Paths and Dependencies
Ensure the following directories are in the include path for compilation:
- `api/`
- `api/dto/`
- `api/services/`
- `api/controllers/`
- `api/middleware/`
- `api/router/`
- Existing project directories (for SAdapter, Boost, nlohmann_json, etc.)

Required libraries:
- Boost.Beast (for HTTP)
- nlohmann_json (for JSON serialization)
- PostgreSQL library (libpq) for SAdapter

### 2. Implement Remaining Modules
For each domain (sessions, subscriptions, exchanges, metrics, events, requests, backtests, config, health, admin), follow this pattern:

#### a. DTOs
Create DTOs in `api/dto/` that represent the API request/response payloads.
- Use `std::optional` for optional fields.
- Map closely to the SAdapter structs but exclude internal fields if needed.
- Example: `SessionResponseDto`, `SubscriptionRequestDto`, etc.

#### b. Services
Create service classes in `api/services/` that encapsulate business logic.
- Inject a `std::shared_ptr<datafeed::SAdapter>` via constructor.
- Implement methods corresponding to API endpoints (e.g., `getSessionById`, `createSubscription`, `updateExchangeHealth`).
- Use the SAdapter methods to perform database operations.
- Convert between SAdapter structs and DTOs.
- Handle errors and return appropriate types (e.g., `std::optional<T>` for single items, `std::vector<T>` for collections, `bool` for success/failure).

#### c. Controllers
Create controller classes in `api/controllers/` that handle HTTP requests.
- Inject a `std::shared_ptr<Service>` via constructor.
- Implement handler methods for each endpoint (e.g., `handleGetSessions`, `handlePostSubscription`).
- Extract path parameters, query parameters, and request body using Boost.Beast.
- Call the appropriate service method.
- Serialize DTOs to JSON using nlohmann_json for the response body.
- Set appropriate HTTP status codes (200 OK, 201 Created, 400 Bad Request, 404 Not Found, 500 Internal Server Error).
- Set `Content-Type: application/json` header.

#### d. Register Routes
In the main server setup (e.g., `server.cpp`), create an instance of the router and register all routes.

Example for a GET endpoint:
```cpp
router->add_route("GET", "/api/v1/sessions/{session_id}",
    [sessionService](const auto& req, const auto& match) {
        // Extract session_id from match[1]
        // Call sessionService->getSessionById
        // Build and return HTTP response
    });
```

### 3. Integrate Router into Existing Server
Modify `server.cpp` to:
1. Initialize the SAdapter and other dependencies (session manager, live source, etc.) as before.
2. Create service instances, passing the shared SAdapter.
3. Create controller instances, passing the service shared pointers.
4. Create the router and register all routes, capturing the necessary dependencies in lambdas.
5. In `http_session::on_read`, after checking for WebSocket upgrade, check if the request target starts with `/api/v1`. If so, route it to the router; otherwise, proceed with existing WebSocket logic or return 404.

Example snippet for `http_session::on_read`:
```cpp
if (websocket::is_upgrade(parser_.get())) {
    // Existing WebSocket upgrade logic
} else if (req.target().starts_with("/api/v1")) {
    auto res = g_router->handle_request(req);
    if (res) {
        http::async_write(stream_, *res,
            beast::bind_front_handler(&http_session::on_write, shared_from_this()));
    } else {
        // 404 Not Found
        http::response<http::string_body> res{http::status::not_found, req.version()};
        res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(http::field::content_type, "text/plain");
        res.keep_alive(req.keep_alive());
        res.body() = "Not Found";
        res.prepare_payload();
        http::async_write(stream_, res,
            beast::bind_front_handler(&http_session::on_write, shared_from_this()));
    }
} else {
    // Not found
    // ... same as above
}
```

### 4. Implement Middleware (Optional but Recommended)
Create middleware components for:
- **Authentication**: Validate API keys or JWT tokens.
- **Logging**: Log request/response details.
- **Rate Limiting**: Prevent abuse.
- **CORS**: Handle Cross-Origin Resource Sharing if needed.

Middleware can be implemented as wrappers around route handlers or as pre-processing steps in the router.

### 5. Error Handling and Validation
- Validate input parameters (e.g., UUID format, numeric ranges).
- Return 400 Bad Request for invalid input.
- Catch exceptions from services and return 500 Internal Server Error with a generic error message (log message (in development) or without (in production).
- Consider using a global error handler in the router.

### 6. Testing
- Write unit tests for services using a mock SAdapter.
- Write integration tests for controllers using a test server.
- Test edge cases and error conditions.

## API Endpoints Summary
Based on the provided specification, the following endpoints should be implemented:

### Feed Control
- GET `/api/v1/feed/status`
- GET `/api/v1/feed/topology`
- GET `/api/v1/feed/instances`
- POST `/api/v1/feed/start`
- POST `/api/v1/feed/stop`
- POST `/api/v1/feed/restart`
- POST `/api/v1/feed/reconnect`
- POST `/api/v1/feed/switch-exchange`

### Client API
- GET `/api/v1/clients`
- GET `/api/v1/clients/{client_id}`
- POST `/api/v1/clients`
- PATCH `/api/v1/clients/{client_id}`
- DELETE `/api/v1/clients/{client_id}`

### Session API
- GET `/api/v1/sessions`
- GET `/api/v1/sessions/{session_id}`
- GET `/api/v1/sessions/active`
- GET `/api/v1/sessions/by-client/{client_id}`
- DELETE `/api/v1/sessions/{session_id}`
- POST `/api/v1/sessions/{session_id}/disconnect`

### Subscription API
- GET `/api/v1/subscriptions`
- GET `/api/v1/subscriptions/{subscription_id}`
- GET `/api/v1/subscriptions/by-session/{session_id}`
- GET `/api/v1/subscriptions/by-symbol/{symbol}`
- POST `/api/v1/subscriptions`
- PATCH `/api/v1/subscriptions/{subscription_id}`
- DELETE `/api/v1/subscriptions/{subscription_id}`

### Exchange API
- GET `/api/v1/exchanges`
- GET `/api/v1/exchanges/health`
- GET `/api/v1/exchanges/{exchange}`
- GET `/api/v1/exchanges/{exchange}/latency`
- GET `/api/v1/exchanges/{exchange}/stats`
- POST `/api/v1/exchanges/{exchange}/reconnect`
- POST `/api/v1/exchanges/{exchange}/disable`
- POST `/api/v1/exchanges/{exchange}/enable`

### Feed Instance API
- GET `/api/v1/instances`
- GET `/api/v1/instances/{instance_id}`
- PATCH `/api/v1/instances/{instance_id}`
- DELETE `/api/v1/instances/{instance_id}`

### Metrics API
- GET `/api/v1/metrics`
- GET `/api/v1/metrics/latest`
- GET `/api/v1/metrics/latency`
- GET `/api/v1/metrics/throughput`
- GET `/api/v1/metrics/connections`
- GET `/api/v1/metrics/cpu`
- GET `/api/v1/metrics/memory`
- GET `/api/v1/metrics/history` (with query params: hours, instance, exchange)

### Event API
- GET `/api/v1/events`
- GET `/api/v1/events/recent`
- GET `/api/v1/events/errors`
- GET `/api/v1/events/{event_id}`
- GET `/api/v1/events/by-trace/{trace_id}`
- GET `/api/v1/events/by-correlation/{correlation_id}`
- GET `/api/v1/events?action=SUBSCRIBE`
- GET `/api/v1/events?result=FAILURE`

### API Analytics (Requests)
- GET `/api/v1/requests`
- GET `/api/v1/requests/stats`
- GET `/api/v1/requests/top-endpoints`
- GET `/api/v1/requests/latency`
- GET `/api/v1/requests/errors`

### Backtest API
- POST `/api/v1/backtests`
- GET `/api/v1/backtests`
- GET `/api/v1/backtests/{job_id}`
- DELETE `/api/v1/backtests/{job_id}`
- POST `/api/v1/backtests/{job_id}/pause`
- POST `/api/v1/backtests/{job_id}/resume`
- POST `/api/v1/backsets/{job_id}/cancel`

### Config API
- GET `/api/v1/config`
- GET `/api/v1/config/version`
- GET `/api/v1/config/flags`
- POST `/api/v1/config/reload`
- POST `/api/v1/config/feature-flags`

### Health API
- GET `/api/v1/health`
- GET `/api/v1/health/live`
- GET `/api/v1/health/ready`
- GET `/api/v1/health/database`
- GET `/api/v1/health/exchanges`
- GET `/api/v1/health/system`

### Admin API
- GET `/api/v1/admin/stats`
- GET `/api/v1/admin/audit`
- POST `/api/v1/admin/clear-events`
- POST `/api/v1/admin/clear-metrics`
- POST `/api/v1/admin/reload-config`
- POST `/api/v1/admin/shutdown`

## Data Flow Example
1. Client sends HTTP GET request to `/api/v1/feed/status`.
2. Router matches the route and calls the handler in `FeedController`.
3. `FeedController` extracts any path/query parameters (none in this case).
4. `FeedController` calls `FeedService::getFeedStatus()`.
5. `FeedService` uses the SAdapter to query the `feed_instances` table for a running instance.
6. `FeedService` maps the SAdapter `FeedInstance` struct to `FeedStatusResponse` DTO.
7. `FeedController` serializes the DTO to JSON and returns an HTTP 200 response.

## Security Considerations
- Implement authentication (API keys, JWT) in middleware.
- Use HTTPS in production.
- Validate and sanitize all inputs.
- Implement rate limiting to prevent abuse.
- Log requests and responses for audit trails.

## Performance Considerations
- Use connection pooling for database connections (SAdapter already uses a connection per instance? Consider sharing a connection pool).
- Cache frequently accessed data if appropriate (e.g., configuration).
- Use asynchronous database operations if the SAdapter supports it (currently synchronous).
- Compress HTTP responses (e.g., gzip) for large payloads.

## Conclusion
This plan provides a roadmap to complete the REST API layer. By following the modular structure and implementing each domain consistently, the API will be maintainable, scalable, and aligned with the existing system.