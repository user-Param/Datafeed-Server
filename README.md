Live on - https://datafeed.fun


# Datafeed Server

A high-performance, low-latency market data feed server supporting multiple cryptocurrency exchanges with WebSocket and HTTP interfaces.

## Overview

The Datafeed Server is designed to aggregate real-time market data from various cryptocurrency exchanges (Binance, Jupiter, Birdeye) and distribute it to connected clients via WebSocket subscriptions. The server features:

- **Multi-exchange support**: Connect to Binance, Jupiter (Solana), and Birdeye exchanges
- **WebSocket API**: Real-time data streaming with topic-based subscriptions
- **HTTP API**: REST endpoints for configuration and monitoring
- **PostgreSQL persistence**: Comprehensive logging and metrics storage
- **Horizontal scalability**: Designed for multiple instances behind a load balancer
- **Comprehensive monitoring**: Built-in metrics, health checks, and audit trails
- **Dockerized deployment**: Easy containerized deployment with docker-compose

## Features

### Core Functionality
- Real-time ticker data from multiple exchanges
- Topic-based WebSocket subscriptions (ticker_, price_, bid_, ask_, etc.)
- Live and backtest data modes
- Exchange failover and reconnection handling
- Message rate limiting and backpressure management

### Monitoring & Observability
- Real-time performance metrics (latency, throughput, error rates)
- Per-exchange health monitoring
- Detailed audit logging of all actions
- API request logging with latency tracking
- Prometheus-compatible metrics endpoint (planned)

### Administration
- Runtime exchange switching without downtime
- Dynamic symbol subscription management
- Configuration via environment variables or config files
- Comprehensive PostgreSQL schema for persistence
- Health check endpoints for load balancer integration

### Architecture
- Modular design with separate exchange adapters
- Session management for tracking client connections
- Event-driven architecture using Boost.Asio and Boost.Beast
- Thread-safe data structures for high concurrency
- Zero-copy message passing where possible

## Getting Started

### Prerequisites
- Docker and Docker Compose (for containerized deployment)
- OR:
  - C++17 compiler (GCC 9+ or Clang 10+)
  - CMake 3.10+
  - Boost 1.74+
  - OpenSSL 1.1.1+
  - PostgreSQL 12+
  - nlohmann_json 3.0+
  - libpqxx 7.0+

### Quick Start with Docker

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd datafeed
   ```

2. Build and start the services:
   ```bash
   docker compose up --build
   ```

3. The server will be available at:
   - WebSocket: `ws://localhost:4444`
   - HTTP health check: `http://localhost:4444/health` (if implemented)

### Manual Build & Run

1. Install dependencies:
   ```bash
   # Ubuntu/Debian
   sudo apt-get install build-essential cmake libboost-all-dev libssl-dev libpq-dev libnlohmann-json-dev
   
   # macOS (using Homebrew)
   brew install boost openssl postgresql nlohmann-json libpqxx
   ```

2. Build the project:
   ```bash
   mkdir -p build && cd build
   cmake ..
   make -j$(nproc)
   ```

3. Run the server:
   ```bash
   ./datafeed 0.0.0.0 4444 1
   ```
   Arguments: `<address> <port> <thread-count>`

4. Ensure PostgreSQL is running and configured:
   ```bash
   # Initialize database
   psql -h localhost -U postgres -f init/schema.sql
   psql -h localhost -U postgres -f init/seed.sql
   ```

## Configuration

The server can be configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `EXCHANGE` | Default exchange to connect to (BINANCE, JUPITER, BIRDEYE) | BINANCE |
| `PORT` | Server port | 4444 |
| `ADDRESS` | Bind address | 0.0.0.0 |
| `THREADS` | Number of I/O threads | 1 |
| `DATABASE_URL` | PostgreSQL connection string | postgres://localhost/datafeed |
| `LOG_LEVEL` | Logging level (trace, debug, info, warn, error) | info |
| `JUPITER_API_KEY` | API key for Jupiter exchange (optional) | (none) |
| `ENABLE_METRICS` | Enable Prometheus metrics endpoint | false |

Example `.env` file:
```env
EXCHANGE=BINANCE
PORT=4444
ADDRESS=0.0.0.0
THREADS=2
DATABASE_URL=postgres://user:password@localhost:5432/datafeed
LOG_LEVEL=info
ENABLE_METRICS=true
```

## WebSocket API

### Connection
Connect to `ws://<host>:<port>` using any WebSocket client.

### Message Format
All messages are JSON objects.

#### Client-to-Server (Commands)
```json
{
  "action": "subscribe",
  "topic": "ticker_"
}
```

Available actions:
- `subscribe`: Subscribe to one or more topics
- `unsubscribe`: Unsubscribe from topics
- `switch_exchange`: Change the exchange being used (requires symbols)
- `set_mode`: Switch between live and backtest modes

#### Server-to-Client (Data & Events)
```json
{
  "topic": "ticker_",
  "symbol": "BTCUSDT",
  "price": 43250.12,
  "bid": 43249.88,
  "ask": 43250.36,
  "timestamp": 1640995200000
}
```

Topics:
- `ticker_`: Latest price, bid, ask for a symbol
- `price_`: Price updates only
- `bid_`: Bid price updates
- `ask_`: Ask price updates
- `trade_`: Individual trades (if available from exchange)
- `orderbook_`: Order book updates (if available)

### Subscription Examples

Subscribe to ticker updates for BTC and ETH:
```json
{"action": "subscribe", "topic": "ticker_", "symbols": ["BTCUSDT", "ETHUSDT"]}
```

Subscribe to all available topics:
```json
{"action": "subscribe", "topic": "all_"}
```

Switch to Jupiter exchange with Solana symbols:
```json
{
  "action": "switch_exchange",
  "exchange": "JUPITER",
  "symbols": ["SOL", "BTC", "ETH"]
}
```

## HTTP API (Planned)

The following endpoints are planned for future implementation:

- `GET /health` - Health check endpoint
- `GET /metrics` - Prometheus metrics
- `GET /instances` - List active feed instances
- `GET /clients/{id}` - Get client information
- `POST /clients` - Create new client
- `GET /subscriptions` - List active subscriptions
- `POST /subscriptions` - Create new subscription
- `DELETE /subscriptions/{id}` - Delete subscription
- `GET /events` - Query audit events
- `GET /exchange-health` - Get health of all exchanges

## Database Schema

The server uses PostgreSQL for persistence. See `init/schema.sql` for the complete schema.

Key tables:
- `clients` - Tenant/client information
- `sessions` - WebSocket/HTTP session tracking
- `subscriptions` - Client subscriptions to market data
- `feed_instances` - Runtime feed health per instance
- `feed_metrics_snapshots` - Time-series performance metrics
- `feed_events` - Audit/action log
- `api_requests` - HTTP request logging
- `exchange_health` - Per-exchange health monitoring
- `backtest_jobs` - Backtest/replay job tracking
- `config_versions` - Configuration and version tracking

## Monitoring and Metrics

The server exposes several mechanisms for monitoring:

### Built-in Logging
- Structured logging with severity levels
- Console and file output options
- JSON log format available

### Database Metrics
- Feed instance health stored in `feed_instances` table
- Time-series metrics in `feed_metrics_snapshots` table
- Error counts and rates in various tables
- Audit trail in `feed_events`

### Health Checks
- TCP connection health (port 4444)
- Exchange connectivity status
- Database connection status
- Memory and CPU usage (via external monitoring)

### Planned Prometheus Endpoint
When enabled (`ENABLE_METRICS=true`), the server will expose:
- `datafeed_messages_sent_total`
- `datafeed_messages_received_total`
- `datafeed_latency_milliseconds` (histogram)
- `datafeed_exchange_connection_status` (gauge)
- `datafeed_subscriptions_active` (gauge)
- `datafeed_cpu_usage_ratio`
- `datafeed_memory_usage_bytes`

## Development

### Building for Development
```bash
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug ..
make -j$(nproc)
```

### Running Tests
```bash
# Unit tests (when implemented)
ctest --output-on-failure

# Stress test
python3 stress_test.py --clients 50 --duration 60
```

### Code Style
- Follows Google C++ Style Guide with exceptions
- clang-format configuration in `.clang-format`
- Lint with `clang-tidy`

## Deployment

### Production Docker Deployment
```yaml
# docker-compose.prod.yml
version: '3.8'

services:
  datafeed:
    build: .
    ports:
      - "4444:4444"
    environment:
      - EXCHANGE=BINANCE
      - DATABASE_URL=postgres://user:password@postgres:5432/datafeed
      - LOG_LEVEL=info
    depends_on:
      - postgres
    restart: unless-stopped

  postgres:
    image: postgres:14
    environment:
      - POSTGRES_DB=datafeed
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=password
    volumes:
      - postgres_data:/var/lib/postgresql/data
    restart: unless-stopped

volumes:
  postgres_data:
```

### Kubernetes Deployment
See `k8s/` directory for example manifests (when available).

### Scaling
- Run multiple instances behind a TCP load balancer (HAProxy, NGINX, cloud LB)
- Use sticky sessions or Redis-backed session store if needed
- Each instance maintains its own exchange connections
- Consider sharding by symbol or client for very large deployments

## Testing

### Stress Testing
The included `stress_test.py` script can simulate multiple clients:
```bash
python3 stress_test.py --help
```

Example: 100 clients for 2 minutes
```bash
python3 stress_test.py --clients 100 --duration 120
```

### Manual Testing
1. Start the server
2. Use `wscat` or similar WebSocket client:
   ```bash
   wscat -c ws://localhost:4444
   ```
3. Send subscription commands:
   ```
   > {"action": "subscribe", "topic": "ticker_"}
   < {"topic":"ticker_","symbol":"BTCUSDT","price":43250.12,"bid":43249.88,"ask":43250.36,"timestamp":1640995200000}
   ```

## Troubleshooting

### Common Issues

#### Connection Problems
- Verify server is running: `docker compose ps` or `ps aux | grep datafeed`
- Check port binding: `netstat -tlnp | grep 4444`
- Review logs: `docker compose logs datafeed` or `cat server.log`

#### Database Issues
- Ensure PostgreSQL is running and accessible
- Verify connection string in `DATABASE_URL`
- Check that schema has been applied: `psql -c "\dt"` in datafeed database
- Look for errors in server startup logs

#### No Data Received
- Confirm exchange connection: Look for "[Exchange1] Connected to Binance WebSocket" in logs
- Check subscription was processed: Look for "[EAdapter] Subscribed to 3 symbols"
- Verify topic matching in client subscription
- Check firewall/network restrictions to exchange endpoints

#### High Latency
- Check system resources (CPU, memory, network)
- Verify exchange WebSocket connection stability
- Look for garbage collection pauses or thread contention
- Consider increasing thread count if CPU-bound

## Performance Benchmarks
- Monitor database write performance if logging is enabled

## Support

For issues, questions, or contributions:
1. Check existing issues in the repository
2. Submit a new issue with detailed reproduction steps
3. For security concerns, contact security@yourcompany.com

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Boost.Asio and Boost.Beast for networking
- nlohmann_json for JSON parsing
- libpqxx for PostgreSQL C++ interface
- The cryptocurrency exchange APIs (Binance, Jupiter, Birdeye) for providing market data
