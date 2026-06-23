# Datafeed Server Stress Test

This directory contains a stress test script for evaluating the scalability and performance of the datafeed server.

## Overview

The stress test (`stress_test.py`) creates multiple WebSocket client connections to the datafeed server, subscribes them to topics, and measures:
- Connection establishment rate
- Message throughput
- Latency statistics
- Error rates

## Prerequisites

1. Python 3.7+
2. websockets library (`pip install websockets`)
3. Built datafeed server (see build instructions below)

## Building the Server

```bash
# From the project root directory
mkdir -p build
cd build
cmake ..
make
```

This will produce three executables:
- `eadapter`: Exchange adapter (WebSocket client that connects to exchanges)
- `datafeed`: Main server application (WebSocket server for clients)

## Running the Stress Test

### Step 1: Start the Server Components

In separate terminal windows:

**Terminal 1: Start the exchange adapter**
```bash
./eadapter
```

**Terminal 2: Start the datafeed server**
```bash
./datafeed 0.0.0.0 8080 4
```
This starts the server on port 8080 with 4 worker threads.

### Step 2: Run the Stress Test

```bash
python3 stress_test.py --host localhost --port 8080 --clients 100 --duration 30
```

### Command Line Arguments

- `--host`: Server hostname (default: localhost)
- `--port`: Server port (default: 8080)
- `--clients`: Number of concurrent WebSocket clients (default: 100)
- `--duration`: Test duration in seconds (default: 30)
- `--topics`: Topics to subscribe to (default: price_ bid_ ask_)
- `--ramp-up`: Client connection ramp-up time in seconds (default: 10)
- `--output`: Save report to file (optional)

## Understanding the Results

The stress test generates a detailed report including:

### Connection Statistics
- Total clients attempted vs. successfully connected
- Connection success rate
- Connection establishment rate (clients/sec)

### Message Statistics
- Total messages sent/received
- Message throughput (msg/sec)
- Average messages per client

### Latency Statistics (if available)
- Min, max, mean, median latency
- Standard deviation
- Percentiles (50th, 90th, 95th, 99th)

### Performance Assessment
- Recommendations based on observed performance
- Identification of potential bottlenecks

## Example Output

```
============================================================
DATAFEED SERVER STRESS TEST REPORT
============================================================
Test Start Time: 2026-06-23 10:30:00
Test End Time: 2026-06-23 10:30:30
Test Duration: 30.00 seconds

CONNECTION STATISTICS:
  Total Clients Attempted: 100
  Successfully Connected: 98
  Connection Errors: 2
  Connection Success Rate: 98.00%

MESSAGE STATISTICS:
  Total Messages Sent: 0
  Total Messages Received: 2450
  Message Send Rate: 0.00 msg/sec
  Message Receive Rate: 81.67 msg/sec
  Avg Messages Sent per Client: 0.00
  Avg Messages Received per Client: 25.00

LATENCY STATISTICS (milliseconds):
  Latency Samples: 2450
  Min Latency: 1.23 ms
  Max Latency: 45.67 ms
  Mean Latency: 12.34 ms
  Median Latency: 10.56 ms
  Latency Std Dev: 8.45 ms
  50th Percentile (Median): 10.56 ms
  90th Percentile: 25.67 ms
  95th Percentile: 32.45 ms
  99th Percentile: 41.23 ms

PERFORMANCE ASSESSMENT:
  Connection Establishment Rate: 3.27 clients/sec
  Message Throughput per Client: 0.83 msg/client/sec
  Overall Message Throughput: 81.67 msg/sec

RECOMMENDATIONS:
  ✅ Connection error rate is acceptable (<2%)
  ✅ Latency is excellent (<50ms average)
============================================================
```

## Notes

1. The stress test measures client-to-server communication. To test end-to-end performance (including exchange data fetching), you would need to have the eadapter connected to actual exchange data sources.

2. For realistic testing, ensure you have:
   - Sufficient system resources (file descriptors, memory)
   - Appropriate ulimit settings for open files
   - Network capacity to handle the expected load

3. The test uses WebSocket connections, which are more resource-intensive than HTTP connections. Adjust the number of clients based on your server's capabilities.

4. To test higher loads, consider:
   - Increasing server thread count when starting the datafeed server
   - Running multiple server instances behind a load balancer
   - Optimizing system limits (ulimit -n)

## Troubleshooting

### "Too many open files" error
Increase the file descriptor limit:
```bash
ulimit -n 65536
```

### Connection refused errors
Verify the server is running and accessible:
```bash
netstat -tulpn | grep 8080
```

### Poor performance
Check:
- Server thread count
- System resource utilization (CPU, memory, disk I/O)
- Network latency and bandwidth