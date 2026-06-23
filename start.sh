#!/bin/bash

# Datafeed Server Startup Script
# This script starts all necessary services for the datafeed system

# Configuration
SERVER_PORT=4444
SERVER_ADDRESS="0.0.0.0"
SERVER_THREADS=4
BUILD_DIR="./build"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Starting Datafeed Server...${NC}"

# Check if we're in the right directory
if [ ! -f "CMakeLists.txt" ]; then
    echo -e "${RED}Error: CMakeLists.txt not found. Please run this script from the project root directory.${NC}"
    exit 1
fi

# Build the project if needed
if [ ! -f "${BUILD_DIR}/datafeed" ]; then
    echo -e "${YELLOW}Building project...${NC}"
    if [ ! -d "${BUILD_DIR}" ]; then
        mkdir -p "${BUILD_DIR}"
    fi
    cd "${BUILD_DIR}" && cmake .. && make
    if [ $? -ne 0 ]; then
        echo -e "${RED}Build failed!${NC}"
        exit 1
    fi
    cd ..
    echo -e "${GREEN}Build successful!${NC}"
fi

# Function to kill processes on a specific port
kill_port() {
    local port=$1
    echo -e "${YELLOW}Checking for existing processes on port ${port}...${NC}"

    # Find and kill processes using the port
    if command -v lsof > /dev/null; then
        PID=$(lsof -ti:${port})
        if [ ! -z "$PID" ]; then
            echo -e "${YELLOW}Killing process ${PID} on port ${port}${NC}"
            kill -9 $PID 2>/dev/null
            sleep 1
        fi
    elif command -v netstat > /dev/null; then
        PID=$(netstat -tulpn | grep :$port | awk '{print $7}' | cut -d'/' -f1)
        if [ ! -z "$PID" ] && [ "$PID" != "-" ]; then
            echo -e "${YELLOW}Killing process ${PID} on port ${port}${NC}"
            kill -9 $PID 2>/dev/null
            sleep 1
        fi
    elif command -v ss > /dev/null; then
        PID=$(ss -tulpn | grep :$port | awk '{print $7}' | cut -d'/' -f1)
        if [ ! -z "$PID" ] && [ "$PID" != "-" ]; then
            echo -e "${YELLOW}Killing process ${PID} on port ${port}${NC}"
            kill -9 $PID 2>/dev/null
            sleep 1
        fi
    fi
}

# Kill any existing processes on our ports
echo -e "${YELLOW}Clearing ports before startup...${NC}"
kill_port $SERVER_PORT

# Start the server
echo -e "${YELLOW}Starting server on ${SERVER_ADDRESS}:${SERVER_PORT} with ${SERVER_THREADS} threads...${NC}"
cd "${BUILD_DIR}"
./datafeed "${SERVER_ADDRESS}" "${SERVER_PORT}" "${SERVER_THREADS}" &
SERVER_PID=$!
cd ..

# Save PID for potential cleanup
echo $SERVER_PID > server.pid

# Wait a moment to see if the server starts successfully
sleep 3

# Check if server is running
if ps -p $SERVER_PID > /dev/null; then
    echo -e "${GREEN}Server started successfully (PID: $SERVER_PID)${NC}"
    echo -e "${GREEN}Server is listening on ws://${SERVER_ADDRESS}:${SERVER_PORT}${NC}"
    echo -e "${GREEN}To stop the server, run: kill $(cat server.pid)${NC}"
else
    echo -e "${RED}Server failed to start! Check logs above for details.${NC}"
    exit 1
fi

# Keep the script running so we can monitor the server
echo -e "${YELLOW}Server is running. Press Ctrl+C to stop.${NC}"
wait $SERVER_PID