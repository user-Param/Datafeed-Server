FROM ubuntu:22.04 AS build

RUN apt-get update && apt-get install -y \
    build-essential cmake git libboost-all-dev libssl-dev \
    libcurl4-openssl-dev nlohmann-json3-dev python3-dev pybind11-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Build Broker (outputs eadapter)
RUN cd broker && mkdir -p build && cd build && cmake .. && make

# Build Executor
RUN cd executor && mkdir -p build && cd build && cmake .. && make

# Build Engine
RUN cd engine && mkdir -p build && cd build && cmake .. && make

# Runtime stage
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    supervisor nginx \
    libboost-thread1.74.0 libssl3 libcurl4 \
    libpython3.10 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy only the binaries that exist
COPY --from=build /app/broker/build/eadapter ./broker/eadapter
COPY --from=build /app/executor/build/executor ./executor/executor
COPY --from=build /app/engine/build/engine ./engine/engine

# Copy configs
COPY supervisord.conf /etc/supervisor/supervisord.conf
COPY nginx.conf /etc/nginx/nginx.conf

EXPOSE 80
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]