FROM ubuntu:22.04

# Set non-interactive mode for apt-get
ENV DEBIAN_FRONTEND=noninteractive
ENV DOCKER_ENV=true

# Install dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    libboost-all-dev \
    libssl-dev \
    libpq-dev \
    libpqxx-dev \
    nlohmann-json3-dev \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN useradd -m -u 1000 appuser
USER appuser
WORKDIR /home/appuser

# Copy source code
COPY --chown=appuser:appuser . .

# Build the application
RUN mkdir -p build && cd build && \
    cmake .. && \
    make -j4 && \
    ls -la datafeed

# Set working directory to where the executable is
WORKDIR /home/appuser/build

# Expose local default and Render's default web port.
EXPOSE 4444 10000

# Use Render's PORT environment variable, default to 10000 (Render's default)
CMD ["sh", "-c", "if [ -f ./datafeed ]; then exec ./datafeed 0.0.0.0 ${PORT:-10000} ${WEB_CONCURRENCY:-1}; else echo 'Executable not found'; ls -la; exit 1; fi"]
