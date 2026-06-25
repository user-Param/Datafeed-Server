FROM ubuntu:22.04

# Set non-interactive mode for apt-get
ENV DEBIAN_FRONTEND=noninteractive

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
    make -j4

# Set working directory to where the executable is
WORKDIR /home/appuser/build

# Make the startup script executable
RUN chmod +x /home/appuser/start.sh

# Expose the port (default 4444)
EXPOSE 4444

# Default command: use start.sh (Docker-aware, skips host checks)
# Can be overridden by docker-compose or docker run
CMD ["/home/appuser/start.sh"]