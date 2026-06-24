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
    libnlohmann-json-dev \
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

# Expose the port (default 4444)
EXPOSE 4444

# Default command: run server on 0.0.0.0:4444 with 1 thread
# Can be overridden by docker-compose or docker run
CMD ["./datafeed", "0.0.0.0", "4444", "1"]