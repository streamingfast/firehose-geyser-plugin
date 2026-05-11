# Define build arguments for image tags (must be before FROM statements)
ARG SOLANA_TAG=v4.0.0-rc.1-novote
ARG FIREHOSE_CORE_TAG=v1.14.1

# Stage 1: Build the Geyser plugin
FROM ubuntu:24.04 AS builder

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN bash -c "curl https://sh.rustup.rs -sSf | sh -s -- -y"
ENV PATH="/root/.cargo/bin:${PATH}"

# Set working directory
WORKDIR /build

# Copy the entire project
COPY . .

# Build the plugin with native CPU optimizations
RUN RUSTFLAGS="-C target-cpu=native" cargo build --release

# Stage 2: Extract Solana binaries
FROM ghcr.io/streamingfast/solana:${SOLANA_TAG} AS solana

# Stage 3: Extract Firehose Core binary
FROM ghcr.io/streamingfast/firehose-core:${FIREHOSE_CORE_TAG} AS firehose-core

# Stage 4: Final assembly
FROM ubuntu:24.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
RUN mkdir -p /app

# Copy Solana binaries from solana stage
COPY --from=solana /agave/target/release/agave-validator /app/agave-validator
COPY --from=solana /agave/target/release/solana-test-validator /app/solana-test-validator

# Copy Geyser plugin from builder stage
COPY --from=builder /build/target/release/libfirehose_geyser_plugin.so /app/libfirehose_geyser_plugin.so

# Copy Firehose Core binary from firehose-core stage
COPY --from=firehose-core /app/firecore /app/firecore

# Add /app to PATH
ENV PATH="/app:${PATH}"

# Set working directory
WORKDIR /app
