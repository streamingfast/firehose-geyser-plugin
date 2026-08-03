# Define build arguments for image tags (must be before FROM statements)
ARG SOLANA_TAG=v4.2.0-rc.1-fh3.0
ARG FIREHOSE_CORE_TAG=v1.14.5

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

# Build a portable x86_64 plugin. Do NOT use target-cpu=native here: the image is
# built on CI runners whose ISA may be newer than production hosts (e.g. multichain
# nodes). native can emit AVX-512/other opcodes that trap as SIGILL
# ("trap invalid opcode ... in libfirehose_geyser_plugin.so") on solBankNotif.
# gxhash requires AES-NI + SSE2; enable only those features so the .so runs on any
# AES-capable server without host-specific instructions.
# See: https://github.com/ogxd/gxhash (RUSTFLAGS="-C target-feature=+aes,+sse2")
RUN RUSTFLAGS="-C target-feature=+aes,+sse2" cargo build --release

# Stage 2: Extract Solana binaries
FROM ghcr.io/streamingfast/solana:${SOLANA_TAG} AS solana

# Stage 3: Extract Firehose Core binary
FROM ghcr.io/streamingfast/firehose-core:${FIREHOSE_CORE_TAG} AS firehose-core

# Stage 4: Final assembly
FROM ubuntu:24.04

# Install runtime dependencies (libcap2-bin provides setcap, used below)
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libcap2-bin \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
RUN mkdir -p /app

# Copy Solana binaries from solana stage
COPY --from=solana /agave/target/release/agave-validator /app/agave-validator
COPY --from=solana /agave/target/release/solana-test-validator /app/solana-test-validator

# Stamp file capabilities onto the validator binary. Starting with agave-validator
# v4.2.0 (alpenglow client), the validator requires CAP_NET_ADMIN and CAP_NET_RAW to
# manage its UDP sockets. The image runs non-root (sf-operator sets user <uid>:65534),
# so cap_add alone only fills the bounding set. File capabilities grant them effective
# at exec, bounded by the cap_add (NET_ADMIN, NET_RAW) sf-operator provides.
RUN setcap cap_net_admin,cap_net_raw+ep /app/agave-validator

# Copy Geyser plugin from builder stage
COPY --from=builder /build/target/release/libfirehose_geyser_plugin.so /app/libfirehose_geyser_plugin.so

# Copy Firehose Core binary from firehose-core stage
COPY --from=firehose-core /app/firecore /app/firecore

# Add /app to PATH
ENV PATH="/app:${PATH}"

# Set working directory
WORKDIR /app
