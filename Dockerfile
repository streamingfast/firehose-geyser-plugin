# Define build arguments for image tags (must be before FROM statements)
ARG AGAVE_VERSION=v4.4.0-rc.1
ARG FIREHOSE_CORE_TAG=latest

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

# Stage 2: Build the validators from upstream Agave. Anza publishes no Docker image
# for Agave 4.x, and its release archive has solana-test-validator but no agave-validator.
FROM ubuntu:24.04 AS agave
ARG AGAVE_VERSION

RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    pkg-config \
    libssl-dev \
    libudev-dev \
    zlib1g-dev \
    llvm \
    clang \
    cmake \
    make \
    libprotobuf-dev \
    protobuf-compiler \
    libclang-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN bash -c "curl https://sh.rustup.rs -sSf | sh -s -- -y"
ENV PATH="/root/.cargo/bin:${PATH}"

# One layer for clone, build and cleanup, so the build cache keeps only the two binaries
# instead of the multi-GB target directory. --workspace stops cargo from turning on
# dev-context-only-utils through feature unification, like Agave's own release build.
RUN git clone --depth 1 --branch ${AGAVE_VERSION} https://github.com/anza-xyz/agave.git /agave \
    && cd /agave \
    && cargo build --release --workspace --bin agave-validator --bin solana-test-validator \
    && mkdir /out \
    && cp target/release/agave-validator target/release/solana-test-validator rust-toolchain.toml /out/ \
    && rm -rf /agave /root/.cargo/registry /root/.cargo/git

# The plugin is loaded into the validator with dlopen and exchanges Rust trait objects
# with it, so both must be built by the same rustc.
COPY rust-toolchain.toml /plugin-rust-toolchain.toml
RUN agave_channel="$(grep '^channel' /out/rust-toolchain.toml)" \
    && plugin_channel="$(grep '^channel' /plugin-rust-toolchain.toml)" \
    && if [ "$agave_channel" != "$plugin_channel" ]; then \
         echo "rust-toolchain.toml mismatch: Agave ${AGAVE_VERSION} has '$agave_channel', plugin has '$plugin_channel'"; \
         exit 1; \
       fi

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

# Copy Solana binaries from agave stage
COPY --from=agave /out/agave-validator /app/agave-validator
COPY --from=agave /out/solana-test-validator /app/solana-test-validator

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
