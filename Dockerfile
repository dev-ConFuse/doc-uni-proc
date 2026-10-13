# ==============================================================================
# Unified Processor Service - Dockerfile
# ==============================================================================
# Multi-stage build for Rust service
# Port supplied at runtime via .map.env / .secret.env
# ==============================================================================

# ==============================================================================
# Stage 1: Rust builder
# ==============================================================================
FROM debian:bookworm-slim AS rust-builder

ARG RUST_VERSION=stable

# Install minimal build-time dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    pkg-config \
    libssl-dev \
    libcurl4-openssl-dev \
    zlib1g-dev \
    cmake \
    build-essential \
    librdkafka-dev \
    && apt-get autoremove -y && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* /root/.cache

# Install Rust via rustup to guarantee latest stable compiler
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain ${RUST_VERSION}
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

# ---------------------------------------------------------------------------
# Dependency caching layer
# ---------------------------------------------------------------------------
COPY Cargo.toml Cargo.lock* ./

# Build a minimal stub that mirrors the actual lib.rs module layout so cargo
# can compile all crate dependencies without the real source files.
RUN mkdir -p \
        src/core \
        src/processors \
        src/infra \
        src/graph \
        src/utils && \
    echo 'fn main() {}' > src/main.rs && \
    printf 'pub mod core;\npub mod processors;\npub mod infra;\npub mod graph;\n' > src/lib.rs && \
    touch \
        src/core/mod.rs \
        src/processors/mod.rs \
        src/infra/mod.rs \
        src/graph/mod.rs

# Cache dependencies
RUN cargo build --release 2>/dev/null; \
    cargo clean -p doc-uni-proc 2>/dev/null; \
    rm -rf /app/target/release/deps /app/target/release/build && \
    true

# ---------------------------------------------------------------------------
# Real build
# ---------------------------------------------------------------------------
RUN rm -rf src/*
COPY src/ ./src/

# Force Cargo to invalidate the cache by updating timestamps
RUN touch src/lib.rs src/main.rs && cargo build --release

# ==============================================================================
# Stage 2: Runtime image
# ==============================================================================
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    dumb-init \
    curl \
    ca-certificates \
    libpq5 \
    libssl3 \
    librdkafka1 \
    libgomp1 \
    libglib2.0-0 \
    && apt-get autoremove -y && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* /root/.cache

# Guarantee the linker can find the shared library
ENV LD_LIBRARY_PATH="/usr/lib/x86_64-linux-gnu:/usr/local/lib:$LD_LIBRARY_PATH"

# SECURITY: Create a non-root user and group
RUN groupadd -r appgroup && useradd -r -g appgroup appuser

WORKDIR /app

# Copy the compiled Rust binary and set ownership
# (binary name = auto-discovered src/main.rs → package name)
COPY --from=rust-builder --chown=appuser:appgroup /app/target/release/doc-uni-proc /usr/local/bin/doc-uni-proc

# Ensure the appuser owns the working directory
RUN chown -R appuser:appgroup /app

# SECURITY: Switch to the non-root user
USER appuser

# ⚠️ PORT is supplied at runtime (.map.env / platform env) — see .cline/rules/env-policy.md
EXPOSE 8080

ENTRYPOINT ["dumb-init", "--"]
CMD ["doc-uni-proc"]
