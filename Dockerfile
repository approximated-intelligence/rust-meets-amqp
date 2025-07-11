# Build stage - use musl target
FROM rust:1-alpine AS builder

# Install musl-dev for static linking
RUN apk add --no-cache musl-dev

WORKDIR /usr/src/app

# Add musl target
RUN rustup target add x86_64-unknown-linux-musl

# FORCE STATIC LINKING - this is the key addition
ENV RUSTFLAGS="-C target-feature=+crt-static"

# Copy dependency files first for better layer caching
COPY Cargo.toml Cargo.lock ./

# Create dummy main.rs to build dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl && \
    rm -rf src

# Copy actual source
COPY src ./src

# Build static binary
RUN cargo build --release --target x86_64-unknown-linux-musl


# Runtime stage - minimal Alpine
FROM alpine:latest

# Install only CA certificates (might be needed for TLS)
RUN apk add --no-cache musl
# RUN apk add --no-cache ca-certificates

# Create non-root user
RUN adduser -D -s /bin/sh amqp

# Copy static binary
COPY --from=builder /usr/src/app/target/x86_64-unknown-linux-musl/release/amqp-client /usr/local/bin/amqp-client

# Make executable and set ownership
RUN chmod +x /usr/local/bin/amqp-client && chown amqp:amqp /usr/local/bin/amqp-client

# Copy startup script
COPY start_amqp.sh /usr/local/bin/start_amqp.sh

# Make executable and set ownership
RUN chmod +x /usr/local/bin/start_amqp.sh && chown amqp:amqp /usr/local/bin/start_amqp.sh

# Switch to non-root user
USER amqp
