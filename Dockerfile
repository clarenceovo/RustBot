# Stage 1: Build Stage
FROM rust:1.80 as builder

# Set the working directory inside the container
WORKDIR /app

# Copy only the Cargo.toml and Cargo.lock first to cache dependencies
COPY Cargo.toml Cargo.lock ./

# Pre-fetch dependencies by building an empty project
RUN cargo fetch
RUN cargo build --release || true  # Force a dummy build to cache dependencies

# Copy the rest of the source code and build the actual binary
COPY ./ ./
RUN cargo build --release

---

# Stage 2: Final Lightweight Image
FROM debian:bullseye-slim  # Minimal runtime base image

# Install minimal dependencies needed to run the binary
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl-dev && \
    rm -rf /var/lib/s
