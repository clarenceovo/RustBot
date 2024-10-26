# Use the official Rust image as the base image
FROM rust:latest AS builder

# Set the working directory inside the container
WORKDIR /usr/src/RustBot

# Copy the Cargo.toml and Cargo.lock files
COPY Cargo.toml Cargo.lock ./

# Copy the source code
COPY src ./src

# Build the application
RUN cargo build --release

# Use a minimal image for the final stage
FROM debian:buster-slim

# Copy the compiled binary from the builder stage
COPY --from=builder /usr/src/RustBot/target/release/RustBot /usr/local/bin/RustBot

# Set the entrypoint command to run the application
ENTRYPOINT ["/usr/local/bin/RustBot"]