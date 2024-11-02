# Stage 1: Build the application
FROM rust:latest as builder

# Create a new empty shell project
RUN USER=root cargo new --bin rustbot
WORKDIR /rustbot

# Copy the Cargo.toml and Cargo.lock files
COPY Cargo.toml Cargo.lock ./

# Copy the source code
COPY src ./src

# Build the project
RUN cargo build --release

# Stage 2: Create a minimal image to run the application
FROM debian:buster-slim

# Copy the build artifact from the builder stage
COPY --from=builder /rustbot/target/release/rustbot /usr/local/bin/rustbot

# Set the startup command to run the binary
CMD ["rustbot"]