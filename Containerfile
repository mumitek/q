# --- Stage 1: Build ---
FROM rust:1.92-slim AS builder

WORKDIR /usr/src/app
COPY . .

# Build for release
RUN cargo build --release

# --- Stage 2: Runtime ---
# Using Google's Distroless for the smallest, most secure footprint
FROM gcr.io/distroless/cc-debian12

# Copy the binary from the builder stage
COPY --from=builder /usr/src/app/target/release/q /usr/local/bin/q

EXPOSE 9095
CMD ["q", "--port", "9095", "--data-dir", "/var/data/q/", "--num-partition", "32", "--host", "0.0.0.0"]
