FROM rust:1.98-slim-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY protocol ./protocol
COPY migrations ./migrations
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    cargo build --locked --release \
    && cp /build/target/release/ecopaste-sync-server /build/ecopaste-sync-server

FROM debian:bookworm-slim

LABEL io.github.ecopaste.component="sync-server"

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/ecopaste-sync-server /usr/local/bin/ecopaste-sync-server

ENV ECOPASTE_DATA_DIR=/data \
    ECOPASTE_BIND=0.0.0.0:44820 \
    RUST_LOG=info
VOLUME ["/data"]
EXPOSE 44820/udp

ENTRYPOINT ["ecopaste-sync-server"]
