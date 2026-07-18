# syntax=docker/dockerfile:1.7

FROM rust:1-bookworm AS builder

WORKDIR /workspace
ENV CARGO_PROFILE_RELEASE_STRIP=symbols

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

ARG TARGETPLATFORM

RUN --mount=type=cache,id=pg-kinetic-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=pg-kinetic-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=pg-kinetic-target-${TARGETPLATFORM},target=/workspace/target,sharing=locked \
    cargo build --locked --release -p pg-kinetic && \
    cp /workspace/target/release/pg-kinetic /usr/local/bin/pg-kinetic

FROM debian:bookworm-slim AS runtime

RUN apt-get update && \
    apt-get install --yes --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/pg-kinetic /usr/local/bin/pg-kinetic

RUN groupadd --system --gid 10001 pg-kinetic && \
    useradd --system --uid 10001 --gid 10001 --home-dir /nonexistent --shell /usr/sbin/nologin pg-kinetic

USER 10001:10001

EXPOSE 6432 7000 9090 9091

ENTRYPOINT ["/usr/local/bin/pg-kinetic"]
