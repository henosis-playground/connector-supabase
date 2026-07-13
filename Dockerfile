FROM rust:1.96-bookworm AS build

ENV RUSTUP_TOOLCHAIN=1.96.0
WORKDIR /src
COPY . .
RUN cargo build --locked --release \
    -p henosis-connector-supabase-server \
    -p henosis-supabase-contract-harness

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
RUN install -d -m 0750 /var/lib/henosis-connector-supabase

FROM runtime AS contract-harness

COPY --from=build /src/target/release/henosis-supabase-contract-harness /usr/local/bin/henosis-supabase-contract-harness
ENTRYPOINT ["/usr/local/bin/henosis-supabase-contract-harness"]

FROM runtime AS final

COPY --from=build /src/target/release/henosis-connector-supabase-server /usr/local/bin/henosis-connector-supabase

EXPOSE 8082
ENTRYPOINT ["/usr/local/bin/henosis-connector-supabase"]
