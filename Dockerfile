FROM rust:1.80-slim-bookworm as builder

RUN USER=root apt-get update && apt-get install --yes pkg-config libssl-dev ca-certificates
RUN USER=root update-ca-certificates
RUN USER=root mkdir /basin-s3
WORKDIR /basin-s3

COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
COPY ./lib ./lib

RUN rustup component add rustfmt
RUN cargo build --features binary --release

FROM debian:bookworm-slim

# copy the build artifact from the build stage
COPY --from=builder /usr/bin/openssl /usr/bin/openssl
COPY --from=builder /usr/lib/ /usr/lib/
COPY --from=builder /usr/share/ca-certificates/ /usr/share/ca-certificates/
COPY --from=builder /usr/local/share/ca-certificates/ /usr/local/share/ca-certificates/
COPY --from=builder /etc/ssl/certs /etc/ssl/certs

COPY --from=builder /basin-s3/target/release/basin_s3 .

EXPOSE 8014

CMD ["./basin_s3"]
