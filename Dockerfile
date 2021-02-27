FROM rustlang/rust:nightly-slim AS build

RUN rustup target add x86_64-unknown-linux-musl --toolchain=nightly
RUN rustup component add rustfmt
ENV PKG_CONFIG_ALLOW_CROSS=1

WORKDIR /app
COPY . .
RUN cargo build --release --target x86_64-unknown-linux-musl

FROM alpine:3.13.1

WORKDIR /var/lib/dumpstors
RUN chown -R 1000 /var/lib/dumpstors
USER 1000

COPY --from=build /app/target/x86_64-unknown-linux-musl/release/dumpstors \
                  /app/target/x86_64-unknown-linux-musl/release/dumpcli \
                  /usr/local/bin/

ENV RUST_LOG=info
EXPOSE 4242

CMD ["dumpstors"]
