FROM rust:1.49.0-slim AS builder
WORKDIR /build
COPY . .

RUN rustup component add rustfmt
RUN cargo build --release

FROM alpine:3.13.1
WORKDIR /app

COPY --from=builder /build/target/release /app
USER 1000

CMD ["/app/dumpstors"]