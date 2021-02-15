FROM rust:1.49.0-slim AS builder
WORKDIR /app
COPY . .

RUN rustup component add rustfmt
RUN cargo build --release

FROM alpine:3.13.1
WORKDIR /app

COPY --from=builder /app/target/release .
USER 1000

CMD ["/app/dumpstors"]