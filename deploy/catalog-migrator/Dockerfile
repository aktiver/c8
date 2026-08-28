ARG RUST_BUILDER_IMAGE
ARG RUNTIME_IMAGE

FROM ${RUST_BUILDER_IMAGE} AS builder
WORKDIR /source
COPY . .
RUN cargo build --locked --release --package ngkg-catalog-migrator

FROM ${RUNTIME_IMAGE}
COPY --from=builder /source/target/release/ngkg-catalog-migrator /usr/local/bin/ngkg-catalog-migrator
COPY migrations /opt/ngkg/migrations
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/ngkg-catalog-migrator"]
