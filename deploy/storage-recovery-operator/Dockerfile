ARG RUST_BUILDER_IMAGE
ARG RUNTIME_IMAGE

FROM ${RUST_BUILDER_IMAGE} AS builder
WORKDIR /source
COPY . .
RUN cargo build --locked --offline --release --package ngkg-storage-recovery-operator

FROM ${RUNTIME_IMAGE}
COPY --from=builder /source/target/release/ngkg-storage-recovery-operator /usr/local/bin/ngkg-storage-recovery-operator
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/ngkg-storage-recovery-operator"]
