ARG RUST_BUILDER_IMAGE
ARG RUNTIME_IMAGE

FROM ${RUST_BUILDER_IMAGE} AS builder
ARG BUILD_OFFLINE=false
WORKDIR /source
COPY . .
RUN case "${BUILD_OFFLINE}" in true) set -- --offline ;; false) set -- ;; *) exit 64 ;; esac; \
    cargo build --locked "$@" --release --package ngkg-operator

FROM ${RUNTIME_IMAGE}
COPY --from=builder /source/target/release/ngkg-operator /usr/local/bin/ngkg-operator
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/ngkg-operator"]
