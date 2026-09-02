ARG RUST_BUILDER_IMAGE
ARG RUNTIME_IMAGE

FROM ${RUST_BUILDER_IMAGE} AS build
ARG BUILD_OFFLINE=false
WORKDIR /source
COPY . .
RUN case "${BUILD_OFFLINE}" in true) set -- --offline ;; false) set -- ;; *) exit 64 ;; esac; \
    cargo build --locked "$@" --release --package ngkg-distributed-operator

FROM ${RUNTIME_IMAGE}
COPY --from=build /source/target/release/ngkg-distributed-operator /usr/local/bin/ngkg-distributed-operator
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/ngkg-distributed-operator"]
