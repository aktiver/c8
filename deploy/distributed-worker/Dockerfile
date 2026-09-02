ARG RUST_BUILDER_IMAGE
ARG RUNTIME_IMAGE

FROM ${RUST_BUILDER_IMAGE} AS build
ARG BUILD_OFFLINE=false
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY services ./services
RUN case "${BUILD_OFFLINE}" in true) set -- --offline ;; false) set -- ;; *) exit 64 ;; esac; \
    cargo build --locked "$@" --release --bin ngkg-distributed-worker

FROM ${RUNTIME_IMAGE}
COPY --from=build /src/target/release/ngkg-distributed-worker /usr/local/bin/ngkg-distributed-worker
ENTRYPOINT ["/usr/local/bin/ngkg-distributed-worker"]
USER 65532:65532
