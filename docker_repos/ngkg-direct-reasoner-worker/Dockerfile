ARG RUST_BUILDER_IMAGE
ARG MAVEN_BUILDER_IMAGE
ARG JAVA_RUNTIME_IMAGE
FROM ${MAVEN_BUILDER_IMAGE} AS reasoner-builder
ARG BUILD_OFFLINE=false
WORKDIR /source
COPY NGKG_1_0_0_GA/adapters/hermit-reasoner ./adapters/hermit-reasoner
RUN case "${BUILD_OFFLINE}" in true) set -- --offline ;; false) set -- ;; *) exit 64 ;; esac; \
    mvn --batch-mode --no-transfer-progress "$@" -f adapters/hermit-reasoner/pom.xml clean package
FROM ${RUST_BUILDER_IMAGE} AS rust-builder
ARG BUILD_OFFLINE=false
WORKDIR /source
COPY NGKG_1_0_0_GA/ .
RUN case "${BUILD_OFFLINE}" in true) set -- --offline ;; false) set -- ;; *) exit 64 ;; esac; \
    cargo build --locked "$@" --release --package ngkg-direct-reasoner-worker
FROM ${JAVA_RUNTIME_IMAGE}
COPY --from=rust-builder /source/target/release/ngkg-direct-reasoner-worker /usr/local/bin/ngkg-direct-reasoner-worker
COPY --from=reasoner-builder /source/adapters/hermit-reasoner/target/ngkg-hermit-adapter.jar /opt/ngkg/ngkg-hermit-adapter.jar
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/ngkg-direct-reasoner-worker"]
