ARG RUST_BUILDER_IMAGE
ARG MAVEN_BUILDER_IMAGE
ARG JAVA_RUNTIME_IMAGE

FROM ${MAVEN_BUILDER_IMAGE} AS reasoner-builder
WORKDIR /source
COPY adapters/hermit-reasoner ./adapters/hermit-reasoner
RUN mvn --batch-mode --no-transfer-progress --offline -f adapters/hermit-reasoner/pom.xml clean package

FROM ${RUST_BUILDER_IMAGE} AS builder
WORKDIR /source
COPY . .
RUN cargo build --locked --offline --release --package ngkg-online-serving

FROM ${JAVA_RUNTIME_IMAGE}
COPY --from=builder /source/target/release/ngkg-online-serving /usr/local/bin/ngkg-online-serving
COPY --from=reasoner-builder /source/adapters/hermit-reasoner/target/ngkg-hermit-adapter.jar /opt/ngkg/ngkg-hermit-adapter.jar
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/ngkg-online-serving"]
