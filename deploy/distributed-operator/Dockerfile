ARG RUST_BUILDER_IMAGE
ARG RUNTIME_IMAGE

FROM ${RUST_BUILDER_IMAGE} AS build
WORKDIR /source
COPY . .
RUN cargo build --locked --release --package ngkg-distributed-operator

FROM ${RUNTIME_IMAGE}
COPY --from=build /source/target/release/ngkg-distributed-operator /usr/local/bin/ngkg-distributed-operator
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/ngkg-distributed-operator"]
