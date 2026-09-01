ARG RUST_BUILDER_IMAGE
ARG RUNTIME_IMAGE

FROM ${RUST_BUILDER_IMAGE} AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY services ./services
RUN cargo build --locked --offline --release --bin ngkg-distributed-worker

FROM ${RUNTIME_IMAGE}
COPY --from=build /src/target/release/ngkg-distributed-worker /usr/local/bin/ngkg-distributed-worker
ENTRYPOINT ["/usr/local/bin/ngkg-distributed-worker"]
USER 65532:65532
