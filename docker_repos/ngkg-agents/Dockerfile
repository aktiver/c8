ARG RUST_BUILDER_IMAGE
ARG RUNTIME_IMAGE
FROM ${RUST_BUILDER_IMAGE} AS builder
ARG BUILD_OFFLINE=false
WORKDIR /source
COPY ngkg-agents/ .
RUN case "${BUILD_OFFLINE}" in true) set -- --offline ;; false) set -- ;; *) exit 64 ;; esac; \
    cargo build --locked "$@" --release \
    --package ngkg-mcp-gateway \
    --package ngkg-agent-catalog-migrator \
    --package ngkg-prompt-compiler \
    --package ngkg-qualification-worker \
    --package ngkg-inference-gateway \
    --package ngkg-vllm-pod-agent \
    --package ngkg-context-slice-broker \
    --package ngkg-context-slice-gc
FROM ${RUNTIME_IMAGE}
COPY --from=builder /source/target/release/ngkg-mcp-gateway /usr/local/bin/
COPY --from=builder /source/target/release/ngkg-agent-catalog-migrator /usr/local/bin/
COPY --from=builder /source/target/release/ngkg-prompt-compiler /usr/local/bin/
COPY --from=builder /source/target/release/ngkg-qualification-worker /usr/local/bin/
COPY --from=builder /source/target/release/ngkg-inference-gateway /usr/local/bin/
COPY --from=builder /source/target/release/ngkg-vllm-pod-agent /usr/local/bin/
COPY --from=builder /source/target/release/ngkg-context-slice-broker /usr/local/bin/
COPY --from=builder /source/target/release/ngkg-context-slice-gc /usr/local/bin/
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/ngkg-mcp-gateway"]
