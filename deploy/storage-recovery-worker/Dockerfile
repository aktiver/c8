FROM gcr.io/distroless/cc-debian12:nonroot
COPY ngkg-storage-recovery-worker /usr/local/bin/ngkg-storage-recovery-worker
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/ngkg-storage-recovery-worker"]
