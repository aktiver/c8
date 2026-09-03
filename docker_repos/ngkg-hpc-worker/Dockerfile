ARG RUST_BUILDER_IMAGE
ARG MPI_BUILDER_IMAGE
ARG MPI_RUNTIME_IMAGE

FROM ${RUST_BUILDER_IMAGE} AS rust-builder
ARG BUILD_OFFLINE=false
WORKDIR /source
COPY NGKG_1_0_0_GA/ .
RUN case "${BUILD_OFFLINE}" in true) set -- --offline ;; false) set -- ;; *) exit 64 ;; esac; \
    cargo build --locked "$@" --release --package ngkg-distributed-worker

FROM ${MPI_BUILDER_IMAGE} AS native-builder
WORKDIR /native
COPY hpc/native/ngkg_mpi_exec.c hpc/native/ngkg_openmp_filter.c ./
RUN mpicc -O3 -fstack-protector-strong -D_FORTIFY_SOURCE=2 -Wformat -Wformat-security -Werror=format-security \
      -o /out-ngkg-mpi-exec ngkg_mpi_exec.c && \
    cc -O3 -fopenmp -fstack-protector-strong -D_FORTIFY_SOURCE=2 -Wformat -Wformat-security -Werror=format-security \
      -o /out-ngkg-openmp-filter ngkg_openmp_filter.c

FROM ${MPI_RUNTIME_IMAGE}
COPY --from=rust-builder /source/target/release/ngkg-distributed-worker /usr/local/bin/ngkg-distributed-worker
COPY --from=native-builder /out-ngkg-mpi-exec /usr/local/bin/ngkg-mpi-exec
COPY --from=native-builder /out-ngkg-openmp-filter /usr/local/bin/ngkg-openmp-filter
ENV NGKG_OPENMP_FILTER_EXECUTABLE=/usr/local/bin/ngkg-openmp-filter \
    OMP_DYNAMIC=FALSE \
    OMP_MAX_ACTIVE_LEVELS=1 \
    OMP_PROC_BIND=close \
    OMP_PLACES=cores \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1
RUN test -x /usr/bin/mpirun && \
    test -x /usr/local/bin/ngkg-mpi-exec && \
    test -x /usr/local/bin/ngkg-openmp-filter
USER 65532:65532
