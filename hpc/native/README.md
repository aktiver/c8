# Native MPI and OpenMP boundaries

`ngkg-mpi-exec` is launched once per pod/rank by `mpirun`. It derives the real
MPI rank and node-local rank, exports the normalized `NGKG_MPI_*` contract,
runs the Rust partition worker, reduces exit status with `MPI_Allreduce`, and
does not let any rank pass the terminal barrier after another rank failed.

`ngkg-openmp-filter` is an optional deterministic predicate kernel. Rust owns
all buffers and sends a bounded little-endian process message; the kernel never
owns Rust memory. The Rust runtime repeats the semantic predicate checks after
the OpenMP prefilter, so a native-kernel error fails closed and cannot alter a
certified result silently.
