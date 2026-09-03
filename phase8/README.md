# Phase 8 source acceptance

Run `./phase8/acceptance.sh` from the repository root. The gate validates the 13-image Docker/Helm catalog, strict HPC contracts, cgroup/MPI/Parquet/OpenMP source invariants, Kubernetes gang safeguards and all Swagger operation descriptions. When GCC with OpenMP is available, it also compiles the native predicate kernel and compares it with the scalar reference across deterministic test vectors.

This gate is intentionally not a live qualification. Cargo, Helm, image, MPI/Kueue and five-provider tests remain mandatory in Phases 9 and 10.
