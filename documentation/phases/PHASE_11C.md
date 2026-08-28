# Phase 11C — Networking, autoscaling, and node-local HPC

Phase 11C creates separate REST/control and Arrow/binary data planes, headless ownership-aware services, default-deny data-plane policy, mTLS Secret mounts, PDBs, topology placement, and custom-metric HPAs. External dependency egress is denied until private CIDRs or an audited gateway are configured.

The runtime reads its effective cpuset and fails startup if Rust, I/O, OpenMP, BLAS, and control thread budgets oversubscribe it. Kubernetes nodes supply whole cores and NUMA policy; OpenMP and BLAS accelerate only measured native/dense kernels. Node scaling, batch admission, pod scaling, and NGKG work partitioning remain separate control loops.

