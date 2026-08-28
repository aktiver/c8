# Phase 12 — Verification and performance qualification

Phase 12 adds exact endpoint benchmarking, distributed/failure matrices, release commands, cumulative-tag verification, and reproducible phase packaging. The benchmark runs every declared cache state and concurrency level, compares NGKG and the certified baseline to the expected SPARQL multiset, retains failed queries, and gates the 20× production / 50× hot targets.

No production claim is made by generating these files. Release requires real catalog, object store, Parquet/Iceberg, Arrow Flight, reasoner, mTLS/CNI, Kubernetes/RKE2, autoscaling, fault injection, and same-hardware Jena+DL execution. Unavailable gates remain blocked in verification evidence.

