# Phase 40.13 — Operator ceiling propagation

Phase 40.13 closes the Helm → operator → generated Job → reference-worker resource-policy chain. Both operators load the immutable Phase 40 exact-reasoner ConfigMap, validate the same shared Rust ceiling object, include it in work-spec identity, and copy the exact values plus their SHA-256 into generated reference/reasoner Jobs.

The distributed operator propagates this bundle only to the `Reasoner` stage; projection/reducer/artifact stages remain independent HPC workloads. Existing Jobs whose work-spec or Phase 40 policy hash differs fail closed instead of being adopted after an operator restart.

This phase does not change OWL Direct semantics, candidate enumeration, proof construction, or online admission behavior. Native Cargo/Helm/RKE2 execution remains a later qualification gate.
