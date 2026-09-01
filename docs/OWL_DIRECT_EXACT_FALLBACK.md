# OWL Direct exact fallback

`ngkg-direct-reasoner` is the correctness-first coordinator for uncovered Phase 40.7 legal BGPs. It writes immutable per-partition requests, invokes the checksum-pinned HermiT adapter, validates every partition identity and counter, and refuses exact completion unless ordinal coverage is contiguous from zero through the complete candidate count.

The Java adapter loads the complete pinned ontology/import set plus the exact scoped ABox, verifies OWL 2 DL profile and consistency, builds deterministic finite domains from the merged ontology, grounds each candidate, applies the grounded OWL 2 DL check, and asks HermiT to entail only the logical target axioms.

Kubernetes distribution is intentionally separated from semantic partitioning: Phase 40.8 defines stable candidate partitions; Phase 40.15 will schedule those same partitions across nodes.
