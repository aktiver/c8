# Phase 40.4 — Direct Certificate Runtime Contract + JSON Schema

Phase 40.4 adds the shared Direct certificate object, deterministic result digest, exact completeness-evidence envelope, reasoner identity, proof/support reference vocabulary, Draft 2020-12 JSON Schema, independent validator, fixtures, reasoner-boundary verification, ancestry evidence and cumulative gates.

The phase does **not** implement arbitrary OWL Direct BGP legality, exact HermiT fallback, or full proof-DAG/support coverage. Those remain Phase 40.7, 40.8 and 40.9 respectively.

HPC relevance is correctness-focused: certificate completeness carries partition counts and execution-root evidence for future multi-node exact reasoning, while result hashing sorts per-solution digests so distributed completion order cannot affect identity.

Phase 40.4 also makes the Phase 40.3 ancestry gate descendant-safe: the historical 40.2→40.3 evidence is validated as immutable evidence, while the new 40.3→40.4 manifest proves current-tree changes. This prevents legitimate descendant edits from being misclassified as historical corruption.
