# Phase 2 — Immutable source planning

Phase 2 makes work discovery deterministic and distributed. Parquet work names exact files, row groups, and required columns. Iceberg work names an exact table snapshot, manifest, and entry ordinals. TriG work can only reference canonical shards produced by a syntax-aware safe-scan stage; the type system has no arbitrary TriG byte-range variant.

Partition identifiers derive from immutable context and safe source ranges. Replanning produces the same envelopes. Workers verify input, mapping, ontology, schema, and output-contract hashes before writing content-addressed results.

