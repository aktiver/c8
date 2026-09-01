# Phase 5 — Arrow projection and semantic spine

Phase 5 introduces the columnar hot path. Compiled mappings stream exact source ranges into typed Arrow batches, split facts by treatment, write immutable domain Parquet/Iceberg data and the integer semantic spine, and fan the same validated batch into downstream contribution builders.

The batch validator enforces the schema version and exactly one object representation matching `object_kind8`. SQL `NULL` cannot become an RDF literal. Projection returns checksummed object manifests and never mutates the active database or publishes a snapshot.

