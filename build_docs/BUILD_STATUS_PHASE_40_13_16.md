# Build status — Phase 40.13.16

Status: `source-implemented-static-qualified-candidate`

## Passed

- Phase 40.13.16 static contract.
- Phase 40.13.8–15 cumulative static contracts.
- Online and control-plane OpenAPI parity.
- Helm values/schema and production autoscaling overlay validation.
- JSON, TOML, non-template YAML, and shell syntax checks.
- Fail-closed source inspection for replica identity, authorization, completeness, checksum, and
  scalar-result equality.

## Blocked in this environment

- `cargo fmt --all --check`
- `cargo check --locked --workspace --all-targets --all-features`
- `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings`
- `cargo test --locked --workspace --all-features`
- Maven HermiT adapter tests
- Helm lint/render
- live Kubernetes replica, HPA, cluster-autoscaler, node-loss, timeout, and mismatch qualification

## Release boundary

This is not a production-qualified complete-distributed-SPARQL claim. The live cluster must prove
all query-form equivalence and failure behavior, and the runtime must replace the scalar-image
dependency with partition-native semantic scans before datasets larger than 64 GiB can be activated
and queried through this lane.
