# Build status — MCP Agent 0.5.0

`acceptance/static.sh` passed all cumulative source gates: internal manifest verification, strict JSON parsing, catalog SQL structure, Phase 3 delegation authentication, Phase 4 long-input compilation, Phase 5 provider/orchestrator semantics, frozen NGKG OpenAPI hashes, public-client route isolation, Kubernetes security assertions, and 80% CPU/memory targets.

`acceptance/native.sh` stopped with its intentional prerequisite exit code because `cargo` is unavailable in this workspace. Therefore Rust compilation, formatting, unit tests, Clippy, Helm lint/rendering, OCI builds, live PostgreSQL RLS tests, provider interoperability, live OWL entailment queries, KEDA behavior, GPU scheduling, node provisioning and multinode Kubernetes tests remain unexecuted. This is a source-implemented candidate, not a production-qualified binary release.
