# NGKG Phase 38 Build Status

Status: **implementation-candidate-not-production-qualified**. Workspace version: **0.6.0**.

Phase 38 is a cumulative change on top of the Phase 37 candidate. The public SPARQL 1.1, `sd:UnionDefaultGraph`, OWL Direct, and OWL 2 DL standards claims remain disabled until the release qualification evidence enables those gates.

## Implemented in this candidate

- One shared `ngkg-sparql-compiler` parses SPARQL with the pinned SPARQL 1.1 `spargebra` grammar and `standard-unicode-escaping` behavior.
- Offline certification and online serving consume the same typed algebra instead of separately scanning query text.
- Query-level `FROM` and `FROM NAMED` are derived from the parsed dataset object.
- Protocol dataset parameters retain precedence over query dataset clauses; an authorized active dataset must still match the offline semantic dataset certificate before execution succeeds.
- Each certified query binds a versioned canonical algebra SHA-256 in addition to the exact raw query SHA-256 and exact result certificate.
- Routing is derived from typed graph patterns and active-dataset identities. Property paths and ambiguous routing conservatively use the complete active default graph set.
- Distributed execution is emitted only for a typed, pure constant-`GRAPH` inner-join tree that is independently executed and proven result-equivalent before publication.
- `SELECT` results use the standards serializer for SPARQL JSON, XML, TSV, and CSV with bounded output and HTTP content negotiation.
- `ASK`, `CONSTRUCT`, and `DESCRIBE` remain fail-closed pending Phase 39 scalar algebra/result certification.
- Swagger UI is embedded from a pinned vendored crate and served locally from `/docs`; runtime access to a public CDN is not required.
- Upstream transport deadlines and propagated 408/504 statuses return `504 Gateway Timeout`; other dependency failures remain `503`, with no complete result emitted on either path.
- Existing Arrow IPC, bounded Tokio execution, NVMe spooling, Grace joins, Parquet hydration, per-tenant admission, HPA, Kueue, and RKE2 cluster-autoscaler topology are retained.
- Online SPARQL parsing runs in the bounded compute/blocking pool after admission; PostgreSQL pool size is deployment-configurable through `NGKG_DATABASE_MAX_CONNECTIONS`, so parser CPU and catalog connections can be tuned against per-pod resources and HPA maxima.

## Qualification boundary

This environment does not provide Cargo/rustc, Maven, Helm, kubectl, or a live RKE2 API server. Those gates are therefore **not** recorded as passing. `Cargo.lock` is intentionally not fabricated; a toolchain-complete qualification environment must generate/verify it and run release commands with `--locked`.

Phase 38 is not releasable until `scripts/qualify_phase38.sh` and `scripts/ci_release.sh` succeed with the pinned W3C suite and live Kubernetes/RKE2 gates. Any parser, algebra, result-equivalence, authorization, protocol, serializer, build, or cluster failure remains a hard failure rather than a partial success.
