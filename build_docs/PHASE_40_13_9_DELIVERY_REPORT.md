# NGKG Phase 40.13.9 delivery report

## Outcome

Phase 40.13.9 adds the exact distributed property-path source foundation on top of Phase 40.13.8.
It is not an online distributed-property-path or production qualification claim.

## Delivered

- Typed SPARQL property paths compile into checksum-bound Thompson NFAs covering predicate,
  inverse, sequence, alternative, negated-property-set, zero-or-one, zero-or-more and one-or-more
  semantics.
- Plans bind graph scope, endpoints, partitions and iteration/frontier/visited/checkpoint/spill/hot
  vertex ceilings.
- Frontier identity is `(origin, entity, automaton state)`, preserving independent input bindings.
- Stable hash ownership distributes work independently of the current replica count.
- High-degree vertices split into deterministic edge subranges for bounded concurrent execution.
- Endpoint pairs are set-deduplicated even when multiple physical routes exist.
- A complete-work barrier rejects missing, duplicate, foreign, partial or checksum-invalid results.
- Global termination requires a complete iteration followed by an empty new frontier.
- Canonical checkpoints bind query, plan, path, automaton, iteration, visited states, frontier and
  endpoints under a size ceiling and SHA-256.
- Ordinary exact online query evidence binds the property-path plan and automaton hashes.
- Fragment HPA source observes pending path work and frontier size; query HPA source observes
  checkpoint bytes while retaining CPU/memory signals and existing RKE2 node scaling.
- The existing Arrow, Grace-join, local-NVMe, cgroup and OpenMP/BLAS resource contracts remain.

## Qualification executable in this environment

- Parent archive and embedded manifest integrity.
- Phase 40.13.1 through 40.13.9 static contracts.
- OpenAPI route parity.
- Cargo manifest/lock dependency closure and JSON/YAML/TOML parsing.
- Archive path safety and fresh-extraction SHA-256 verification.

## Blocking gates

- Rust formatting, compilation, Clippy and native tests because Cargo/Rust are unavailable here.
- Maven and Helm execution because their tools are unavailable here.
- Worker transport and authorized adjacency-index activation.
- W3C and scalar differential endpoint-set equality for every property-path form.
- Checkpoint resume, corrupt checkpoint, timeout, retry, duplicate delivery and pod-drain tests.
- Real multinode frontier skew, spill, HPA and node-autoscaler qualification.

Until these gates pass, the scalar property-path evaluator remains authoritative. Phase 40.13.10
federation/protocol work must not be used to bypass Phase 40.13.9 activation and qualification.

No ontology-alignment or raw-data-mapping database functionality was added.
