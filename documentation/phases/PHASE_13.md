# Phase 13 — Real single-node semantic reference slice

Phase 13 implements the tutorial's first recommended integration milestone: one process, real Parquet, a semantic spine, deterministic identity, a deliberately limited certified query set, direct GUID hydration, and independent answer equality. It builds on every Phase 12 contract but does not pretend that earlier stubs are already distributed services.

## Executable flow

```text
checksum-verified aligned TriG
  → standards TriG parser
  → deterministic skolem IRIs, GUIDs and FactIDs
  → exhaustive core / virtual / payload projection
  → semantic-spine.parquet + payload.parquet
  → fixed-width GUID locator
  → version-locked HermiT/OWLAPI adapter
  → finite named-entity materialization
  → Oxigraph SPARQL evaluation
  → independent multiset comparison
  → atomic local snapshot rename
  → certified-query execution + direct Parquet hydration
```

User data controls ontology content, projection policy, bounded query corpus, and requested resource limits. Operator configuration controls the Java executable, adapter JAR checksum, accepted reasoner identity, and hard ceilings for every requested bound. This separation prevents an uploaded ingestion manifest from selecting executable code or increasing its own denial-of-service budget. Imports must resolve to checksum-bound ontology documents in the same request; unresolved imports fail before Java starts.

## Correctness boundary

A query succeeds only when its SHA-256 appears in the immutable snapshot certificate. Compilation generates that certificate only after the offline reasoner succeeds, the ontology is consistent, all artifacts parse, and the observed SPARQL multiset equals an independently supplied expected multiset. Runtime re-verifies every artifact hash and rebuilds the small reference query store from verified N-Quads and reasoner output before evaluation.

The adapter's output is not advertised as a finite closure of arbitrary OWL 2 DL. It materializes named-individual class/object/data assertions, `sameAs`, and named class/property hierarchies. It reports that no proof DAG is available. General Direct-Semantics query answering therefore remains outside this phase unless a query/snapshot pair has been independently certified.

## Acceptance gate

The phase gate requires:

- Cargo formatting, clippy with warnings denied, and all Rust tests;
- Java 17/Maven adapter compilation and tests;
- successful reference compilation from `cross-domain.trig`;
- exact equality for `q01-cross-domain`;
- direct hydration of `rawMessage` for the bound observation GUID;
- rejection of an altered source, query, expected answer, adapter JAR, reasoner version, or snapshot artifact;
- rejection of unknown predicates, default-graph input under the checked-in policy, inconsistent ontologies, and uncertified queries;
- proof that payload hydration does not change semantic eligibility; and
- an explicit blocked status when Cargo, Maven, Java, or the real reasoner cannot run.

The environment that created this archive did not contain Cargo, Rust, Java, Maven, Docker, Helm, kubectl, or RKE2. Structural parsing can be run here, but executable and cluster gates must remain blocked until run in the declared toolchain environment.
