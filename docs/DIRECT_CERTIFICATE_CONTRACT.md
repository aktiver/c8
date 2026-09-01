# Direct Certificate Contract — Phase 40.4

Phase 40.4 introduced the immutable evidence envelope for one successful exact OWL Direct basic-graph-pattern result. Phase 40.9 extends that envelope with proof-bound format version 2: every exact returned solution multiplicity is covered by an immutable reasoner-check record and every successful execution carries a completion-barrier support ID. HermiT derivation DAGs are still not available and are not fabricated.

## Binding

Every certificate binds the dataset, snapshot, query SHA-256, canonical BGP SHA-256, active dataset, authorized graph set, Phase 40.1 OWL signature, Phase 40.2 datatype policy, active graph context, and the scheduling-independent digest of the Phase 40.3 Direct-BGP result.

A certificate may only certify an `exact-complete` result. Failed, timed-out, partially evaluated, or resource-exhausted results never receive a success certificate.

## Completeness evidence

The Phase 40.4 completeness method is `exhaustive-candidate-entailment`. The certificate records the candidate-space hash, candidate and checked counts, distributed partition counts, exact reasoner request/success counts, and an execution-root SHA-256. Checked candidates must equal the full candidate inventory, every partition must complete, and every reasoner request must succeed.

This is intentionally shaped for the Phase 40.8 distributed exact fallback: Kubernetes may split the finite candidate inventory across workers/nodes, but the certificate is admitted only after complete deterministic reduction.

## Deterministic result digest

`direct_bgp_result_sha256` uses a domain-separated SHA-256 encoding. Solution mappings are independently hashed from sorted `BTreeMap` bindings and exact RDF terms, then those per-solution hashes are sorted before the result digest is finalized. Consequently pod completion order and CPU scheduling cannot change the digest of the same SPARQL multiset.

## Proof/support references

The certificate has a closed support-reference vocabulary and a `proofCoverage` state. Legacy format version 1 remains readable for immutable Phase 40.4 artifacts. Format version 2 requires `proofManifestSha256`, `proofCoverage=complete`, and strictly sorted reasoner-check support references that all bind the same proof manifest. The proof manifest must reproduce the exact SPARQL result multiplicities and the global completion barrier. Public OWL Direct standards claims remain disabled until the later conformance gates pass.
