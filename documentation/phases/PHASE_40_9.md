# Phase 40.9 — Direct proof/support IDs and certificate verification

Phase 40.9 makes every successful Phase 40.8 exact result auditable. HermiT still does not expose a derivation DAG through this adapter, so NGKG does not invent one. Instead, every entailed candidate now returns the SHA-256 of the exact grounded RDF bytes and a canonical SHA-256 of the logical OWL axioms submitted to `isEntailed`. Rust binds those facts, the candidate ordinal, partition, request hash and exact semantic snapshot identities into a deterministic `supportId`.

The coordinator emits `DirectProofManifest`. Its answer records cover every candidate ordinal contributing to the compressed SPARQL multiset, while `completionSupportId` binds the exhaustive candidate-space and partition completion roots. The latter gives an exact empty result auditable completion evidence even when no answer record exists.

Direct certificate format v2 requires `proofManifestSha256`, `proofCoverage=complete`, and support references whose artifact SHA is exactly that manifest. The certificate is rejected unless the proof records reproduce the result's binding multiplicities exactly. Legacy format-v1 certificates remain parseable/validatable for prior immutable artifacts but cannot claim Phase 40.9 proof-manifest binding.

## HPC behavior

Proof construction is linear in the number of *entailed candidates*, not the full candidate space. Candidate partitions already execute independently in 40.8. Each partition emits fixed-size hashes with its entailed rows; the Rust coordinator deterministically reduces these records after the gap-free completion barrier. Duplicate SPARQL solutions stay compressed in `DirectBgpResult`; proof records retain their distinct candidate ordinals, so no duplicate row expansion is required in the query result.

Support IDs are independent of thread scheduling and Kubernetes worker completion order. Phase 40.15 can therefore distribute the same immutable candidate partitions across nodes and still produce identical support IDs, proof manifest content, result digest and certificate.

## What this phase does not claim

`proofCoverage=complete` means complete **answer-support coverage**: every returned multiset occurrence is backed by an exact grounded HermiT check and the global exhaustive-completion barrier is bound. It does not mean HermiT supplied a minimal logical derivation DAG. The standards claim remains disabled until the later cumulative/W3C/native qualification milestones pass.
