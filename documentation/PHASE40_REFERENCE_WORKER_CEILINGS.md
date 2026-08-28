# Phase 40 reference-worker ceilings

`ngkg-reference-worker direct-bgp` requires these trusted environment variables:

- `NGKG_PHASE40_DIRECT_MAX_CANDIDATE_BINDINGS`
- `NGKG_PHASE40_DIRECT_MAX_PARTITION_CANDIDATES`
- `NGKG_PHASE40_DIRECT_MAX_EXACT_PARTITIONS`
- `NGKG_PHASE40_DIRECT_MAX_GROUNDED_AXIOMS_PER_CANDIDATE`
- `NGKG_PHASE40_DIRECT_MAX_GROUNDED_RDF_BYTES_PER_CANDIDATE`
- `NGKG_PHASE40_DIRECT_REASONER_CONCURRENCY`
- `NGKG_PHASE40_DIRECT_REASONER_HEAP_MIB_PER_LANE`
- `NGKG_PHASE40_DIRECT_REASONER_TIMEOUT_SECONDS`
- `NGKG_PHASE40_DIRECT_MAX_CERTIFICATE_BYTES`
- `NGKG_PHASE40_DIRECT_MAX_PROOF_SUPPORT_IDS`

They are rendered by `charts/ngkg-platform/templates/phase40-reference-ceilings.yaml` from the authoritative Phase 40.10 Helm values. Job-envelope values are sub-ceilings: they may lower an execution budget but cannot raise the trusted limit.
