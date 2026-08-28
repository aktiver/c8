# Phase 40.3 Direct-BGP Result Contract

Phase 40.3 defines the closed result object that later OWL Direct legality and exact reasoner phases must populate. It is deliberately a **contract phase**: no arbitrary Direct-BGP completeness claim is enabled yet.

A successful result is bound to the dataset, immutable snapshot, exact query bytes, canonical BGP, active dataset, graph authorization set, Phase 40.1 OWL signature and Phase 40.2 datatype policy. Its graph context is either the active default graph hash or one named graph IRI.

RDF terms are lossless: IRI, scoped blank node, or literal with explicit lexical form/datatype and optional language. Duplicate SPARQL solution mappings are compressed as an exact positive `multiplicity`; `solutionMultiplicityTotal` must equal the checked sum without expanding the bag.

`status=complete` is legal only with `exactness=exact`, `completeness=complete`, and no error. `status=failed` cannot carry partial successful solutions and must carry a bounded machine-readable failure. This preserves fail-closed behavior for later timeout/resource/coverage failures.

For large result vectors the Rust validator uses up to 32 CPU-aware deterministic lanes. Each lane validates disjoint solution chunks without expanding duplicate mappings; if multiple chunks are invalid, the lowest original solution index is reported. Parallelism therefore changes validation throughput only, never the accepted result or failure identity.

The JSON Schema is `contracts/direct-bgp-result.schema.json`; `scripts/validate_direct_bgp_result.py` independently checks schema plus cross-field semantics. Phase 40.7 decides BGP legality, Phase 40.8 produces bounded exact results, and Phase 40.9 binds each returned solution multiplicity to deterministic exact-reasoner support evidence plus a global completion barrier in the Direct proof manifest.
