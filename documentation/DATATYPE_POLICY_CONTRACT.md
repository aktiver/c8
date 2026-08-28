# NGKG Phase 40.2 Datatype Policy Contract

Phase 40.2 makes datatype support explicit, fail-closed and checksum-bound. The repository ships `policies/owl-direct-datatype-policy.json`; uploaded compile manifests cannot replace it. The reference compiler copies the exact bytes into `reasoner/datatype-policy.json`, records the SHA-256, validates every reasoning-visible normalized RDF literal, and sends the same path/hash to the trusted OWLAPI/HermiT adapter.

The policy currently publishes 31 datatype IRIs and the lexical validator used for each. Unsupported datatypes and ill-typed literals reject snapshot compilation. Source lexical forms are preserved rather than silently canonicalized. `maxLexicalBytes`, integer digit limits, date/time year digit limits and the documented ASCII XML-name subset bound hostile or pathological lexical inputs.

## HPC validation

Literal validation is embarrassingly parallel and can dominate preprocessing on very large ABoxes. `validate_reasoning_literals` therefore partitions the already-normalized fact vector into at most 32 deterministic chunks using `std::thread::available_parallelism()`. Each lane performs independent read-only validation and local datatype counting. Results merge through ordered maps, and failures are reduced by the lowest original fact index, so one core and many cores accept or reject exactly the same dataset. This phase does not introduce nested BLAS/OpenMP work or alter query semantics.

## Cross-language enforcement

The HermiT request format is version 3 and includes `datatypePolicyPath` and `datatypePolicySha256`. The adapter re-hashes and parses that policy before reasoning, then rejects any datatype in the merged OWL ontology signature that is absent from the supported map. The reasoner report format is version 4 and retains required `datatypePolicySha256` while adding Phase 40.5 profile/import evidence binding; Rust requires it to equal the request and snapshot artifact hash.

The policy is intentionally narrower than every lexical form XML Schema has ever defined. Unsupported XML-name Unicode classes and unsupported OWL/XSD datatypes fail explicitly rather than being approximated. Expanding the policy requires a versioned policy/schema/test change.

## Inherited contract supersession

Phase 36 originally pinned `reasoner/report.json` to format version 2. Phase 40.2 first superseded that envelope with v3 for `datatypePolicySha256`; Phase 40.5 strictly supersedes the envelope again with v4 for `owlProfileQualificationSha256`. The datatype-policy binding remains mandatory and unchanged.
