# NGKG HermiT adapter

This adapter is the first real offline reasoner boundary. It verifies immutable input hashes, loads the governed ontology/core-ABox bundle through OWLAPI, invokes HermiT, rejects inconsistent input, and writes a finite named-entity materialization plus a machine-readable report.

It does **not** claim that its output is a finite closure of every OWL 2 DL consequence, and this first adapter does not emit a proof DAG. The NGKG reference compiler therefore certifies only exact query hashes on one immutable snapshot after independent answer comparison. The later exact-residual service and proof adapter remain required for arbitrary production OWL 2 DL query answering.

Build with Java 17 and Maven:

```bash
mvn --batch-mode --no-transfer-progress clean package
```

The shaded artifact is `target/ngkg-hermit-adapter.jar`. HermiT is LGPL-licensed; deployment and redistribution must comply with its license and the licenses of its dependencies.

### Phase 40.2 datatype policy

Reasoner request format 2 carries `datatypePolicyPath` and `datatypePolicySha256`. The adapter verifies the exact policy bytes and rejects any datatype in the merged ontology signature that is not in the operator-supported map before HermiT reasoning. Reasoner report format 3 binds the same digest as `datatypePolicySha256`.


## Phase 40.5

Request format v3 adds `outputOwlProfileQualificationPath`. The adapter emits checksum-bound `owl-profile-qualification.json` from the actual OWLAPI-loaded ontology set, version identifiers, resolved local import edges, and merged OWL 2 DL profile report. Reasoner report v4 binds that artifact with `owlProfileQualificationSha256`.

Phase 40.6 request format v4 adds `outputOwlConsistencyQualificationPath`. The adapter emits checksum-bound `owl-consistency-qualification.json` from HermiT `OWLReasoner.isConsistent()` over the complete merged ontology. Reasoner report v5 binds that artifact with `owlConsistencyQualificationSha256`; inconsistent ontologies are recorded with `publicationPermitted=false` and are rejected by Rust.

## Phase 40.9 exact support evidence

The direct exact adapter now emits, for every entailed candidate, `groundedRdfSha256`, `logicalAxiomsSha256`, and `logicalAxiomCount`. Rust uses those immutable facts plus candidate/request/snapshot identity to construct deterministic reasoner-check support IDs and a global proof/support manifest. This is complete answer-support coverage, not a HermiT derivation DAG; the adapter still makes no claim that HermiT exposes a derivation proof graph.
