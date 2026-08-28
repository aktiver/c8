# Phase 40.1 OWL Signature Contract

`reasoner/owl-signature.json` is the deterministic, checksum-bound entity signature of the exact merged ontology presented to OWLAPI/HermiT. It is an offline snapshot artifact; it is not a query result and does not by itself claim complete OWL 2 Direct query answering.

The artifact binds `datasetId`, `snapshotId`, and the aggregate SHA-256 of every reasoner input. `ontologyDocuments` records the SHA-256 and declared ontology/version IRIs of every input, including the generated core ABox document with an empty ontology-IRI list. `imports` records the resolved import IRIs observed by OWLAPI.

The entity signature contains strictly sorted, duplicate-free absolute IRIs for classes, object properties, data properties, annotation properties, named individuals, and datatypes. The HermiT adapter generates the signature from the already-loaded merged ontology, writes it before profile/consistency qualification, hashes the exact JSON bytes, and places that digest in `reasoner/report.json` as `owlSignatureSha256`.

Rust does not trust the adapter output merely because the process exits successfully. `ngkg-reference` reparses the signature using a closed `serde(deny_unknown_fields)` model, requires its dataset/snapshot/aggregate-input identity to match the request, requires `ontologyDocuments` to exactly match the checksum-bound request inputs, checks deterministic ordering and IRI validity, hashes the file, and requires the digest to equal the reasoner report. The compiler then includes the signature in the immutable snapshot artifact inventory and surfaces its digest in the snapshot manifest and certification verification record.

This phase intentionally does not parallelize signature extraction. OWLAPI already holds the merged ontology in memory for profile/consistency work; a single deterministic traversal and sorted serialization avoids cross-thread ordering ambiguity and adds negligible work relative to HermiT reasoning. Existing Arrow/Parquet/mmap/NVMe/Grace-join HPC paths remain unchanged.
