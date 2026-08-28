# NGKG 1.0.x maintenance policy

The 1.0.x line accepts compatible security patches, correctness fixes, operational hardening, dependency updates, and documentation corrections. Every change receives a defect identity, compatibility review, regression coverage, and requalification of affected gates.

Breaking API, schema, CRD, database, snapshot, partition, proof, checkpoint, storage-layout, or Helm changes require a new release line. Major query, reasoning, storage, alignment, mapping, or autoscaling features do not enter a 1.0.x patch silently.

Security notices identify affected versions, severity, mitigation, fixed version, artifact digests, and any required rotation or recovery action. Patch artifacts retain the same reproducibility, SBOM, provenance, signature, checksum, and immutable-publication requirements as 1.0.0.
