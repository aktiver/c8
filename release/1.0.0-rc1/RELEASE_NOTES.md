# NGKG 1.0.0-RC1 release notes

NGKG 1.0.0-RC1 freezes the public API, schemas, Kubernetes contracts, storage layouts, semantic evidence formats, and operational configuration built through Phase 40.13.24. It adds release certification and deterministic packaging only; it does not add query, reasoning, storage, federation, security, or autoscaling features.

The production implementation remains Rust. Apache Jena is external qualification infrastructure only. HermiT remains the pinned exact OWL 2 DL qualification boundary and is not represented as a generally distributed reasoner.

This archive is a source-implemented RC1 candidate. Publication remains blocked until the live qualification, reproducible-build, signed-artifact, SBOM, vulnerability, license, and five-provider Kubernetes evidence listed in `KNOWN_ISSUES.md` is supplied.
