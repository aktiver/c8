# NGKG 1.0.0 release notes

NGKG 1.0.0 is the planned General Availability baseline for the Kubernetes-native distributed Rust RDF/OWL semantic database developed through Phase 40.13.24 and 1.0.0-RC1. GA introduces no major data-plane feature. It closes RC defects, binds all supported behavior to live same-release evidence, freezes the final public contracts, and packages immutable signed artifacts.

The supported runtime covers governed SPARQL 1.1, authorized union-default and named graphs, OWL 2 DL qualification, distributed reasoning, property paths and algebra, atomic publication, secured federation, multinode recovery, enterprise operations, and CPU-or-memory autoscaling at 80%. Kubernetes targets are RKE/RKE2, EKS, AKS, and GKE; TriG source paths include S3, Azure Blob, GCS, and qualified S3-compatible storage.

Apache Jena is not a production dependency. HermiT remains a pinned, isolated exact OWL 2 DL qualification boundary. This source archive is a candidate until the live acceptance ledger, signatures, immutable image digests, security reports, and final go certificate are supplied.
