# NGKG 1.0.0 GA known issues and publication status

This source candidate contains the GA certification implementation, final version freeze, deterministic packager, runtime-boundary audit, acceptance contracts, and operator documentation. It is **not yet a published General Availability release**.

Release publication remains blocked until the exact `1.0.0` binaries and images have complete live same-subject evidence for all 20 GA qualification areas. In particular, this build environment did not provide real RKE/RKE2, EKS, AKS, or GKE clusters; cloud workload identities and bucket mounts; HA PostgreSQL; destructive chaos approval; enterprise datasets; two isolated native Rust/container builders; SBOM/CVE/license scanners; an OCI registry; or artifact-signing keys and transparency-log access.

The checked-in support matrix therefore intentionally declares every provider unqualified. No image digest, performance number, supported Kubernetes version, signature, CVE result, availability result, or disaster-recovery result has been invented.

No ontology alignment, schema matching, or raw-data mapping capability is part of this release.
