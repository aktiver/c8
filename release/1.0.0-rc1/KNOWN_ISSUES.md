# NGKG 1.0.0-RC1 known issues

This source candidate is not yet publishable as RC1. The implementation and fail-closed release harness are present, but the following mandatory evidence is unavailable in the build environment used to assemble this archive:

- Native Rust 1.97.1 format, Clippy, locked workspace build, and full test results.
- Maven verification of the pinned OWLAPI/HermiT adapter.
- Full pinned W3C and external differential qualification.
- Real enterprise performance and capacity results.
- Complete Phase 40.13.24 RKE/RKE2, EKS, AKS, and GKE qualification certificates.
- Seventy-two-hour multi-node soak evidence.
- Approved compute, network, storage, upgrade, rollback, backup, and restore disruption evidence.
- Two isolated reproducible release builds.
- Signed multi-architecture OCI image indexes and Helm packages.
- SPDX and CycloneDX SBOMs, provenance, secret scan, license report, and zero-unapproved-critical/high-CVE report.

The RC1 prerequisite checker reports these conditions as publication blockers. None is converted into a warning or synthetic pass.
