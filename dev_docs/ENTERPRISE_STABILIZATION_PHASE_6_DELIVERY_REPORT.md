# Enterprise Stabilization Phase 6

The native serving API now exposes `x-ngkg-native-cutover-mode` and a full canonical `x-ngkg-semantic-result-sha256` on successful SPARQL Protocol responses. These values allow the controlled differential and capacity harnesses to prove that production requests ran in required-native mode and returned deterministic complete semantics.

The candidate-level `phase6/` package implements qualification-only oracle comparison, multinode capacity and saturation, physical pod/node resource validation, 80% CPU/RAM autoscaling checks, serialized chaos recovery, four-provider portability, signed OCI/SBOM verification, two-builder reproducibility and keyless certificate issuance.

This source candidate is not live-qualified. Its production gate requires signed Phase 3, Phase 4 and Phase 5 evidence plus successful RKE2, EKS, AKS and GKE runs.
