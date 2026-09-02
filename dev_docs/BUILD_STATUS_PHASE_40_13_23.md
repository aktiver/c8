# Phase 40.13.23 build status

Status: **source-implemented candidate; live performance qualification pending**

Parent archive: `NGKG_PHASE_40_13_22_CANDIDATE(1).zip`  
Parent SHA-256: `8ea7c5f3df37c0f76caf14dbeec8e87287754c2008447a184c0a7527a2b08816`

## Green gates

- Parent ZIP safety/integrity and all 1,069 inherited manifest hashes passed.
- Phase 40.13.23 static and executable synthetic performance barrier passed.
- All seven required workload families, external-baseline isolation, thresholds, resource ceilings, dense trials, and certificate construction were exercised.
- A deliberately corrupted completed observation was rejected.
- Phase 40.13.22 static qualification and API/OpenAPI parity remained green.
- JSON, TOML, non-template YAML, Maven XML, Python, and shell structural checks passed.
- 23 of 24 cumulative Phase 40.13 static verifiers passed; the inherited umbrella verifier was dependency-blocked by unavailable Python `jsonschema`, with no asserted code failure.

## Unavailable/live gates

- Cargo check/test/Clippy/Rustfmt with Rust 1.97.1.
- Signed NGKG release image build and SBOM.
- Three real 100-GiB-or-larger representative enterprise datasets.
- Same-hardware external Apache Jena execution.
- RKE/RKE2, EKS, AKS, and GKE 1/2/4-node and 1/8/32/100/250-client runs.
- Live 80% autoscaling, scale-from-zero, node-loss, recovery, and cost evidence.
- Sustained soak and final performance/capacity certificate.

No performance, capacity, cost, or comparative speed claim is enabled by static evidence.
