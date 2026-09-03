# Enterprise Stabilization Phase 3 build status

Date: 2026-09-01

This is a source-implemented controlled-release and live-qualification candidate. Source controls pass in the available environment. No OCI or live-infrastructure success is claimed without externally signed evidence.

## Gate results

| Gate | Result | Evidence |
|---|---|---|
| Supplied archive integrity | PASS | Input SHA-256 `20967ad670215078467158cc4130d5c373f8100df90e4959fecb37b205ac0f53`; 1,390/1,390 internal payload hashes |
| Phase 3 source acceptance | PASS | Shell syntax, Python compilation, strict JSON, exact inventories and release-control markers |
| Controlled workflow YAML | PASS | Parsed successfully; manual approval, protected environment, pinned actions and four-provider matrix present |
| Evidence tamper rejection | PASS | Synthetic unit fixture issues only with complete inputs and rejects a modified SBOM hash |
| OCI build and registry push | NOT RUN | Docker, Podman and BuildKit are unavailable on this runner |
| SBOM and vulnerability scan | NOT RUN | Syft, Grype and Trivy are unavailable on this runner |
| OCI/evidence signing | NOT RUN | Cosign and registry access are unavailable on this runner |
| PostgreSQL migration/HA | NOT RUN | psql, pg_dump and an approved HA PostgreSQL service are unavailable |
| Helm deployment | NOT RUN | Helm and Kubernetes contexts are unavailable |
| RKE2 integration | NOT RUN | Dedicated approved cluster and external failure drivers required |
| EKS integration | NOT RUN | Dedicated approved cluster and external failure drivers required |
| AKS integration | NOT RUN | Dedicated approved cluster and external failure drivers required |
| GKE integration | NOT RUN | Dedicated approved cluster and external failure drivers required |
| HermiT runtime qualification | NOT RUN | Requires built Java adapter image and live workload |
| 80% CPU/RAM autoscaling | NOT RUN | Requires metrics, load generation and provider node provisioning |
| Recovery and node-loss | NOT RUN | Requires approved destructive drivers and isolated clusters |
| GPU/vLLM qualification | NOT RUN | Requires GPU node pools, KEDA and real inference traffic |
| Tenant-isolation integration | NOT RUN | Requires two real tenant identities and deployed services |
| Phase 3 certificate | NOT ISSUED | Deliberately fail-closed until every signed external gate passes |

## Required signed closure

The final issuer requires exactly twelve dual-architecture image records with zero high or critical findings, signed SPDX/provenance attestations, qualified core and agent migrations on an HA primary, and exactly twelve complete scenarios from each of RKE2, EKS, AKS and GKE. It recomputes checksums and verifies Cosign identities before producing `phase3-certificate.json`.

Use `phase3/README.md` for runner inputs and commands. The complete automated entry point is `.github/workflows/phase3-controlled-release.yml`.
