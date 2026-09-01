# Build Status — NGKG MCP/Agent 0.3.0

## Source and structural checks

| Gate | Status | Evidence |
| --- | --- | --- |
| Phase 2 input SHA-256 | PASS | `1e58df3cba2b3cef39d5154716e980a22c3df8d9ee396a0b5161013a2797bcd3` |
| Phase 2 internal bundle manifest | PASS | Verified before extraction and modification |
| Frozen NGKG GA gates | PASS | Verified on the unchanged sibling before modification |
| JSON syntax | PASS | All Phase 3 contracts and Helm values schema parsed |
| Auth structural policy | PASS | `qualification/validate_auth_phase3.py` |
| Source manifest | PASS | Regenerated after final source freeze |
| Fresh archive extraction | PASS | Bundle manifest and cumulative static acceptance rerun |

## Toolchain-dependent checks

| Gate | Status | Required completion environment |
| --- | --- | --- |
| `cargo fmt --check` | BLOCKED | Reviewed Rust 1.97.1 toolchain |
| `cargo check --locked --workspace --all-targets` | BLOCKED | Controlled dependency mirror and generated `Cargo.lock` |
| Rust unit/integration tests | BLOCKED | Same Rust environment plus local TLS/JWKS/exchange fixtures |
| Clippy with warnings denied | BLOCKED | Same Rust environment |
| Helm lint/template | BLOCKED | Reviewed Helm toolchain |
| Kubernetes server dry run | BLOCKED | Supported Kubernetes API server |
| Live OAuth/JWKS rotation and outage | BLOCKED | Test issuer/exchange service and TLS identities |
| NGKG 1.1 subject/actor propagation | BLOCKED | NGKG 1.1 query/control builds |
| Multicloud HA qualification | BLOCKED | RKE/RKE2, EKS, AKS, and GKE clusters |

## Publication classification

The archive is a deterministic Phase 3 source candidate. It must not be
represented as a compiled, vulnerability-scanned, signed, or live-qualified
production release. Delegation mode must not be enabled against frozen NGKG
1.0 because the service-side 1.1 verifier contract is not present there.
