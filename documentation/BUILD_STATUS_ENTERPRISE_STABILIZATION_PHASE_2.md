# Enterprise Stabilization Phase 2 build status

Date: 2026-09-01

This is a source-implemented native-build-closure candidate. The Rust and Helm gates are complete in the available environment. OCI assembly and the Java adapter dependency fetch remain blocked by environment prerequisites and are not represented as successful builds.

## Gate results

| Gate | Result | Evidence |
|---|---|---|
| Supplied archive integrity | PASS | Input SHA-256 `afc8fb7656e03fb4497024cd174cab8f10dd880905dbfdb49232937b76208cda` |
| Agent dependency lock | PASS | Cargo generated and consumed `ngkg-agents/Cargo.lock` with `--locked --offline` |
| Core dependency lock | PASS | `NGKG_1_0_0_GA/Cargo.lock` refreshed and consumed with `--locked --offline` |
| Rust formatting | PASS | `cargo fmt --all --check` in both workspaces |
| Rust compilation | PASS | 48-member core and 20-member agent workspaces |
| Rust Clippy | PASS | Both workspaces, all targets, `-D warnings` after reviewed workspace lint policy |
| Rust tests | PASS | 268 core plus 29 agent tests; 297 total |
| Rust release payloads | PASS | Ten core service binaries and eight agent service binaries built with `--release --locked --offline` |
| Core OpenAPI parity | PASS | 19 control-plane and 20 online-data-plane operations |
| Helm lint/render | PASS | `ngkg-crds`, `ngkg-platform`, `ngkg-workloads`, and `ngkg-agents`; Kubernetes 1.33.0 |
| Immutable image input enforcement | PASS | Negative test rejects mutable tag references with exit status 2 |
| HermiT adapter Maven build | BLOCKED | Maven Central DNS unavailable; plugin dependency was not cached |
| OCI image assembly | BLOCKED | No Docker/Podman/BuildKit runtime and no approved digest-pinned base image set were available |
| Live Kubernetes qualification | NOT RUN | Requires RKE2/EKS/AKS/GKE clusters and provider infrastructure |

## Toolchain

- Rust/Cargo 1.97.1, host `x86_64-unknown-linux-gnu`, LLVM 22.1.6.
- Helm 3.17.1.
- Maven 3.9.9 with Java 17.0.20.
- Kubernetes render target 1.33.0.

## Release payload hashes

These hashes identify the binaries built in this environment. Build outputs are deliberately excluded from the source archive.

| Binary | SHA-256 |
|---|---|
| `ngkg-api` | `9f0d5efc58e81e926c446e05e7244bc347b7fe05899e3da2c696077e8ceddb34` |
| `ngkg-catalog-migrator` | `9fbf8cab4ff57c4ee260647fdeef5d21cff98d53d6a46b041945856dbbbcfce1` |
| `ngkg-distributed-operator` | `b528c0fa5f33825d5406fb33cc2daecffa50a865477c66b6505af5f3a02a4d02` |
| `ngkg-distributed-worker` | `053bad66b45100bc2fd89341d300664680e1baa992067b41bd20344239001ac5` |
| `ngkg-operator` | `7c56e4977eaa9d8587c2f992636106c9fcbcf79dbb1a444c6c8a3c3b745c8548` |
| `ngkg-storage-recovery-operator` | `71ff3232b1c60c194fc15b57b7ac55ad0f8b24cd37548d749e6c00f173de4256` |
| `ngkg-storage-recovery-worker` | `7301695026f0f9626a8fb9de1fcd8a232b86cb095e843dd8cf8b2b0969efcc3a` |
| `ngkg-direct-reasoner-worker` | `7f63a421fec528a5a9e5edadb6e445c78d16249334695907c5fba87c15b30e45` |
| `ngkg-reference-worker` | `010367661758c1839f0f0d561699e715bd906ce3c7d20c876ca408228f49c4fb` |
| `ngkg-online-serving` | `46518b3a6155829105373ad1c64dd44116776a50291547a007f285394b20304f` |
| `ngkg-mcp-gateway` | `a4791497cc83d12b079ad18d2a54536002de0555e7f5b4cd288446b16486ec2e` |
| `ngkg-agent-catalog-migrator` | `46420ff3201f109a87cc6f51573199c00bcd30bc20412f3213fe556a119992eb` |
| `ngkg-prompt-compiler` | `c36b688686da5cd0218d7ec510b55cb1e521957da4702c3cbd224717f244d294` |
| `ngkg-qualification-worker` | `c1f9e6468c98f3d25d521fd7314c672c1dbc1599b98433d88299fe8073dc41f9` |
| `ngkg-inference-gateway` | `7ebf60697aaa0e5c20a83c5796474f39a728e776c769b5843e15d3df0e04aa23` |
| `ngkg-vllm-pod-agent` | `762b367a00b6816afd17146eb5a06ea3a298c66c0638c2a0ab9734bece4a9c95` |
| `ngkg-context-slice-broker` | `3204b69040b9e141913cf5941871ad2913f40613f0b2437e8fbb5a2244ba0c60` |
| `ngkg-context-slice-gc` | `0686a3d62e946db67b6bcd5b8bda607f90f01ea523ccfdfc18b19ac6d510d589` |

## Exact external closure commands

Build all 11 application images only on a release runner with an OCI builder, locally available dependency inputs, and four approved base images referenced by `@sha256:` digest:

```bash
export NGKG_RUST_BUILDER_IMAGE='registry.example/rust@sha256:<64-hex-digest>'
export NGKG_RUNTIME_IMAGE='registry.example/nonroot@sha256:<64-hex-digest>'
export NGKG_MAVEN_BUILDER_IMAGE='registry.example/maven@sha256:<64-hex-digest>'
export NGKG_JAVA_RUNTIME_IMAGE='registry.example/java@sha256:<64-hex-digest>'
export NGKG_IMAGE_REGISTRY='registry.example/ngkg'
export NGKG_IMAGE_TAG='<source-revision>'
./build_all_images.sh
```

The build driver disables Dockerfile network access and fails closed on non-digest base references. Base layers and Maven dependencies therefore must be pre-provisioned by the controlled release runner.
