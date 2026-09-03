# NGKG Phase 8 deployment-readiness review

Date: 2026-09-02  
Reviewed inputs: the complete codebase catalog and the Phase 8 candidate source archive.

## Decision

The original candidate was **not buildable or deployable as supplied**. It contained compile-time Rust lint contradictions, an always-offline container build that could not populate dependencies, a broken controlled MPI image build, an omitted HPC image in the controlled Helm deployment, private-registry gaps, values/schema conflicts, incorrect values-file precedence, and obsolete release gates. The corrected candidate removes those source-level blockers and is ready for a real staging build/deploy attempt, but it is **not yet proven deployable and is not production-qualified** because this review environment has no Rust, Docker, Helm, kubectl, MPI toolchain, registry, or Kubernetes cluster.

The first external go/no-go boundary is now explicit: both Rust workspaces must pass format, Clippy, test, and locked release build; all 13 images must build and push; every digest must be pullable by a cluster node; all four charts must lint/render and pass server-side dry-run; then the control, ingestion, publication, and online SPARQL paths must pass in-cluster smoke tests.

## Catalog baseline

The supplied catalog covers 1,520/1,520 files, 4,577 detected functions or routines, 1,319 types/tables, 72 Cargo packages, 113 OpenAPI operations across ten contracts, and 58 Kubernetes/Helm definition files. It correctly classifies the solution as a source-implemented distributed RDF/SPARQL and OWL 2 DL platform with MCP/agent services, not as a live-qualified release. This review used that catalog as the coverage map and checked the deployable source separately; the catalog line numbers and hashes describe the unmodified input archive, while this report describes the remediation delta.

## Blocking findings and remediation

| Severity | Original condition | Effect | Corrected state |
| --- | --- | --- | --- |
| Critical | Both Rust workspaces set `unsafe_code = "forbid"`, but each contains a required `memmap2` unsafe mapping call. | `cargo build` could not compile either affected crate; `forbid` cannot be lowered locally. | Workspace policy is `deny`; only the two reviewed mmap functions carry `#[allow(unsafe_code)]` and explicit safety invariants. |
| Critical | Agent Clippy denied `expect_used`, while the context-slice test used three `.expect()` calls. | The protected `cargo clippy ... -D warnings` gate failed. | The test returns `Result` and propagates failures with `?`; first-party `.expect()` count is zero. |
| Critical | Active Cargo and Maven Dockerfiles always used offline mode. The archive contains locks, not complete Cargo/Maven caches. | A normal digest-pinned builder could not fetch locked dependencies on its first build. | `NGKG_BUILD_OFFLINE=false` is the developer default; controlled builds explicitly use `true`; Cargo stays `--locked` in both modes. |
| Critical | The Phase 3 supply-chain script did not require or pass `MPI_BUILDER_IMAGE` or `MPI_RUNTIME_IMAGE`. | The 13th image had invalid `FROM` inputs and the controlled build stopped. | Both digest-pinned inputs are required, passed to Buildx, and recorded in provenance; the workflow now supplies them. |
| Critical | The Phase 3 deploy script omitted `ngkg-hpc-worker` from the image lock overlay. | Strict platform values remained incomplete, even when HPC was disabled. | `images.hpcWorker.repository` and digest are injected from the 13-image lock. |
| High | The local builder emitted `pullPolicy`, but the platform/workload schemas rejected that property. | `helm lint/install` failed values-schema validation. | Image schemas require `pullPolicy` with `Always`, `IfNotPresent`, or `Never`; defaults and templates consume it. |
| High | Charts had no common `imagePullSecrets` value. | Private local registries produced `ImagePullBackOff`, especially for dynamically generated worker Jobs. | All charts accept pull Secrets. Static pod specs receive them, and generated-worker ServiceAccounts carry them. External `identityRef` ServiceAccounts remain the operator's responsibility. |
| High | The documented Helm commands put generated image values before site values. | Site placeholders could override the generated digest lock. | Generated image files are last; the docs explain Helm's later-file precedence. |
| High | The protected Helm render used hard-coded Kubernetes 1.33.0. | Evidence did not represent the target cluster and used an end-of-life release. | It reads the server version from the selected kubectl context. Current deployment guidance is Kubernetes 1.35-1.37. |
| High | Phase 18 required obsolete anonymous-mmap tokens; Phase 40.12 required an obsolete unhashed ConfigMap name; Phase 40.13.21 required retired resource field names. | These static release gates failed despite the newer implementations. | Gates and Phase 18 documentation now match the checksum-verified file-backed map, content-addressed ConfigMap, and current query-audit model. |
| Medium | The agent workspace lacked its own `rust-toolchain.toml`. | Direct builds could select an uncontrolled compiler. | Both workspaces pin Rust 1.97.1, rustfmt, and Clippy. |
| Medium | The root image command bypassed the validated local build wrapper; the wrapper rejected every non-Linux host. | Environment validation was skipped, and Docker Desktop hosts could not use the supported Linux Buildx path. | The root command delegates to the wrapper; Docker Desktop/native Docker are allowed; node-unreachable localhost registries are rejected by default. |
| Medium | Platform/workload ServiceAccounts lacked configurable annotations. | IRSA, EKS Pod Identity, Azure Workload Identity, and GKE Workload Identity could not be configured cleanly for object access. | ServiceAccount annotations are values/schema-controlled for the relevant platform and online roles. |
| Medium | Quickstart chart paths were relative to a different directory than the image builder paths. | Copy/paste commands could not work from one documented working directory. | All commands are rooted at the extracted candidate directory. |

## Current API and dependency review

- The ordinary Kubernetes resources use current stable APIs: `apps/v1`, `batch/v1`, `autoscaling/v2`, `policy/v1`, `networking.k8s.io/v1`, `rbac.authorization.k8s.io/v1`, and `apiextensions.k8s.io/v1`.
- The optional integrations use current documented APIs: Kueue `kueue.x-k8s.io/v1beta1`, MPI Operator `kubeflow.org/v2beta1`, KEDA `keda.sh/v1alpha1`, Prometheus Operator `monitoring.coreos.com/v1`, and Gateway API `gateway.networking.k8s.io/v1`.
- Chart `apiVersion: v2` is correct for Helm 3+ and remains supported by Helm 4. CRDs are deliberately installed in a separate chart before custom resources; Helm does not upgrade or delete CRDs automatically.
- As of this review, Kubernetes maintains 1.37, 1.36, and 1.35 release branches. The charts retain `>=1.33` API compatibility, but staging should use a current patch of 1.35-1.37.
- Rust 1.97.1 is a real stable point release containing a compiler miscompilation fix. Rust 1.98.0 is newer, but retaining 1.97.1 is appropriate for a locked candidate until a deliberate toolchain qualification is run. Edition 2024 with resolver 3 is supported; Cargo documents resolver 3 as requiring Rust 1.84+.
- OpenAPI 3.1.0 remains a valid contract version. Control-plane source/OpenAPI parity passes at 19 operations, and online-serving parity passes at 22 operations. The cumulative 113 count includes overlapping contracts and is not 113 unique public endpoints.

Official references: [Kubernetes releases](https://kubernetes.io/releases/), [Kubernetes image pull behavior](https://kubernetes.io/docs/concepts/containers/images/), [Helm chart format](https://helm.sh/docs/topics/charts), [Helm CRD lifecycle](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/), [Kueue MPIJob integration](https://kueue.sigs.k8s.io/docs/tasks/run/kubeflow/mpijobs/), [KEDA ScaledObject](https://keda.sh/docs/2.20/reference/scaledobject-spec/), [Rust 1.97.1](https://blog.rust-lang.org/2026/07/16/Rust-1.97.1/), [Cargo resolver 3](https://doc.rust-lang.org/cargo/reference/resolver.html), and [Rust lint levels](https://doc.rust-lang.org/rustc/lints/levels.html).

## External prerequisites that remain release-blocking

1. A node-reachable TLS registry and seven reviewed digest-pinned bases: Rust builder, non-root runtime, Maven builder, Java 17+ runtime, vLLM source, MPI/OpenMP builder, and MPI runtime. The runtime bases must include CA roots and every dynamically linked library required by the built binaries.
2. A current Kubernetes 1.35-1.37 cluster with a network-policy-capable CNI, Metrics Server, correctly labelled/tainted node pools, sufficient ephemeral storage, and provider autoscaling configured outside NGKG.
3. HA PostgreSQL and S3-compatible artifact storage with separate migration/runtime identities. Cloud object access requires real workload-identity annotations or a restricted Secret.
4. Kueue before installing the default workload chart. MPI Operator is required only when `hpc.enabled=true`. KEDA plus Prometheus is required only for KEDA-owned profiles or vLLM scale-to-zero. Gateway API and Prometheus Operator CRDs are required only when their corresponding templates are enabled.
5. A Java 17-compatible HermiT adapter/runtime and the exact adapter SHA-256. The reference/reasoner workload also needs its shared-token and workspace contracts.
6. Python packages from `NGKG_1_0_0_GA/conformance/python-requirements.lock` for the full schema and standards gates. Twelve static scripts were dependency-blocked here solely because `jsonschema` was absent.
7. Fresh build, SBOM, vulnerability, signature, provenance, five-provider, chaos, autoscaling, recovery, MPI, and semantic differential evidence. The existing GA freeze correctly detects the remediation as source drift and must not be treated as valid evidence for the modified archive.

## Validation performed here

| Check | Result |
| --- | --- |
| Deployment static preflight | PASS: two scoped unsafe blocks, zero first-party `.expect()`, 13 Dockerfiles/images, three deployable charts, three CRDs, 19 control and 22 online operations. |
| Phase 3 source acceptance | PASS, including image catalog/build contracts and Python unit test. |
| Phase 8 acceptance | PASS: 13 images, three HPC contracts, 113 cumulative OpenAPI operations, OpenMP kernel check. |
| Core structural validation | PASS: 1,214 files in the core root, zero structural errors. |
| OpenAPI/runtime parity | PASS: control 19; online 22. |
| Helm cross-field values validators | PASS for platform and workload defaults. |
| Static phase verifier sweep | 56 passed; 12 could not start without `jsonschema`; obsolete Phase 18, 40.12, and 40.13.21 checks were corrected and then passed. The RC1-only verifier is inapplicable to the 1.0.0 tree. |
| Native Rust build/test/Clippy | NOT RUN: `cargo` and `rustc` unavailable. |
| OCI build/push/pull | NOT RUN: Docker/Buildx and a registry unavailable. |
| Helm lint/template and API-server dry-run | NOT RUN: Helm and kubectl unavailable. |
| Live install, migrations, readiness, certified-TriG upload, ingestion, publication, SPARQL | NOT RUN: no cluster, PostgreSQL, or object store. |

## REST, TriG, OWL 2 DL, and SPARQL boundary

The control service exposes Swagger at `/docs`, YAML/JSON contracts at `/openapi.yaml` and `/openapi.json`, dataset creation, direct TriG upload, ingestion/jobs, snapshot read/publication, and storage recovery. The online service separately exposes Swagger and standards-shaped SPARQL GET/POST; POST supports `application/sparql-query` and `application/x-www-form-urlencoded`, consistent with the [SPARQL 1.1 Protocol](https://www.w3.org/TR/sparql11-protocol/). TriG handling is aligned with the [RDF 1.1 TriG recommendation](https://www.w3.org/TR/trig/), and the semantic qualification model targets [OWL 2 Direct Semantics](https://www.w3.org/TR/owl2-direct-semantics/).

A `201` response from the direct upload route proves that one UTF-8 `application/trig` object was checksum-verified, parsed, summarized, and stored. The database deliberately separates immutable source upload from the explicit ingestion and publication operations that partition the RDF dataset, build the distributed indexes and activate a queryable snapshot. The upstream alignment automation owns ontology alignment and OWL 2 DL certification; NGKG consumes those certified inputs and preserves their checksum/evidence binding while executing its existing database validation, compilation, reasoning and distributed-query logic. This explicit lifecycle is not a missing alignment or certification service.

Use `DEPLOYMENT_TESTING_RUNBOOK.md` for the staging sequence and stop at the first failed go/no-go gate.

## Final assessment

**Original archive: NO-GO. Corrected archive: GO for a controlled staging build and deployment attempt; NO-GO for claims of successful deployment or production readiness until the external gates above pass.** The source-level failures discovered in this review have been remediated, but only execution on the intended toolchain, registry, infrastructure, and semantic corpus can establish that the system actually deploys and answers correctly.
