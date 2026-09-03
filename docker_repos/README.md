# Local registry image build and Helm deployment

All 13 deployable OCI images are built from the repository root with Dockerfiles in this directory. The thirteenth image is the optional finite MPI/OpenMP/Parquet worker and is never used by long-lived gateways. The build refuses mutable builder/runtime inputs so an unreviewed base-image update cannot silently change a supposedly identical NGKG release.

## Prerequisites

Install Docker with Buildx, Python 3.11+, Helm 3 or 4, kubectl, PostgreSQL access, Metrics Server, and Kueue. Install KEDA plus Prometheus only for profiles that select KEDA-owned autoscaling or enable vLLM scale-to-zero. Docker Desktop or a native Linux Docker daemon can run the Linux Buildx build. The Kubernetes nodes must be able to resolve and pull from the value in `NGKG_LOCAL_REGISTRY`. For a registry with a private CA, install that CA in every node's container runtime trust store.

Start a registry reachable from every cluster node. This Docker example binds port 5000 on the build host; replace `registry.lan:5000` with the DNS name or IP that the Kubernetes nodes use:

```bash
docker run -d --restart=always --name ngkg-registry -p 5000:5000 registry:2
export NGKG_LOCAL_REGISTRY='registry.lan:5000'
export NGKG_LOCAL_REGISTRY_NAMESPACE='ngkg'
```

Provide reviewed, locally available, digest-pinned build dependencies:

```bash
export NGKG_RUST_BUILDER_IMAGE='registry.lan:5000/base/rust@sha256:<64-hex>'
export NGKG_RUNTIME_IMAGE='registry.lan:5000/base/nonroot-runtime@sha256:<64-hex>'
export NGKG_MAVEN_BUILDER_IMAGE='registry.lan:5000/base/maven@sha256:<64-hex>'
export NGKG_JAVA_RUNTIME_IMAGE='registry.lan:5000/base/java-runtime@sha256:<64-hex>'
export NGKG_VLLM_SOURCE_IMAGE='registry.lan:5000/base/vllm@sha256:<64-hex>'
export NGKG_MPI_BUILDER_IMAGE='registry.lan:5000/base/openmpi-build@sha256:<64-hex>'
export NGKG_MPI_RUNTIME_IMAGE='registry.lan:5000/base/openmpi-runtime@sha256:<64-hex>'
```

Set `NGKG_BUILD_OFFLINE=false` for the first local build so Cargo and Maven can populate their dependency caches from the locked manifests. Set it to `true` for a controlled or air-gapped build only when the Rust and Maven builder images already contain every locked dependency. Both modes keep Cargo's `--locked` invariant. The Rust builder must contain the pinned Rust 1.97.1 toolchain. Runtime images must contain the CA bundle and dynamic libraries required by the compiled binaries; Java runtime images must provide Java 17 or newer for the adapter compiled with `maven.compiler.release=17`. The MPI builder must provide `mpicc`, a C compiler and OpenMP headers. The MPI runtime must be a reviewed, non-root, MPI-Operator-compatible worker image whose inherited entrypoint starts its rank transport, and it must contain `/usr/bin/mpirun` plus matching MPI/OpenMP shared libraries; the launcher template deliberately overrides that entrypoint while worker pods deliberately retain it.

## Build and push locally

From the candidate root:

```bash
set -a
source docker_repos/example.env
set +a
./docker_repos/build_all_local.sh
```

`build_all_local.sh` is the supported entry point. It verifies Docker/Buildx, rejects a registry address that cluster nodes cannot normally reach, validates all digest-pinned build inputs, optionally logs in through password-stdin, builds and pushes every image, resolves registry digests, and validates the final image lock.

The builder uses the repository root as the context, pushes to the local registry, resolves the registry manifest digest, and writes:

- `docker_repos/generated/image-lock.json`
- `docker_repos/generated/platform-local-registry-values.yaml`
- `docker_repos/generated/workloads-local-registry-values.yaml`
- `docker_repos/generated/agents-local-registry-values.yaml`

Helm therefore deploys `repository@sha256:digest`, never a mutable local tag.

## Helm commands

Create the namespace and required secrets before installing. Values shown as files must contain real site credentials or workload-identity configuration; never put credentials directly on a Helm command line.

```bash
kubectl create namespace ngkg
kubectl label namespace ngkg ngkg.io/kueue-enabled=true --overwrite

# Required only when the target registry needs basic registry credentials.
kubectl -n ngkg create secret docker-registry ngkg-registry \
  --docker-server="${NGKG_LOCAL_REGISTRY}" \
  --docker-username="${NGKG_LOCAL_REGISTRY_USERNAME}" \
  --docker-password="${NGKG_LOCAL_REGISTRY_PASSWORD}"

helm upgrade --install ngkg-crds NGKG_1_0_0_GA/charts/ngkg-crds \
  --namespace ngkg --wait

helm upgrade --install ngkg-platform NGKG_1_0_0_GA/charts/ngkg-platform \
  --namespace ngkg \
  --values site/platform-values.yaml \
  --values docker_repos/generated/platform-local-registry-values.yaml \
  --atomic --wait --wait-for-jobs --timeout 30m

helm upgrade --install ngkg-workloads NGKG_1_0_0_GA/charts/ngkg-workloads \
  --namespace ngkg \
  --values site/workloads-values.yaml \
  --values docker_repos/generated/workloads-local-registry-values.yaml \
  --atomic --wait --timeout 30m

helm upgrade --install ngkg-agents ngkg-agents/charts/ngkg-agents \
  --namespace ngkg \
  --values site/agents-values.yaml \
  --values docker_repos/generated/agents-local-registry-values.yaml \
  --atomic --wait --wait-for-jobs --timeout 30m
```

Helm gives later values files precedence. Keep each generated image file last so
site placeholders cannot override the digest-pinned image coordinates.

For a private registry, put the following in each site values file. The charts add it to every static Pod and generated worker ServiceAccount:

```yaml
imagePullSecrets:
  - name: ngkg-registry
```

Any separate ServiceAccount supplied as a cloud-import `identityRef` must also reference that same namespaced pull Secret. Registry login on the build host authenticates the push only; it does not authenticate cluster nodes.

Confirm that every running container uses the generated digest lock:

```bash
kubectl -n ngkg get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\\n"}{range .status.containerStatuses[*]}  {.name}{" = "}{.imageID}{"\\n"}{end}{end}'
kubectl -n ngkg get hpa
kubectl -n ngkg get scaledobjects.keda.sh
```

CPU and memory HPAs use 80% targets. KEDA scales durable queue consumers, including eligible workers that scale from zero. Kubernetes node provisioning remains the responsibility of the configured RKE/RKE2, EKS, AKS, or GKE node autoscaler; NGKG creates resource requests and pending pods rather than calling cloud scaling APIs directly.

## Image-to-chart ownership

The authoritative mapping is `docker_repos/images.json`. `validate_image_parity.py` fails when a release image has no Dockerfile, when the Phase 3 and local catalogs differ, when a chart mapping is absent, or when the generated lock is incomplete. The common `ngkg-agents` image intentionally contains the gateway, orchestrator, memory, prompt compiler, tool broker, qualification, inference, vLLM pod-agent, context broker, and context GC binaries; each workload selects its own command and environment in Helm. `ngkg-hpc-worker` is isolated because its MPI/OpenMP native runtime is not appropriate for API, MCP or ordinary online query pods.
