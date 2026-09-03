# Enterprise Stabilization Phase 3 controlled-runner guide

Phase 3 converts the Phase 2 source closure into a signed deployment and live-infrastructure qualification. It is intentionally fail-closed: the source archive contains no fabricated image digest, vulnerability result, database result, cluster result, or qualification certificate.

## Controlled runner prerequisites

The protected `ngkg-phase3-release` environment must use ephemeral or scrubbed self-hosted runners with:

- Docker Buildx with `linux/amd64` and `linux/arm64` builders.
- `crane`, `syft`, `grype`, `trivy`, `cosign`, `jq`, Helm, `kubectl`, `psql`, and `pg_dump`.
- Rust, Maven, and MPI/OpenMP builder images whose complete Cargo registry and Maven repository are already populated. Dockerfile execution is network-disabled and both package managers run offline. The MPI runtime image must contain the matching MPI/OpenMP shared libraries and rank-transport entrypoint.
- Registry push access through workload identity.
- Cosign keyless OIDC or an approved KMS key. No private signing key belongs in the repository.
- A PostgreSQL primary with at least one visible streaming replica for each qualification database.
- Dedicated RKE2, EKS, AKS, and GKE qualification clusters, each spanning at least three labeled failure domains and containing or able to provision a GPU node.
- Provider-specific workload identity, object storage, KMS, CSI, load-generator and node-failure drivers prepared outside the repository.

The runner must also provide `/etc/ngkg-phase3/toolchain-lock.json`, derived from `phase3/config/toolchain-lock.example.json`, with reviewed SHA-256 hashes for every release executable. Phase 3 hashes the resolved binaries before use and binds that evidence into the image lock.

Every base image and the approved vLLM source must be named by `repository@sha256:<digest>`. Mutable tags fail before a build begins.

## Image closure

`phase3/config/images.json` is the authoritative thirteen-image inventory. Twelve NGKG images are built, including the finite MPI/OpenMP worker; the approved upstream vLLM image is copied by digest into the controlled registry so the exact deployed artifact can be scanned, attested and signed under the same release policy.

Run manually with the environment described in `phase3/scripts/build_supply_chain.sh`:

```bash
./phase3/scripts/build_supply_chain.sh
```

For every image the script:

1. Builds or mirrors the multi-architecture OCI index.
2. Resolves its registry digest.
3. Generates SPDX JSON and CycloneDX JSON SBOMs.
4. Runs Grype and Trivy and rejects any high or critical finding.
5. Generates source-, builder-, base-image- and platform-bound provenance.
6. Signs the image and attaches signed SBOM/provenance attestations.
7. Verifies the signature before recording evidence.
8. Adds the immutable repository/digest pair to `image-lock.json`.

Security exceptions are not accepted by this script. A reviewed exception process must change the explicit policy and be independently approved; it must never convert scanner failure into `complete: true`.

## PostgreSQL qualification

Core NGKG and agent migrations may use separate databases because both SQLx migration sets use numeric versions beginning at one. `qualify_postgres.sh` therefore requires separate migration-owner and runtime-role URLs for each set.

The script records schema-only hashes before and after migration, runs the exact digest-locked migrator containers, checks the SQLx ledgers, proves it is connected to a primary with a streaming replica, and executes transactional negative tests for forced RLS, immutable audit/agent rows, illegal transitions and cross-tenant visibility. Test rows are rolled back.

```bash
export NGKG_PHASE3_IMAGE_LOCK=/secure/evidence/supply-chain/image-lock.json
export NGKG_CORE_MIGRATION_DATABASE_URL='postgresql://...?...sslmode=verify-full'
export NGKG_CORE_RUNTIME_DATABASE_URL='postgresql://...?...sslmode=verify-full'
export NGKG_AGENT_MIGRATION_DATABASE_URL='postgresql://...?...sslmode=verify-full'
export NGKG_AGENT_RUNTIME_DATABASE_URL='postgresql://...?...sslmode=verify-full'
export NGKG_AGENT_RUNTIME_DATABASE_ROLE='ngkg_agent_runtime'
./phase3/scripts/qualify_postgres.sh
./phase3/scripts/sign_evidence.sh phase3-evidence/postgres/postgres-evidence.json phase3-evidence/signatures
```

## Provider deployment

Each provider runner owns three approved values files. Values contain references to existing Secrets, workload identities, KMS keys, bucket mounts, CIDRs and provider node classes—not credentials. `deploy_cluster.sh` overlays the signed image lock, renders all manifests, records their checksum and installs CRDs, platform, workloads and agents with Helm `--atomic --wait`; migration Jobs also require `--wait-for-jobs`.

Required variables are `NGKG_PROVIDER`, `NGKG_KUBECTL_CONTEXT`, `NGKG_NAMESPACE`, `NGKG_PHASE3_IMAGE_LOCK`, `NGKG_PLATFORM_VALUES`, `NGKG_WORKLOAD_VALUES`, `NGKG_AGENT_VALUES`, and `NGKG_PHASE3_DEPLOYMENT_EVIDENCE`.

## Live HA qualification

Copy `phase3/config/cluster.example.json` into the protected runner configuration and replace every placeholder. Tokens must be in mode-0600 files. HTTP probes require TLS. Destructive tests run only when `qualificationCluster` is true and the supplied approval file exactly matches `approvalEvidenceSha256`.

The runner requires all twelve scenarios:

1. Control, online and agent health plus OpenAPI/Swagger route exposure.
2. Exact cross-domain OWL 2 DL SPARQL result equality.
3. MCP initialize, tools/list and snapshot-bound query execution.
4. Exact HermiT fallback evidence and completeness.
5. HPA activation at 80% CPU or memory.
6. Provider node-autoscaler scale-up.
7. Physical node termination with identical post-recovery semantic output.
8. Checksum rejection plus backup and restore equality.
9. vLLM/GPU scale from zero onto a GPU node.
10. Inference drain behavior.
11. Cross-tenant dataset denial.
12. Cross-tenant MCP, memory, tool and query-log denial.

```bash
./phase3/scripts/qualify_cluster.py \
  --config /etc/ngkg-phase3/eks/cluster.json \
  --image-lock phase3-evidence/supply-chain/image-lock.json \
  --deployment-evidence phase3-evidence/deployments/eks.json \
  --approval-evidence /etc/ngkg-phase3/eks/disruption-approval.json \
  --output phase3-evidence/clusters/eks.json
```

The node-loss driver receives one canonical JSON request on stdin and must return JSON containing `complete`, `provider`, `clusterUid`, `terminatedNodeUid`, `replacementNodeUid`, and `postRecoveryResultSha256`. It must terminate the actual provider VM/node, not merely delete a Kubernetes Pod or Node object.

The recovery driver receives action `checksum-backup-restore` and must return `checksumFailureRejected`, `corruptObjectSha256`, `rejectedOperationId`, `backupVerified`, `restoreVerified`, `backupManifestSha256`, `preFailureResultSha256`, `postRestoreResultSha256`, provider/cluster identity and `complete`. The inference driver must make a real inference request and return `complete: true` only after a non-empty bounded response.

## Certificate issuance

`verify_and_issue.py` re-verifies every image signature, SBOM attestation and provenance attestation; verifies signed PostgreSQL and provider evidence blobs; recomputes artifact checksums; requires exactly thirteen images, four providers and twelve scenarios per provider; and only then emits a checksum-bound certificate.

The root workflow `.github/workflows/phase3-controlled-release.yml` wires the entire sequence to a protected, manually approved environment. It will not run from an ordinary push or pull request.

## Local source validation

Source-only validation does not issue a Phase 3 certificate:

```bash
./phase3/acceptance.sh
```

This checks script syntax, strict JSON contracts, offline Dockerfile boundaries, the exact image/scenario/provider inventories, supply-chain controls and controlled-workflow wiring.
