# Enterprise Stabilization Phase 6

Phase 6 is the production differential, capacity, chaos and release-closure gate for the native distributed query and OWL 2 DL reasoning cutover. It does not add Apache Jena or another scalar engine to the product. The oracle endpoint is an isolated qualification-only deployment; the production endpoint must run `NGKG_NATIVE_CUTOVER_MODE=required` and fails closed when native execution evidence is unavailable.

## What the controlled workflow proves

1. Every SELECT, ASK, CONSTRUCT and DESCRIBE case has an identical semantic result on the native and qualification-oracle lanes. SELECT comparison preserves duplicates and unbound variables. Every RDF graph result uses a checksum-pinned RDFC-1.0 canonicalizer.
2. Covered BGPs use certified closure and genuinely uncovered legal OWL 2 DL BGPs use the isolated exact-HermiT boundary. A partial response, changing snapshot, changing result hash or semantic mismatch fails the run.
3. Capacity trials retain every warm-up, measured failure, timeout and saturation point. Results must remain deterministic at 1, 2 and 4 nodes and at concurrency 1, 8, 32, 100 and 250.
4. Driver evidence is accepted only for real Kubernetes pod UIDs and their actual node UIDs. CPU time and peak RSS must be measured values; configured requests and limits are not treated as consumption.
5. HPA targets must be 80% CPU and 80% RAM. Provider-native node provisioning must demonstrate scale from zero and scale down.
6. Pod loss, worker-node loss, network partition, PostgreSQL failover, corrupt objects, duplicate delivery and checkpoint recovery are serialized and must preserve the pre-failure semantic hash without exposing partial results.
7. The same twelve digest-pinned images must run on RKE, RKE2, EKS, AKS and GKE. S3, Azure Blob and GCS object-store paths, workload identity, HA and node provisioning are verified through provider drivers.
8. Cosign keyless signatures, SPDX SBOM attestations, CycloneDX attestations, zero unapproved high/critical vulnerabilities and two independent reproducible builds are mandatory.
9. Signed Phase 3, Phase 4 and Phase 5 certificates remain prerequisites. Static or synthetic evidence cannot produce a Phase 6 certificate.

## Controlled runner layout

Copy the examples from `config/` into a protected directory outside the repository:

```text
/etc/ngkg-phase6/
  release.json
  differential.json
  image-lock.json
  vulnerability-report.json
  builder-a-manifest.json
  builder-b-manifest.json
  defect-ledger.json
  providers/{rke,rke2,eks,aks,gke}.json
  prerequisites/{phase3-certificate,phase4-live-certificate,phase5-live-certificate}.json
  prerequisites/{phase3-certificate,phase4-live-certificate,phase5-live-certificate}.sigstore.json
```

Tokens and disruptive-approval records must be owner-readable only. All source, query, driver, image-lock and approval hashes must be frozen before execution.

Run source tests without touching a cluster:

```bash
python3 NGKG_1_0_0_GA/scripts/verify_enterprise_stabilization_phase4.py
python3 NGKG_1_0_0_GA/scripts/verify_enterprise_stabilization_phase5.py
python3 NGKG_1_0_0_GA/scripts/verify_enterprise_stabilization_phase6.py
python3 NGKG_1_0_0_GA/scripts/verify_api_openapi_parity.py
python3 -m unittest discover -s phase6/tests -p 'test_*.py'
```

After an authorized maintenance window, execute the live workflow:

```bash
export NGKG_PHASE6_EXECUTE_LIVE=YES
export NGKG_PHASE6_RUN_ID='site-change-1234'
./phase6/acceptance.sh /etc/ngkg-phase6 /var/lib/ngkg-phase6/evidence
```

The GitHub workflow provides the same boundary with a protected `ngkg-phase6-release` environment and five independent provider runners.

## External driver protocol

Capacity, chaos and cloud-provider operations differ across CNI, CSI, managed PostgreSQL and node-provisioner implementations. They therefore use checksum-pinned executables rather than shell fragments embedded in configuration. Each driver reads one canonical JSON request from standard input and emits one JSON response to standard output. Drivers must never print credentials.

The capacity action is `RUN_SATURATION_MATRIX`. It must use Kubernetes Indexed Jobs or an equivalent dense partition set, spread workers across physical nodes, use all cgroup-authorized cores inside each worker and return:

- `workers`: pod UID, node UID, allocated cores, measured CPU nanoseconds and measured peak RSS bytes;
- `trials`: case ID, node count, concurrency, duration, request count, error count, partial-result count and `semanticResultSha256`;
- `autoscalingEvents`: timestamped CPU/RAM trigger and replica/node changes;
- `saturationReached`, saturation boundary measurements and an immutable raw-evidence hash.

The chaos action is `INJECT_AND_RECOVER`. It receives exactly one scenario and must return pre/post semantic hashes, RTO, RPO, partial-response count, recovered state, real worker identities and raw evidence hash. A driver may use Chaos Mesh, Litmus, provider APIs or an approved internal mechanism, but its executable checksum and disruptive approval checksum are part of qualification identity.

The provider action is `VERIFY_IDENTITY_STORAGE_AND_NODE_SCALING`. It must prove workload identity without long-lived credentials, provider-native TriG ingestion, artifact/checkpoint round trips, HA, GPU/CPU pool scale from zero and scale down.

`schemas/driver-response.schema.json` defines the shared response envelope. Provider-specific extensions remain allowed in the driver response, but the verifier ignores claims that lack the required measured identities.

## Release evidence

`verify_and_issue.py` binds every prerequisite, provider, differential, supply-chain, reproducibility and defect record into a canonical evidence root. It emits `phase6-statement.json`. The statement is keylessly signed, verified, then bound into `phase6-certificate.json` using `schemas/phase6-certificate.schema.json`.

The certificate is not issued when any live certificate is absent, any provider fails, result hashes vary across scale, a partial answer appears, a driver reports unknown Kubernetes identities, a high/critical vulnerability remains, builders disagree, or a release-blocking defect is open.
