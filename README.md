# c8: Next Generation RDF Distributed Semantic Database, Kubernetes Native

NGKG is an ontology-native, distributed HPC knowledge graph database. This repository is built cumulatively from the phase gates in `docs/ENGINEERING_SOURCE.md`: every phase tag contains all prior code, contracts, tests, deployment assets, and verification evidence.

> Read the docs: https://aktiver-team.github.io/c8/

## Delivery model

Each archive is produced from an immutable Git tag. `phase-00` is the semantic conformance foundation; later tags extend the same workspace. No phase is a disconnected sample project.

The implementation is fail-closed:

- one immutable `snapshot_id` binds data, ontology, mapping, policy, indexes, proofs, and locators;
- no exact query succeeds without certified coverage or a version-matched exact reasoner path;
- workers consume immutable work envelopes and never discover current state by listing storage;
- public REST handles control metadata, bounded authenticated Arrow IPC over internal REST carries the implemented Phase 23 fragment exchange, and immutable object storage carries bulk artifacts; Arrow Flight remains an unimplemented future optimization;
- Kubernetes schedules disposable pods, while the catalog and immutable artifacts hold durable truth.

## Local verification

```bash
python3 scripts/structural_validate.py --root .
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Release builds also require a checked-in `Cargo.lock`, real PostgreSQL and S3-compatible integration tests, a certified OWL 2 DL reasoner, Helm rendering, and a supported Kubernetes/RKE2 qualification cluster. `verification/` records which checks actually ran. Missing dependencies block a gate; they are never converted into a passing result.

## Cumulative phases

| Tag | Capability added |
|---|---|
| `phase-00` | semantic/reference corpus and benchmark claim |
| `phase-01` | durable catalog and REST control plane |
| `phase-02` | immutable source manifests and safe partitions |
| `phase-03` | compiled semantic projection manifest |
| `phase-04` | GUIDs, FactIDs, and dense dictionaries |
| `phase-05` | Arrow projection and semantic spine |
| `phase-06` | semantic indexes and direct GLI routing |
| `phase-07` | certified OWL 2 DL reasoner boundary |
| `phase-08` | atomic snapshot publication |
| `phase-09` | distributed OWL-aware SPARQL contracts |
| `phase-10` | context assembly and selective hydration |
| `phase-11A` | restart-safe Kubernetes distributed stages |
| `phase-11B` | Helm and RKE2 capacity integration |
| `phase-11C` | data-plane networking, scaling, and HPC budgets |
| `phase-12` | qualification harnesses and release gates |
| `phase-13` | first real TriG → Parquet → HermiT → certified SPARQL → GUID hydration reference slice |
| `phase-14` | authenticated REST → catalog → Kubernetes Job → object store → atomic certification/publication slice |
| `phase-15` | syntax-safe TriG sharding → Indexed projection Jobs → external reducers → canonical source → HermiT-certified snapshot |
| `phase-16` | topology-stable logical shards → encoded Parquet/spine/payload → query/reasoner sidecars → bounded direct-locator merge |
| `phase-17` | cataloged artifact plan → Indexed Parquet workers → global locator/root CAS → reference certification handoff |
| `phase-18` | checksum-bound mmap locator → direct sharded Parquet row-group hydration → 80% online scaling policy |
| `phase-19` | immutable serving root → reference-versus-sharded hydration proof → publication admission gate |
| `phase-20` | certified online SPARQL replica → mmap GUID location → direct sharded Parquet hydration |
| `phase-21` | offline-certified relevant named graphs → checksum-bound routed runtime → contextual GUID hydration |
| `phase-22` | independently certified named-graph fragments → real cross-node execution → exact bounded bag join |
| `phase-23` | typed bounded Arrow IPC fragment exchange → lossless RDF bindings → streamed backpressure |
| `phase-24` | certified stable hash partitions → parallel cross-node bag joins → exact final multiset admission |
| `phase-25` | bounded local-NVMe partition spill → concurrent checksum-verified replay → lower coordinator memory amplification |
| `phase-26` | immutable input-bag cache keys → verified local-NVMe partition reuse → exact hot-query acceleration |
| `phase-27` | pre-body bounded admission → streamed-response lifetime permits → observable overload control |
| `phase-28` | checksum-bound tenant policy → per-tenant data-plane lanes → noisy-neighbor isolation |
| `phase-29` | certified complete response → bounded local-NVMe/mmap reuse → exact recurring-query acceleration |
| `phase-30` | bounded worker Grace buckets → hot-key-safe sparse join chunks → exact skewed enterprise execution |
| `phase-31` | streamed Arrow request spool → incremental RDF decode → direct bounded Grace partitioning |
| `phase-32` | incremental coordinator spill replay → bounded Arrow batches → backpressured HTTP streaming |
| `phase-33` | streamed fragment response ingress → checksum-verified NVMe spool → incremental Arrow decode |
| `phase-34` | certified fragment spool replay → direct primary hash partitioning → zero owned ingress rows on shuffle fast path |
| `phase-35` | streamed shuffle-result ingress → ordered partition spool sequence → no stage-wide intermediate assembly |
| `phase-36` | executable standards/toolchain compliance baseline and fail-closed release qualification |
| `phase-37` | lossless RDF dataset identity, authorization and union-default semantics |
| `phase-38` | typed SPARQL compiler, SPARQL Protocol, service description and online OpenAPI |
| `phase-39` | exact scalar SPARQL 1.1 algebra/query forms with certified distributed SELECT fast path |
| `phase-39.1–39.5` | reproducibility, W3C harness, GRAPH ?g regressions, ad-hoc exact RDF fallback and gate governance |
| `phase-40` | Phase 40 requirements/traceability/HPC ceiling baseline plus REST/OpenAPI/Swagger parity |
| `phase-40.1–40.6` | OWL signature, datatype policy, Direct-BGP result/certificate contracts, and checksum-bound combined OWL 2 DL profile/import qualification |
| `phase-40.7–40.10` | OWL Direct BGP legality, exact HermiT fallback, proof-support certification, and authoritative Helm semantic/HPC ceilings |
| `phase-40.13.1–40.13.18` | native recovery, complete scalar SPARQL, exact online/offline OWL reasoning, cloud TriG compilation, distributed algebra/property paths, and secured federation |
| `phase-40.13.19` | checksum-bound multinode replication, relocation, node-loss retries, backup, storage-verified restore, and Kubernetes Indexed Job execution |
| `phase-40.13.20` | portable RKE/RKE2/EKS/AKS/GKE autoscaling at exact 80% CPU-or-memory, three-cloud read-only TriG mounts, cgroup budgets, scale-from-zero/Kueue qualification, checkpoint-safe drain, and deterministic evidence |

Run `python3 scripts/verify_phase_inheritance.py` to prove that each tag is a descendant of the prior phase. Run `python3 scripts/package_phases.py --output-dir dist` to create reproducible cumulative ZIPs from the tags.

This repository is an incremental engineering implementation, not a passed production release. The checked-in verification reports truthfully show structural checks passing and unavailable compile/cluster gates blocked in the build workspace. Production acceptance requires the exact external systems and workload described in `docs/RELEASE_ACCEPTANCE.md`.

## First executable reference slice

Phase 13 is the first implementation that is intended to execute a semantic path rather than only define its contracts. It performs all of the following on immutable local files:

1. verifies the uploaded TriG, ontology, query, expected result, and reasoner adapter checksums;
2. parses complete TriG syntax and preserves named graphs, datatypes, languages, and deterministic blank-node identity;
3. assigns deterministic GUIDs, FactIDs, and dense dictionaries;
4. writes a real Arrow semantic spine and payload Parquet file;
5. creates a fixed-width GUID locator instead of listing or scanning payload files;
6. invokes a trusted, version-locked HermiT/OWLAPI process over the minimal reasoning core;
7. builds the query dataset from queryable facts plus the reasoner materialization;
8. certifies an exact query hash by comparing its multiset with an independent expected result;
9. atomically publishes the local snapshot directory; and
10. executes only certified query hashes and hydrates bound GUIDs directly from located Parquet rows.

The uploaded manifest cannot choose executable code or raise its own resource ceiling. Java, adapter JAR, adapter checksum, accepted reasoner name/version, and maximum input/quad/dictionary/reasoner/row-group bounds are operator-controlled worker arguments. Any checksum, ontology import, ontology consistency, reasoner identity, result equality, locator, or artifact failure stops the build.

To run the checked-in corpus after installing the pinned Rust toolchain, Java 17, and Maven, create an empty output directory and execute:

```bash
NGKG_REFERENCE_OUTPUT_ROOT=/absolute/empty/output scripts/run_reference_slice.sh
```

This milestone is deliberately narrower than the production thesis. HermiT is a complete OWL 2 DL reasoner, but the first adapter exports only finite named-entity assertions and named hierarchies and does not yet emit proof DAGs. Consequently, Phase 13 certifies only the exact query hashes and immutable snapshots whose answers are independently checked. Arbitrary OWL 2 DL SPARQL, proof-complete results, and distributed compilation/querying remain fail-closed future integration work; Phase 14 adds the catalog, REST, object-store, Kubernetes, and publication boundary around that deliberately limited compiler.

## Atomic service slice

Phase 14 wraps the Phase 13 compiler with a real service boundary. A caller stages a checksum-addressed compilation bundle below an operator-owned `file://` or `s3://` root, creates a dataset, and submits the bundle through the authenticated REST API. PostgreSQL stores tenant-isolated operation truth and immutable audit transitions. The API ensures one deterministic `NgkgCompilation`; the operator ensures one Kueue-labelled Job; the worker downloads exact objects concurrently, runs the Phase 13 compiler, uploads only manifest-listed artifacts, uploads `snapshot-manifest.json` last, and commits certification. Manual or automatic publication uses an active-snapshot compare-and-swap.

The API serves its reviewed Swagger/OpenAPI contract at `/openapi.yaml`. Every protected operation requires a bearer token whose SHA-256 is mapped by an operator-mounted secret to exactly one tenant, principal, and scope set. The database tenant is never accepted from a request body.

For a local checksum-addressed object root, first create an empty directory and stage the checked-in corpus:

```bash
python3 scripts/stage_reference_bundle.py \
  --object-root /absolute/existing/object-root \
  --source test-corpus/datasets/cross-domain.trig \
  --ontology test-corpus/ontologies/core.ttl \
  --projection-policy test-corpus/reference/projection-policy.json \
  --query test-corpus/queries/q01-cross-domain.rq \
  --expected test-corpus/expected/q01-cross-domain.srj \
  --query-id q01-cross-domain \
  --ordered false \
  --required-source-iri https://ngkg.io/id/source-1 \
  --required-source-iri https://ngkg.io/id/source-2 \
  --closure-graph-iri urn:ngkg:graph:reference-closure \
  --dataset-id 4d2e1a82-c2bc-536a-a809-fda7643ef1f7 \
  --snapshot-id 91054ecb-2f68-5a63-b31a-137333c64a7c \
  --dataset-namespace 7b8e1c18-9c22-5b58-a2b4-7cbf21cc9b2b \
  --source-guid d2cfd1d5-5f83-5bb3-9888-f3bc3229a760 \
  --source-snapshot reference-corpus-v1 \
  --max-input-bytes 10485760 \
  --max-quads 100000 \
  --max-dictionary-terms 100000 \
  --max-reasoner-seconds 300 \
  --parquet-row-group-rows 65536 \
  --max-named-individuals 100000 \
  --max-properties 10000
```

The output is the exact `bundleObjectKey` and `bundleSha256` used in `POST /v1/datasets/{datasetId}/ingestions`. In a cluster, use S3-compatible storage and configure `artifactStore.baseUrl`; the request still contains only the relative key and checksum.

Create the dataset with the same `identityNamespace` and projection `policyId` carried by the bundle. The worker treats both as durable identity inputs and rejects a checksum-valid bundle if either differs from catalog truth. For the checked-in corpus, `policyVersion` is `urn:ngkg:projection-policy:reference-v1`.

Install CRDs before the platform chart. The platform pre-install/pre-upgrade hook runs all catalog migrations using the separately privileged `migration-database-url`; API, operator, and workers use the restricted `database-url`. Provide digest-pinned API, both operators, both workers, and migration images, the reasoner JAR checksum, the auth-token secret, and the database secret. On RKE2, configure the responsibility pools, labels, taints, Kueue flavors, and Cluster Autoscaler chain described in `docs/RKE2_AUTOSCALING.md`.

For the distributed profile, create distinct Rancher/RKE2 pools labelled and tainted `semantic-projection`, `semantic-artifact-build`, `index-build`, and `reasoning`; install the pinned Rancher-provider Cluster Autoscaler separately; then set `distributedOperator.logicalPartitions`, `reducerCount`, `artifactRowGroupRows`, and per-stage `maxParallelism` in the Helm values. The chart creates the Indexed Jobs and Kueue demand, while Cluster Autoscaler changes matching machine-pool quantities. It does not provision machines by itself. Exact setup and verification commands are in `docs/RKE2_AUTOSCALING.md`.

## Distributed compilation slice

Phase 15 distributes one compilation without changing its RDF or OWL meaning. The planner parses the complete TriG grammar, source-scopes blank nodes, normalizes facts, and writes content-addressed canonical N-Quads into a stable logical partition space. Kubernetes pod count is deliberately separate from the logical partition count: retries, autoscaling, and different `parallelism` settings change scheduling only.

The distributed operator executes this durable DAG:

```text
single syntax-safe planner
  → Indexed projection Job over catalog completion indexes
  → Indexed external-merge reducer Job
  → one canonical N-Quads source + deterministic dense dictionary
  → Phase 14 compiler using the canonical source
  → HermiT reasoning, exact query certification, and snapshot CAS publication
```

Every worker reads exact catalog/object keys and hashes. It never lists a bucket. Projection and reducer output manifests are uploaded last, then committed with a compare-and-swap. Reducers use bounded k-way merge over sorted runs instead of loading the complete compilation into memory. The global finalizer proves exact partition and reducer coverage before the existing compiler can see the source.

Use `resourceProfile: distributed-hpc-v1` to select this path. The Helm chart deploys a separate distributed operator and maps planning/projection Jobs to `semantic-projection` nodes, reducers/finalization to `index-build` nodes, and HermiT to `reasoning` nodes. Kueue admits each shape; the RKE2 Cluster Autoscaler resizes the matching Rancher machine pool.

This phase does not claim that the initial complete TriG parse itself scales linearly across nodes. It is the correctness boundary that makes all downstream work distributable. Parallel lexical boundary scanning for very large seekable TriG and distributed OWL module reasoning remain later optimizations and must preserve the Phase 15 equivalence gate.

## Distributed semantic-artifact kernel

Phase 16 adds the next execution kernel after canonical source construction. Each Phase 15 logical shard can independently become an integer-encoded semantic-spine Parquet shard, a payload Parquet shard, query-visible N-Quads, reasoner-visible N-Triples, and a sorted GUID locator run. The finalizer validates exact source-plan coverage and externally merges locator runs with bounded memory.

`scripts/run_distributed_artifact_slice.sh` executes the same artifact plan in forward and reverse completion order and requires identical partition manifests, locator bytes, row counts, and semantic roots. This is real encoding code, not a deployment claim. The Phase 16 boundary does not yet make the catalog/object-store operator consume these shards in the certified service path; the Phase 15 reference compiler remains the oracle until that integration passes snapshot and hydration equivalence.

## Durable distributed semantic artifacts

Phase 17 makes the Phase 16 kernel a real cataloged Kubernetes sub-DAG. An immutable artifact plan creates one `ARTIFACT` completion index per logical source partition. Indexed workers publish Parquet and semantic sidecars before committing a manifest; a bounded finalizer merges locator runs and commits one global root. The distributed operator will not schedule HermiT/reference certification until that root exists.

The reference worker independently rebuilds the certified snapshot and verifies artifact-root identity, catalog completion coverage, locator integrity, and logical counts. It does not yet publish the sharded Parquet as online serving truth. This preserves the existing correctness oracle while providing real multi-node artifact production and the durable boundary needed for a later query/hydration cutover. See `docs/phases/PHASE_17.md` for the executed DAG and remaining gate.

## Memory-mapped locator and sharded hydration

Phase 18 adds the first real sharded serving kernel on top of those artifacts. The Phase 17 locator is compiled into a checksum- and snapshot-bound fixed-width binary index, loaded into read-only memory-mapped virtual memory, and searched directly by GUID. Qualified results are grouped by exact Parquet partition and row group, then hydrated across bounded in-pod CPU lanes without listing or scanning unrelated files.

Kubernetes spreads query and hydration replicas across responsibility nodes. CPU and memory HPA targets are capped at 80%, leaving operational headroom before RKE2 adds the next node. OpenMP and BLAS remain single-threaded for sparse locator and Parquet kernels; measured dense scoring kernels receive a separate mutually budgeted profile. This kernel is implemented but is not yet the certified online serving root. See `docs/phases/PHASE_18.md` for the exact boundary and acceptance gates.

## Immutable distributed serving admission

Phase 19 compiles the canonical locator into a durable binary object and publishes `serving-root.json` last. That manifest binds every Parquet payload shard, dictionary, locator, artifact root, snapshot, row-group size and checksum. The distributed operator inserts this stage between artifact finalization and the reference/reasoner Job.

Before certification, every certified query runs through the independent reference snapshot and the sharded mmap/Parquet path. Canonical hydration multisets must be identical, including for bound GUIDs that have no payload rows. PostgreSQL records the immutable equivalence report, and reference certification or automatic publication fails closed without a certificate bound to the exact reference manifest. The authenticated job-status REST response exposes both roots and the certificate. This admits a distributed physical representation; the horizontally scaled online query coordinator remains a later phase. See `docs/phases/PHASE_19.md`.

## Certified online read plane

Phase 20 deploys the first real reader of the Phase 19 serving root. The `ngkg-online-serving` Rust binary runs as independently scalable query, locator, and hydration roles. A query replica accepts only the active published snapshot and an exact SPARQL byte hash certified by the offline HermiT/reference pipeline. It returns the certified bindings, derives deterministic GUIDs, and optionally asks a hydration replica to resolve those GUIDs through the checksum-verified read-only mmap locator and exact Parquet row groups.

The service contract is available at `/openapi.yaml`; query execution requires `queries:execute`.
Phase 40.13.21 adds tenant-scoped `/v1/query_logs` routes: query users see their own executions,
`query-logs:read` grants tenant-wide audit access, and `query-logs:read:text` independently grants
other users' exact query text. Configure `onlineServing.databaseSecret`,
`onlineServing.authTokensSecret`, `onlineServing.artifactStoreBaseUrl`, optional object-store
credentials, and bounded cache/query/result/query-log values in the workload chart. Query clients
must carry the pod label `ngkg.io/query-client=true`, or enter through an approved gateway that
does, because the data plane is default-denied.

RKE2 assigns query/locator replicas to `sparql-query-processing` nodes and hydration replicas to `parquet-hydration` nodes. CPU and memory requests equal limits, Rust control/blocking/hydration lanes are explicitly bounded, and sparse online kernels fix OpenMP/OpenBLAS/MKL to one thread to avoid nested oversubscription. HPA resource targets are capped at 80%; required anti-affinity turns added replicas into responsibility-specific pending demand that Rancher Cluster Autoscaler satisfies with the matching VM pool. See `docs/phases/PHASE_20.md` and `docs/RKE2_AUTOSCALING.md`.

## Certified relevant-graph routes

Phase 21 compiles predicate, class, and cross-graph entity capabilities from the complete normalized dataset. Every certified query receives an immutable N-Quads route containing only its selected asserted named graphs. The compiler executes that route with the exact offline reasoner closure and accepts it only when bindings and required source evidence equal the independent expected result; otherwise it expands to all graphs or fails compilation.

Online query replicas verify the capability index, certificate, route, closure, and published snapshot checksums, then load only the exact route for the submitted certified query hash. A bounded LRU limits resident routed Oxigraph stores and coordinates removal of evicted local route files. Configure `onlineServing.maxResidentQueryRoutes` from measured route sizes and pod memory; this is a hard count bound, not an assertion that all routes cost the same memory. See `docs/phases/PHASE_21.md` for the acceptance and intentional boundary.

Phase 22 compiles eligible explicit cross-domain `GRAPH` blocks into immutable fragment datasets and queries. It executes the fragments offline, performs a bag-correct join, and publishes a distributed plan only when that result equals both the independent expected result and the complete Phase 21 route result. Online coordinators require at least two ready fragment workers, validate every fragment response and the final multiset, then reuse deterministic GUID and Parquet hydration. Unsupported queries keep the certified local route. See `docs/phases/PHASE_22.md` for the exact coverage boundary and deployment gates.

Phase 23 replaces the internal per-row JSON fragment response with a typed, chunked Arrow IPC stream. RDF term family, lexical value, datatype, language, unbound variables and bag duplicates are preserved in explicit columns, while Arrow schema metadata carries the snapshot and fragment certificate identity. Workers and coordinators reject any media-type, schema, metadata, row or byte-bound mismatch before the Phase 22 exact join. The public API remains REST/JSON, and the RKE2 fragment pool continues to scale at the 80 percent resource boundary. See `docs/phases/PHASE_23.md`.

Phase 24 moves eligible multi-fragment join computation off the coordinator. The coordinator partitions both exact SPARQL bags by a canonical SHA-256 of every fully bound shared RDF term, dispatches each partition to the existing fragment worker pool, validates partition ownership and output checksums, and admits the union only when it equals the original offline-certified final multiset. Cross products and stages with unbound join keys retain the exact Phase 23 coordinator join. The same RKE2 fragment node group supplies join capacity and scales at no more than 80 percent CPU or memory. See `docs/phases/PHASE_24.md`.

Phase 25 consumes each shuffle stage into bounded, checksum-verified partition files on query-node local NVMe before network dispatch. Only the configured concurrent partition pairs are replayed into Arrow requests, preventing every cloned input partition from remaining resident at once. Sparse joins still execute across multiple Rust lanes, cores, pods and nodes; OpenMP/BLAS remain single-threaded native dependencies to avoid nested oversubscription, while the fixed-width GUID locator retains its safe read-only mmap path. See `docs/phases/PHASE_25.md`.

Phase 26 caches exact logical join results on fragment-node local NVMe. Reuse requires the same tenant, immutable snapshot, certified query, plan, stage, partition and canonical left/right input bags; every hit is checksummed and semantically revalidated before Arrow transmission, and the coordinator still compares the final bag with the offline certificate. See `docs/phases/PHASE_26.md`.

Phase 27 acquires bounded role-specific execution capacity before request-body extraction and retains it through complete JSON or Arrow response delivery. Query, fragment, shuffle, locator and hydration workloads expose low-cardinality Prometheus admission/service/cache metrics, while overload fails explicitly with retryable HTTP 429 instead of growing an unbounded internal queue. See `docs/phases/PHASE_27.md`.

Phase 28 layers finite, checksum-bound tenant execution and pending limits beneath every Phase 27 global lane. The policy must cover exactly the authorized query tenants; a saturated tenant receives an explicit retryable response without taking an unused lane from another tenant. Tenant and global permits survive the full streamed response lifetime, and metrics distinguish saturation scope without tenant identifiers. See `docs/phases/PHASE_28.md`.

Phase 29 caches only a complete response that has already passed the exact offline query certificate, distributed final-bag validation and optional GUID/Parquet hydration. Its key binds tenant, dataset, immutable snapshot, manifest, serving root, exact query bytes, hydration mode and response schema. Every local-NVMe hit is checksum-verified and logically revalidated before read-only mmap-owned bytes are returned; invalid entries are removed and recomputed through the certified path. The cache is bounded, disposable, per query replica and never authorizes a request or selects semantic truth. See `docs/phases/PHASE_29.md`.

Phase 30 bounds the fragment worker's physical hash-index working set. Oversized primary shuffle partitions are repartitioned by a domain-separated Grace hash into checksum-verified local-NVMe buckets, then joined through bounded right build and left probe chunks. A single repeated RDF key can still produce a large certified bag, but it cannot force an unbounded worker hash table. The coordinator validates mode, spill, bucket and build-chunk evidence before final offline-certificate comparison. Rust partitions and pods provide sparse parallelism; OpenMP/BLAS stay at one thread, mmap remains for fixed-width locator/cache access, and Parquet remains the payload store. See `docs/phases/PHASE_30.md`.

Phase 31 removes the next worker memory boundary. Fragment replicas stream each Arrow shuffle request directly to a bounded, checksum-verified local-NVMe spool, validate and digest RDF rows incrementally, and feed large relations directly into Grace buckets. Small declared partitions retain the bounded in-memory fast path. The coordinator verifies worker-reported input bytes and SHA-256 against the exact request before accepting output, and the final offline certificate remains authoritative. See `docs/phases/PHASE_31.md`.

Phase 32 removes the matching coordinator memory boundary. Query replicas incrementally validate partition spill records, encode only bounded Arrow batches, and transmit through a bounded backpressured channel while computing request SHA-256 and enforcing cumulative exchange limits. The production shuffle path no longer reconstructs complete partition vectors or a complete outgoing request buffer. See `docs/phases/PHASE_32.md`.

Phase 33 externalizes the initial distributed fragment-response bytes. Query replicas stream concurrent Arrow responses into process-budgeted, checksum-verified local-NVMe leases and then decode one Arrow batch at a time. This removes complete encoded response vectors and concurrent complete decodes while retaining exact fragment and final offline certificates. See `docs/phases/PHASE_33.md`.

Phase 34 removes complete decoded fragment vectors from the partitioned-shuffle fast path. Query replicas make one incremental validation pass to capture exact row counts and always-bound join-key summaries, then reopen the immutable checksum-bound fragment files and hash each row directly into its primary NVMe partition. Non-shuffle-compatible plans use an explicit, row-bounded sequential owned fallback. Public evidence and Prometheus counters distinguish both paths. See `docs/phases/PHASE_34.md`.

Phase 35 streams shuffle-worker results to checksum-verified NVMe and retains each certified partition as an ordered lazy spool sequence between join stages. It validates one partition bag at a time, eliminates the stage-wide coordinator row vector, and materializes only the final bounded result required for projection and REST output. See `docs/phases/PHASE_35.md`.

This phase supports exact certified queries over an offline reasoner materialization. It does not yet support arbitrary SPARQL under full OWL 2 DL Direct Semantics, a distributed property-path frontier, or a validated 20–50× performance claim.

## Standards compliance phases 36–38

Phase 36 establishes the executable compliance boundary: checksum-pinned W3C suite sources, combined OWL 2 DL profile/consistency reporting, streaming `application/trig` ingestion, fail-closed build/deploy qualification, and exact fresh-result certificate comparison. Missing toolchains or conformance evidence never enable a standards claim.

Phase 37 makes the RDF dataset lossless across the compiler and serving path. Default and named graph identity, blank-node RDF term kind, graph roles, authorization labels, empty declared graphs, union-default behavior, query/protocol dataset precedence, and active-dataset hashes are preserved independently from internal GUID/dictionary keys. Graph authorization is resolved before semantic state and is bound into plans and caches.

Phase 38 replaces semantic query-text scanning with the shared `ngkg-sparql-compiler`. The pinned SPARQL 1.1 parser produces a typed immutable algebra, dataset specification, routing evidence, canonical algebra certificate, and safe distributed-fragment eligibility. Protocol dataset precedence, vendored Swagger, bounded parsing, configurable PostgreSQL pools, and all Phase 20–35 Arrow/NVMe/Grace/mmap/Parquet HPC machinery remain intact.

Phase 39 adds the bounded exact scalar reference for `SELECT`, `ASK`, `CONSTRUCT`, and `DESCRIBE`. The pinned evaluator owns SPARQL 1.1 multiset/algebra semantics for OPTIONAL, UNION, MINUS, FILTER/BIND/VALUES, subqueries, aggregates/modifiers, and property paths. Result certificates are form-aware: SELECT bag/sequence, ASK Boolean, and RDFC-1.0 graph equality for graph-producing forms. Online exact execution is cooperatively cancellable and uses deployment-configured row/triple/blank-node/time ceilings. SELECT/ASK negotiate JSON/XML/TSV/CSV; CONSTRUCT/DESCRIBE negotiate Turtle/N-Triples/RDF/XML. The existing distributed fast path stays typed-certificate SELECT-only until later operator equivalence phases.

OWL 2 Direct complete-runtime fallback remains Phase 40 work. Certified semantic extents/proofs remain Phase 41, native C++ kernels Phase 42, and full W3C/security/failure/RKE2 qualification Phase 43. Public SPARQL 1.1, union-default, OWL Direct, and OWL 2 DL service-description claims remain disabled until their executable qualification evidence exists.

## Phase 40.2 datatype policy

Phase 40.2 makes OWL Direct datatype support explicit and fail-closed. The reference compiler validates reasoning-visible literals against the repository-shipped policy, emits checksum-bound datatype-policy/validation artifacts, and requires the HermiT adapter to verify that the merged ontology uses only supported datatypes. See `docs/DATATYPE_POLICY_CONTRACT.md`.

## Phase 40.3 Direct-BGP result contract

Phase 40.3 adds the shared fail-closed result object that future OWL Direct BGP execution must produce. It binds exact RDF-term solution bags to dataset/snapshot/query/BGP/authorization/OWL-signature/datatype-policy identities and validates large solution vectors in deterministic bounded CPU lanes without expanding duplicates. See `docs/DIRECT_BGP_RESULT_CONTRACT.md`.


## Phase 40.4 Direct certificate contract

Phase 40.4 adds the immutable certificate that future exact OWL Direct BGP execution must attach to a successful exact-complete result. It binds result identity, reasoner identity, exhaustive candidate/partition evidence and proof/support references, using a solution-order-independent SHA-256 so distributed completion order cannot change certificate identity. Full proof/support runtime coverage remains Phase 40.9. See `docs/DIRECT_CERTIFICATE_CONTRACT.md`.


## Phase 40.5 OWL profile/import qualification

Phase 40.5 binds the exact OWLAPI-loaded ontology/version/import closure and merged `OWL2DLProfile` evidence into `reasoner/owl-profile-qualification.json`, `reasoner/report.json`, and the immutable snapshot. Unresolved imports, ambiguous ontology/version aliases, misplaced ontology-header triples, incomplete local import closure, profile violations, or SHA mismatches fail closed. See `docs/OWL_PROFILE_IMPORT_QUALIFICATION.md`.

## Phase 40.6 OWL consistency qualification

Phase 40.6 turns the existing HermiT consistency decision into a checksum-bound publication gate. The adapter evaluates `OWLReasoner.isConsistent()` over the exact complete merged OWL 2 DL ontology and emits `reasoner/owl-consistency-qualification.json`; Rust verifies its dataset/snapshot/input/signature/datatype/profile/reasoner/count bindings and the immutable snapshot records its SHA-256. Inconsistent ontologies remain diagnostic artifacts only and cannot publish. See `docs/OWL_CONSISTENCY_QUALIFICATION.md`.

## Phase 40.7 — OWL Direct BGP legality preflight

Phase 40.7 adds a fail-closed Rust legality classifier over the typed SPARQL algebra plus the authenticated `POST /v1/datasets/{datasetId}/sparql/direct/validate` Swagger endpoint. It binds every decision to the active dataset/authorization and Phase 40.1–40.6 semantic qualification hashes. Exact OWL Direct entailment remains Phase 40.8.


## Phase 40.8 exact OWL Direct fallback

Phase 40.8 adds a fail-closed exact HermiT fallback for Phase 40.7-admitted BGPs. It revalidates graph authorization/dataset semantics, materializes the exact BGP graph scope, exhaustively checks deterministic finite candidate ordinal partitions, performs grounded OWL 2 DL validation plus HermiT logical-axiom entailment, and emits exact Direct-BGP results/certificates only after a gap-free completion barrier. Anonymous-individual sigma multiplicity remains explicit not-covered until qualified. See `docs/phases/PHASE_40_8.md`.

### Phase 40.9 — exact Direct proof/support binding

Phase 40.9 extends the Phase 40.8 HermiT fallback with deterministic per-answer reasoner-check support IDs, a checksum-bound Direct proof manifest, complete SPARQL multiplicity coverage, a completion-barrier support for exact empty answers, and proof-bound Direct certificate format v2. HermiT derivation-DAG availability remains false; no derivation graph is fabricated.

## Phase 40.11 — reference-worker Phase 40 ceilings

`ngkg-reference-worker direct-bgp` now requires the trusted Phase 40 Direct ceiling environment and treats job-supplied limits only as lower sub-ceilings. The platform chart emits the values in an immutable ConfigMap; operator injection remains intentionally deferred to Phase 40.13.

## Phase 40.12

Phase 40.12 wires `phase40.directAdmission` into an immutable online-serving ConfigMap. Query coordinators now use the trusted Helm BGP/triple ceilings and CPU-aware classification lanes; distributed fragment workers validate the same bundle identity. Operator-created exact reasoner job propagation remains Phase 40.13.

## Phase 40.13.1 recovery increment

Phase 40.13.1 repairs the exact HermiT partition merge ceiling plumbing, separates legal SPARQL parsing from snapshot-certification policy, and wires the shared cpuset-aware Rust/OpenMP/BLAS budget into the online service. Legal volatile functions now take the bounded uncached scalar path; `SERVICE` parses but remains an explicit unavailable execution capability pending the secured federated handler. The workload chart can add per-pod admission-backlog metrics to CPU and memory HPA decisions through `profiles/production-workload-autoscaling.yaml`. Rust 1.97.1 compiles all targets except `ngkg-online-serving`, whose Axum `Send` boundary remains a release blocker; 32 targeted tests and all cumulative static gates pass. This remains a repaired candidate, not a full-SPARQL or production qualification claim.

## Phase 40.13.10 existing-cloud TriG acquisition

Phase 40.13.10 adds tenant-scoped human dataset names and a credential-free REST path that mounts
existing AWS S3, Azure Blob, or GCS buckets read-only through CSI. Bounded Rust workers freeze and
validate selected TriG objects into a checksum-bound source manifest, while Kueue and a dedicated
source-ingestion node pool provide batch admission and scale-from-zero capacity. This is source
acquisition only; the distributed decode/compiler/publication handoff remains an explicit next gate.
See `docs/phases/PHASE_40_13_10.md`.

## Phase 40.13.11 cloud-source compiler handoff

Phase 40.13.11 turns the frozen source manifest into deterministic whole-TriG decode work. A
Kueue-admitted Kubernetes Indexed Job parses complete syntax streams, publishes bounded N-Quads
fragments with object-scoped blank-node identities, and scales across `source-ingestion` nodes;
one all-completions finalizer verifies every remote digest before publishing the immutable compiler
handoff consumed by Phase 40.13.12. TriG byte offsets are never invented as semantic boundaries, and
the canonical graph roles are `https://c8-next-generation.io/<scope>/<subdomain>/{semkg|closure|provenance}`.
See `docs/phases/PHASE_40_13_11.md`.

## Phases 40.13.12–40.13.15 semantic compilation and activation

Phase 40.13.12 compiles the immutable handoff into stable RDF dictionaries, facts, Parquet,
adjacency, and semantic-index partitions. Phase 40.13.13 selects only explicitly authorized
`https://c8-next-generation.io/<scope>/<subdomain>/semkg` graphs, resolves checksum-pinned imports,
and qualifies one synthetic OWL 2 DL snapshot with HermiT 1.4.5.519. Phase 40.13.14 partitions the
exact finite named consequences into closure, extent, equality, and proof-support artifacts while
unknown coverage continues to route to exact HermiT.

Phase 40.13.15 verifies every compiler and reasoner partition, writes one checksum-bound activation
manifest, and commits certification before the existing active-parent compare-and-swap publishes.
Published cloud snapshots are available to ordinary semantic `/sparql` and non-hydrating `/query`
requests through the scalar correctness path up to its 64 GiB admission ceiling. Larger snapshots
remain inactive until Phase 40.13.16 provides partition-native distributed execution; physical
hydration also fails closed until a locator/payload layout is certified. These phases implement no
ontology alignment, schema matching, or raw-data mapping.

## Phase 40.13.16 distributed complete-algebra activation

Phase 40.13.16 connects ordinary exact queries to a checksum-bound fragment-worker algebra route
covering SELECT, ASK, CONSTRUCT, and DESCRIBE. The complete original and OWL-rewritten typed queries,
active-dataset identity, authorization hash, result ceilings, and replica identity are carried to at
least two distinct pods. Every replica uses the pinned scalar semantics, and the coordinator exposes
an answer only when the dense replica set returns the same canonical result hash and typed payload.
Existing Arrow/Grace-shuffle native joins remain the preferred certified fast path.

The fragment HPA charges this work to the algebra/shuffle admission class and scales from backlog,
active spill, CPU, and memory while cgroup-aware Rust lanes and single-threaded OpenMP/BLAS prevent
oversubscription. This candidate still requires native-build, failure-injection, full differential,
and live multinode qualification; snapshots larger than the scalar compatibility image require the
partition-native leaf-scan activation gate before a production-complete claim. No ontology
alignment, schema matching, or raw-data mapping is added.

## Phase 40.13.17 partition-native property paths

Phase 40.13.17 connects typed SPARQL property-path automata to the immutable forward/reverse
adjacency partitions produced by semantic compilation. Every frontier wave covers all semantic
partitions across fragment-worker pods; high-degree vertices are split across bounded Rust lanes,
and the coordinator advances or terminates only after a checksum-valid dense work barrier. Graph
scope is carried in frontier and endpoint identities, literal objects remain valid final endpoints,
and fixed-subject leaves use binary index seeks.

Iteration state is atomically checkpointed to bounded local NVMe and immutable object storage.
Pending partitions, active frontier items, checkpoint bytes, CPU, and memory feed the query and
fragment HPAs while cgroup/OpenMP/BLAS limits prevent nested oversubscription. The scalar oracle
still publishes the final answer pending multinode differential qualification and full algebra
substitution, so this remains a candidate rather than a large-snapshot production claim. See
`docs/phases/PHASE_40_13_17.md`.

## Phase 40.13.18 secured federation and SPARQL Protocol

Phase 40.13.18 enables fixed and variable SPARQL `SERVICE` plus `SERVICE SILENT` through a custom
scalar-evaluator handler. Exact endpoint IRIs are tenant-authorized by a checksum-bound registry;
credentials are separate Secret references. HTTPS enforcement, public-address DNS validation,
address pinning, disabled redirects, time/byte/call/queue/concurrency ceilings, and uncached
execution evidence keep remote state outside the immutable snapshot trust boundary.

Query pods expose federated backlog metrics to the existing CPU/memory/admission-aware HPA and use
an explicit TLS egress CIDR policy. Local HermiT BGP substitution and remote SERVICE evaluation
remain semantically separate. Native, official federation/protocol, Helm, and live multinode
qualification remain release gates. See `docs/phases/PHASE_40_13_18.md`.

## Phase 40.13.20 production autoscaling qualification

Phase 40.13.20 turns the shared 80-percent headroom rule into executable Rust policy. HPA and
KEDA own online pod demand, distributed operators own finite Indexed Job demand, Kueue admits
batch resources, and the selected external RKE/RKE2, EKS, AKS, GKE, or generic node provisioner owns
machine-pool size. CPU or memory reaching exactly 80 percent requests scale-out; pending work scales eligible
batch pools from zero. Scale-down remains blocked while checkpoints, property-path state, response
spools, or Grace-join spill are active.

Workers now validate bounded buffers against the finite cgroup-v2 memory limit before I/O. The
production overlay requires metrics.k8s.io, custom.metrics.k8s.io, Kueue, compatible node provisioning,
whole-core Guaranteed QoS, and one scaling owner per responsibility. Qualification cannot complete
unless scaled and node-loss/retry executions reproduce identical semantic result and artifact-root
checksums. The checked-in live harness is read-only and requires separately approved scale and chaos
evidence; no unobserved infrastructure event is converted into a pass. See
`docs/phases/PHASE_40_13_20.md`.

Phase 40.13.20 also makes the existing cloud-source ingestion boundary explicit across Amazon S3,
Azure Blob Storage, and Google Cloud Storage. The operator verifies the selected CSI driver before
creating a read-only PV/PVC, uses workload identity rather than embedded keys, and retains the
checksum-frozen whole-TriG handoff from Phases 40.13.10–40.13.12.
Phase 40.13.22 adds a content-bound standards and differential qualification plane. Pinned W3C
TriG/SPARQL suites, Apache Jena 6.2.0, and HermiT 1.4.5.519 execute as independent authorities in
stable Kubernetes Indexed Job partitions. The dense merge barrier rejects unsupported, missing,
duplicate, partial, or mismatched evidence; only an exact zero-mismatch report set can produce a
qualification certificate. The source candidate does not claim live qualification until those
external suites and oracle binaries execute on the release images and HA target clusters.

Phase 40.13.23 adds the release-bound performance and capacity qualification plane. The NGKG system
under test remains exclusively the Rust database, workers, operators, and query services. Apache
Jena is permitted only as an isolated same-hardware competitor process for applicable comparisons;
it is not linked, embedded, deployed as an NGKG service, or used as a runtime fallback. Exact result
and artifact hashes must remain invariant before latency, throughput, scale, or cost measurements
count. See `docs/phases/PHASE_40_13_23.md`.

Phase 40.13.24 adds the portable Kubernetes release-qualification plane. A release certificate now
requires an OWL 2 DL-qualified, multi-hop cross-domain `CONSTRUCT` or `DESCRIBE` execution whose
canonical unified context graph equals the scalar Rust oracle, includes reasoned output, completes
across multiple nodes and cores, and retains authorization plus proof evidence. A closed 75-cell
matrix covers 15 release gates on RKE, RKE2, EKS, AKS, and GKE. Non-disruptive cases use dense,
parallel Kueue Indexed Jobs with topology spreading; disruption, upgrade, rollback, and restore
cases are serialized and require content-bound approval for an isolated qualification cluster.
Static and synthetic evidence proves the harness, not production readiness. See
`docs/phases/PHASE_40_13_24.md`.

## 1.0.0-RC1 release freeze

The RC1 tree is versioned `1.0.0-rc.1` across Cargo, OpenAPI, and all Helm charts. It adds no new
data-plane feature. The release qualification crate and scripts now require live same-release
prerequisite certificates, inventory every frozen public/operational surface, require signed
artifacts and two identical isolated builds, and issue a publication certificate only after the
five-provider Kubernetes matrix is complete. The deterministic source packager normalizes ordering,
timestamps, permissions, and archive metadata. Static and synthetic evidence can test this machinery
but cannot publish RC1; current blockers are recorded in `release/1.0.0-rc1/rc1-readiness.json` and
`release/1.0.0-rc1/KNOWN_ISSUES.md`.

## 1.0.0 General Availability boundary

The GA tree is versioned `1.0.0` across Cargo, OpenAPI, all Helm charts, and active qualification
inventories. It adds no major data-plane feature. The final release boundary requires 20 live,
same-subject qualification certificates; closure and regression evidence for RC defects; two exact
isolated builds; a Rust-only production-runtime audit that excludes Apache Jena and keeps HermiT at
its pinned exact boundary; immutable signed artifacts; and qualified RKE/RKE2, EKS, AKS, and GKE
support records. CPU or memory reaching 80 percent remains the scale-out trigger, and S3, Azure Blob,
GCS, and qualified S3-compatible TriG paths remain mandatory live tests.

Static and synthetic tests exercise the fail-closed machinery but cannot issue a production go
certificate. The current external blockers are recorded in `release/1.0.0/ga-readiness.json`; the
live procedure is `release/1.0.0/ACCEPTANCE_TEST_PLAN.md`, and operational handoff is documented in
`docs/GA_RELEASE_AND_OPERATIONS.md`.
