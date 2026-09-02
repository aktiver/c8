# Phase 40.13.18 delivery report

## Outcome

This candidate implements secured SPARQL 1.1 federation through the normal `/sparql` and `/query`
paths. Typed `SERVICE`, `SERVICE SILENT`, fixed endpoints, and bound endpoint variables execute in
the scalar standards oracle. Local BGPs may still use exact HermiT substitution; remote SERVICE
BGPs remain outside local ontology assembly and are evaluated only by their allowlisted endpoint.

## Production logic added

- a new `ngkg-federation` runtime with a checksum-bound, tenant-scoped endpoint registry;
- credential-free registry records and bearer tokens resolved only from Kubernetes Secret-backed
  environment variables;
- HTTPS-only endpoints with userinfo, query-string credential, and redirect rejection;
- per-call DNS resolution, public-address validation, and TLS connection address pinning;
- private, loopback, link-local, multicast, documentation, carrier-grade NAT, benchmark, and
  reserved address rejection;
- bounded per-query calls, process concurrency, pending queue, lane wait, connect time, request
  time, and response bytes;
- standard JSON/XML remote-result parsing through Oxigraph's custom SERVICE handler contract;
- evaluator-owned `SERVICE SILENT` behavior and fail-closed non-silent failures;
- immutable snapshot/certificate/cache exclusion for every remote SERVICE query;
- per-query registry, endpoint-set, call-count, byte-count, and completion evidence in the JSON API
  and SPARQL response headers;
- conditional `sd:BasicFederatedQuery` service-description output;
- a read-only registry Secret mount, separate credential Secret, default-deny CIDR egress, and a
  federation-backlog HPA input on the query StatefulSet;
- an executable federation corpus and Phase 40.13.18 static acceptance gate.

## HPC and autoscaling behavior

Federated calls run in bounded blocking lanes that must fit the pod's cgroup-aware Rust
compute-plus-I/O thread budget. Concurrent client queries are distributed across query-shard pods;
the HPA can add pods from federation backlog while CPU, memory, admission backlog, and path
checkpoint signals remain active. Pod count changes throughput, not endpoint policy, SPARQL
semantics, or completion behavior.

## Qualification executed here

- All 18 cumulative Phase 40.13 static gates passed.
- REST/OpenAPI parity passed with 16 control-plane and 18 online operations.
- Registry, corpus, verification evidence, OpenAPI, Helm values, and values schema parsed cleanly.
- The supplied Phase 40.13.17 parent archive and all 974 parent manifest entries were verified
  before modification.

## Open gates

Cargo, rustc, rustfmt, Maven, Helm, kubectl, controlled public TLS SPARQL endpoints, and a multinode
cluster are unavailable here. Therefore native compilation/tests, official federated-query cases,
HTTP/DNS failure injection, Helm rendering, live HPA/node-autoscaler behavior, and pod/node churn
remain explicit release gates. The next roadmap increment is Phase 40.13.19 multinode storage and
recovery qualification.

No ontology alignment, schema matching, source-to-ontology mapping, or raw-data transformation was
added.
