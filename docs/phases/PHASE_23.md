# Phase 23 — certified chunked Arrow IPC fragment exchange

Phase 23 builds cumulatively on Phase 22 and replaces its internal JSON binding responses with a real, typed, chunked Apache Arrow IPC stream. It does not change which queries are eligible, how offline HermiT output is certified, how named graphs are routed, or how final answers are admitted. The Phase 22 distributed plan and multiset certificates remain the semantic authority.

## Intent

Phase 22 proved that independently certified named-graph fragments can execute on multiple nodes and be joined without changing the complete result. Its response wire format still repeated JSON member names and RDF term shapes for every row. Phase 23 removes that serialization overhead while preserving exact RDF terms, unbound variables, duplicate solution rows, snapshot identity and every fail-closed bound.

The internal execution path is now:

```text
certified query coordinator
  -> authenticated REST fragment request with Arrow Accept header
  -> independent fragment workers on relevant named-graph shards
  -> bounded Arrow IPC stream in configurable row batches
  -> schema and certificate metadata validation
  -> exact Phase 22 bag join and final multiset validation
  -> deterministic GUID qualification and Parquet hydration
```

Public query, locator and hydration APIs remain REST/JSON. Only the worker-to-coordinator binding data plane uses `application/vnd.apache.arrow.stream`.

## Lossless columnar binding contract

The Arrow schema metadata contains:

- format version;
- dataset and snapshot UUIDs;
- exact query SHA-256;
- fragment identifier;
- worker identity;
- certified fragment multiset SHA-256;
- ordered variable names and count.

Each SPARQL variable is represented by four nullable columns:

```text
kind:uint8 | value:utf8 | datatype:utf8 | language:utf8
```

Kind `1` is a named node, `2` is a blank node and `3` is a literal. A bound named or blank node has no datatype or language. A bound literal has exactly one datatype or language. An unbound variable has four nulls. Any other combination is rejected. Binding rows are emitted and decoded in their original order; repeated rows are not deduplicated, so SPARQL bag semantics remain intact.

`NGKG_FRAGMENT_ARROW_BATCH_ROWS` bounds each record batch. The default Helm value is 8,192 rows and cannot exceed the distributed intermediate row ceiling. A bounded synchronous-to-async channel applies socket backpressure; `NGKG_FRAGMENT_ARROW_HTTP_CHUNK_BYTES` multiplied by `NGKG_FRAGMENT_ARROW_CHANNEL_CAPACITY` cannot exceed the response ceiling. The response writer stops before `NGKG_MAX_FRAGMENT_RESPONSE_BYTES`, the coordinator streams the HTTP body only up to that bound, and its atomic reservation prevents all concurrent fragment bodies from crossing `NGKG_MAX_DISTRIBUTED_EXCHANGE_BYTES`.

## Execution and failure semantics

The coordinator requires the Arrow media type before reading a successful worker response. Arrow parsing runs on the bounded blocking compute pool rather than a Tokio control thread. The decoder validates schema layout, metadata, term invariants and total row count before the result enters the join. The coordinator then repeats the Phase 22 checks for dataset, snapshot, query, fragment, worker, head, row count and fragment multiset.

Truncation, malformed IPC, a schema substitution, a type-code substitution, inconsistent datatype/language columns, missing certificate metadata, excessive rows, excessive bytes, a wrong media type or any Phase 22 certificate mismatch produces no partial result. There is no JSON compatibility downgrade on the certified fragment endpoint.

## Kubernetes, RKE2 and HPC behavior

Phase 23 retains the dedicated `sparql-fragment-processing` StatefulSet, headless Service, required host anti-affinity, taint/toleration, default-deny NetworkPolicy and Guaranteed-QoS resources from Phase 22. Independent fragment requests execute concurrently across worker nodes. Arrow encoding and decoding use record batches to cap temporary column allocation and make memory access columnar.

The sparse Oxigraph and hash-join path does not use dense matrix operations. OpenMP, OpenBLAS and MKL remain fixed to one thread per process to prevent nested oversubscription; Rust owns the bounded compute lanes. CPU and memory HPA targets remain capped at 80 percent. HPA creates responsibility-specific pods, and a separately installed Rancher Cluster Autoscaler grows only the labelled and tainted RKE2 machine pool when required anti-affinity leaves a new pod pending.

Phase 23 continues to require the documented service-mesh or CNI mTLS layer for in-cluster confidentiality. It implements Arrow IPC over authenticated REST on the existing port; it does not claim application-native TLS or Arrow Flight.

## Acceptance criteria

Phase 23 is accepted only when all of the following pass:

1. Pinned Rust 1.97.1 format, compilation, Clippy with warnings denied and all workspace tests pass with a generated lockfile committed.
2. Arrow round-trip tests preserve URI, blank-node, typed literal, language literal, unbound-variable and duplicate-row semantics across multiple record batches.
3. Corrupting the IPC footer/body, schema field order or type, term-kind code, null pattern, certificate metadata, head, row count or multiset causes a fail-closed response.
4. A deployed Phase 22 certified cross-domain query reports `exchangeFormat=arrow_ipc_stream_v1`, executes at least two fragments on at least two workers, and returns the exact independent binding and hydrated-payload result.
5. Fragment response and total-exchange byte ceilings, record-batch row ceilings, intermediate row ceilings, timeouts, partial streams and worker loss never return partial success.
6. A non-Arrow `Accept` request to the internal fragment endpoint is rejected, and a coordinator rejects any successful response whose `Content-Type` is not the Arrow stream media type.
7. Helm schema, lint, rendering, server-side dry-run, digest pinning, probes, disruption budgets and default-deny connectivity pass.
8. RKE2 static CPU/topology placement passes; sustained 79 percent fragment load does not cause resource-driven growth, sustained 80 percent does, and only the `sparql-fragment-processing` Rancher pool grows.
9. Measured enterprise binding corpora record Arrow and Phase 22 JSON wire bytes, encode/decode CPU, peak RSS, p50 and p95 latency. A performance claim is released only from those measurements; semantic equality is mandatory for every compared query.

Run deployed application qualification:

```bash
NGKG_ONLINE_QUERY_URL=https://ngkg.example \
NGKG_API_TOKEN="${NGKG_API_TOKEN}" \
NGKG_DATASET_ID=4d2e1a82-c2bc-536a-a809-fda7643ef1f7 \
NGKG_CERTIFIED_QUERY_FILE=test-corpus/queries/q01-cross-domain.rq \
NGKG_EXPECTED_RESULTS_FILE=test-corpus/expected/q01-cross-domain.srj \
NGKG_EXPECTED_ROUTING_FILE=test-corpus/routing/q01-cross-domain.json \
NGKG_KUBERNETES_NAMESPACE=ngkg \
scripts/qualify_phase23.sh
```

## Intentional boundary

Phase 23 implements Arrow IPC record-batch exchange for the exact Phase 22 certified fragment class. It does not implement Arrow Flight, partitioned hash shuffle, property-path frontiers, adaptive retries, arbitrary SPARQL decomposition, arbitrary OWL 2 DL query completeness, proof-DAG export, continuous updates or a universal 20–50x speedup. Those capabilities require separate code and equivalence gates. Unsupported queries continue through the complete certified local route or fail closed.
