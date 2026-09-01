# Managed agent execution

Phase 5 adds one authenticated route, `POST /v1/agent-executions`. It consumes a fully compiled Phase 4 `inputId`, an immutable agent-profile version, a provider/model allowlist entry, and a `CONSTRUCT` or `DESCRIBE` query. The gateway authenticates the tenant, executes the context query through NGKG's public API, pins the resulting published snapshot, and sends a bounded context package to the selected provider.

The provider is not trusted to answer. It may return only closed JSON containing canonical N-Triples. The Rust orchestrator converts each statement into a server-owned `ASK` query and runs that query against the same dataset, snapshot, authorized graph set, active dataset, and serving root. A false `ASK` is recorded as `UNKNOWN` under OWL open-world semantics; it is never treated as a contradiction or false fact. If any claim is invalid, unknown, federated, incomplete, or bound to different evidence, the request returns no partial answer and no certificate.

## Request

```json
{
  "datasetId": "11111111-1111-4111-8111-111111111111",
  "inputId": "22222222-2222-4222-8222-222222222222",
  "profileId": "33333333-3333-4333-8333-333333333333",
  "profileVersion": 1,
  "provider": "vllm",
  "modelId": "ngkg-model",
  "contextQuery": "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o } LIMIT 1000",
  "maximumOutputTokens": 2048,
  "temperatureMilli": 0
}
```

The bearer must contain `agents:execute` and the narrower upstream query delegation. Tenant identity is accepted only from verified authentication. The immutable profile must contain a matching `datasetIds` entry, model provider/ID entry, and `maximumClaims` limit.

## Certified response

HTTP 200 contains `executionId`, `answer`, and `certificate`. `answer` is deterministic N-Triples, not provider prose. The certificate binds the input source/compiled/requirement roots, context query execution, published snapshot, graph authorization, active dataset, serving root, semantic result, model request and response, every claim-validation query and proof reference, and the complete answer checksum.

## Provider Secret

`providers.json` follows `contracts/model-provider-config.schema.json`. OpenAI/ChatGPT-compatible APIs, Hugging Face TGI, and vLLM use `OPEN_AI_COMPATIBLE`; Claude uses `ANTHROPIC_MESSAGES`. External endpoints require HTTPS. Plain HTTP is accepted only for a single-label cluster-local service or loopback. Redirects are disabled, credentials are read only from checksum-bound mounted files, bodies and concurrency are bounded, and network policy must explicitly allow the provider CIDRs.

For the bundled vLLM service, copy `charts/ngkg-agents/examples/model-providers.vllm.json`, hash it, and create the Secret:

```bash
sha256sum model-providers.vllm.json
kubectl -n ngkg create secret generic ngkg-model-providers --from-file=providers.json=model-providers.vllm.json
helm upgrade --install ngkg-agents charts/ngkg-agents -n ngkg --atomic --wait \
  --set managedAgents.enabled=true \
  --set managedAgents.providerFileSha256=<sha256> \
  --set vllm.enabled=true \
  --set vllm.image.repository=<registry>/vllm \
  --set vllm.image.digest=sha256:<digest> \
  --set vllm.model=<model-or-mounted-path> \
  -f production-values.yaml
```

The chart schedules vLLM with `nvidia.com/gpu`, topology spread, a disruption budget, default-deny network policy, and immutable image digest. KEDA scales on waiting provider work plus 80% CPU or memory. Unschedulable GPU pods are the standard signal for Karpenter, Cluster Autoscaler, or the EKS/AKS/GKE/RKE/RKE2 node provisioner to add labeled GPU nodes. A working Metrics Server, Prometheus, KEDA, NVIDIA device plugin/operator, and provider node autoscaler are prerequisites.
