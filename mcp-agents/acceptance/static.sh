#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GA_ROOT="$(cd "${ROOT}/../NGKG_1_0_0_GA" && pwd)"

required=(
  "${ROOT}/Cargo.toml"
  "${ROOT}/SOURCE_MANIFEST_SHA256.txt"
  "${ROOT}/generated-contract-manifest.json"
  "${ROOT}/contracts/reasoned-context-envelope.schema.json"
  "${ROOT}/contracts/agent-audit-event.schema.json"
  "${ROOT}/contracts/agent-execution.schema.json"
  "${ROOT}/contracts/ngkg-delegation-claims.schema.json"
  "${ROOT}/contracts/ngkg-1.1-authenticated-identity.schema.json"
  "${ROOT}/contracts/oauth-protected-resource-metadata.schema.json"
  "${ROOT}/crates/ngkg-auth/src/lib.rs"
  "${ROOT}/crates/ngkg-api-client/src/lib.rs"
  "${ROOT}/crates/ngkg-agent-catalog/src/lib.rs"
  "${ROOT}/crates/ngkg-mcp-contracts/src/lib.rs"
  "${ROOT}/migrations-agents/0001_agent_catalog.sql"
  "${ROOT}/migrations-agents/0002_agent_audit.sql"
  "${ROOT}/migrations-agents/0003_prompt_input_context_compiler.sql"
  "${ROOT}/migrations-agents/0004_managed_orchestrator.sql"
  "${ROOT}/contracts/agent-input-openapi.yaml"
  "${ROOT}/contracts/prompt-manifest.schema.json"
  "${ROOT}/contracts/prompt-chunk.schema.json"
  "${ROOT}/contracts/prompt-requirement.schema.json"
  "${ROOT}/contracts/prompt-requirement-coverage.schema.json"
  "${ROOT}/contracts/compiled-context.schema.json"
  "${ROOT}/crates/ngkg-agent-input/src/compiler.rs"
  "${ROOT}/crates/ngkg-agent-input/src/repository.rs"
  "${ROOT}/crates/ngkg-agent-input/src/storage.rs"
  "${ROOT}/services/mcp-gateway/src/input_api.rs"
  "${ROOT}/services/prompt-compiler/src/main.rs"
  "${ROOT}/crates/ngkg-model-provider/src/lib.rs"
  "${ROOT}/crates/ngkg-agent-orchestrator/src/lib.rs"
  "${ROOT}/services/mcp-gateway/src/agent_api.rs"
  "${ROOT}/contracts/managed-agent-openapi.yaml"
  "${ROOT}/contracts/managed-agent-request.schema.json"
  "${ROOT}/contracts/model-provider-config.schema.json"
  "${ROOT}/contracts/model-proposal.schema.json"
  "${ROOT}/contracts/answer-certificate.schema.json"
  "${ROOT}/contracts/tool-provider.schema.json"
  "${ROOT}/contracts/qualified-tool-catalog.schema.json"
  "${ROOT}/contracts/tool-call.schema.json"
  "${ROOT}/contracts/tool-approval.schema.json"
  "${ROOT}/contracts/tool-broker-openapi.yaml"
  "${ROOT}/crates/ngkg-tool-broker/src/lib.rs"
  "${ROOT}/services/mcp-gateway/src/tool_api.rs"
  "${ROOT}/migrations-agents/0005_evidence_bound_agent_memory.sql"
  "${ROOT}/crates/ngkg-agent-memory/src/lib.rs"
  "${ROOT}/services/mcp-gateway/src/memory_api.rs"
  "${ROOT}/services/mcp-gateway/src/query_api.rs"
  "${ROOT}/services/mcp-gateway/src/openapi.rs"
  "${ROOT}/contracts/mcp-agent-openapi.yaml"
  "${ROOT}/contracts/agent-memory-openapi.yaml"
  "${ROOT}/contracts/memory-proposal.schema.json"
  "${ROOT}/contracts/memory-view.schema.json"
  "${ROOT}/contracts/memory-search.schema.json"
  "${ROOT}/contracts/memory-publication.schema.json"
  "${ROOT}/services/mcp-gateway/src/main.rs"
  "${ROOT}/services/catalog-migrator/src/main.rs"
  "${ROOT}/charts/ngkg-agents/Chart.yaml"
  "${ROOT}/charts/ngkg-agents/values.schema.json"
  "${ROOT}/migrations-agents/0006_cpu_hpc_work_plane.sql"
  "${ROOT}/crates/ngkg-hpc-runtime/src/lib.rs"
  "${ROOT}/crates/ngkg-cpu-work-plane/src/lib.rs"
  "${ROOT}/services/qualification-worker/src/main.rs"
  "${ROOT}/services/mcp-gateway/src/qualification_api.rs"
  "${ROOT}/services/inference-gateway/src/main.rs"
  "${ROOT}/services/vllm-pod-agent/src/main.rs"
  "${ROOT}/migrations-agents/0007_context_slice_broker.sql"
  "${ROOT}/crates/ngkg-context-slice/src/index.rs"
  "${ROOT}/crates/ngkg-context-slice/src/capability.rs"
  "${ROOT}/crates/ngkg-context-slice/src/repository.rs"
  "${ROOT}/crates/ngkg-context-slice/src/storage.rs"
  "${ROOT}/services/context-slice-broker/src/main.rs"
  "${ROOT}/services/context-slice-gc/src/main.rs"
  "${ROOT}/contracts/context-slice-openapi.yaml"
  "${ROOT}/charts/ngkg-agents/templates/context-slice.yaml"
  "${ROOT}/charts/ngkg-agents/templates/context-slice-network-policy.yaml"
  "${ROOT}/docs/CONTEXT_SLICE_BROKER.md"
  "${ROOT}/qualification/run_phase10_context_slice_e2e.sh"
  "${ROOT}/contracts/inference-gateway-openapi.yaml"
  "${ROOT}/contracts/vllm-pod-agent-openapi.yaml"
  "${ROOT}/charts/ngkg-agents/templates/inference-gateway.yaml"
  "${ROOT}/charts/ngkg-agents/templates/vllm.yaml"
  "${ROOT}/charts/ngkg-agents/templates/vllm-autoscaling.yaml"
  "${ROOT}/charts/ngkg-agents/templates/vllm-network-policy.yaml"
)
for file in "${required[@]}"; do
  test -s "${file}"
done

(cd "${ROOT}" && sha256sum --quiet -c SOURCE_MANIFEST_SHA256.txt)

python3 -m json.tool "${ROOT}/generated-contract-manifest.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/generated-contract-manifest.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/query-tool-request.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/reasoned-context-envelope.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/agent-audit-event.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/agent-execution.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/ngkg-delegation-claims.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/ngkg-1.1-authenticated-identity.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/oauth-protected-resource-metadata.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/prompt-manifest.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/prompt-chunk.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/prompt-requirement.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/prompt-requirement-coverage.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/compiled-context.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/managed-agent-request.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/model-provider-config.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/model-proposal.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/answer-certificate.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/tool-provider.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/qualified-tool-catalog.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/tool-call.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/tool-approval.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/memory-proposal.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/memory-view.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/memory-search.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/contracts/memory-publication.schema.json" >/dev/null
python3 -m json.tool "${ROOT}/charts/ngkg-agents/values.schema.json" >/dev/null
python3 "${ROOT}/qualification/validate_agent_catalog.py"
python3 "${ROOT}/qualification/validate_auth_phase3.py"
python3 "${ROOT}/qualification/validate_long_input_phase4.py"
python3 "${ROOT}/qualification/validate_managed_agent_phase5.py"
python3 "${ROOT}/qualification/validate_user_tools_phase6.py"
python3 "${ROOT}/qualification/validate_agent_memory_phase7.py"
python3 "${ROOT}/qualification/validate_cpu_kubernetes_phase8.py"
python3 "${ROOT}/qualification/validate_vllm_gpu_phase9.py"
python3 "${ROOT}/qualification/validate_context_slice_phase10.py"

observed_online="$(sha256sum "${GA_ROOT}/api/online-openapi.yaml" | cut -d' ' -f1)"
observed_control="$(sha256sum "${GA_ROOT}/api/openapi.yaml" | cut -d' ' -f1)"
expected_online="$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(next(row["sha256"] for row in d["inputs"] if row["path"].endswith("online-openapi.yaml")))' "${ROOT}/generated-contract-manifest.json")"
expected_control="$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(next(row["sha256"] for row in d["inputs"] if row["path"].endswith("/openapi.yaml")))' "${ROOT}/generated-contract-manifest.json")"
test "${observed_online}" = "${expected_online}"
test "${observed_control}" = "${expected_control}"

client="${ROOT}/crates/ngkg-api-client/src/lib.rs"
if rg -n '(/fragments|/shuffles|/algebra|/paths|/locate|/hydrate)' "${client}"; then
  echo "forbidden NGKG internal route found in public client" >&2
  exit 1
fi
if rg -n '(ngkg-query|ngkg-catalog|ngkg-reason|ngkg-federation|sqlx|kube)' \
  "${ROOT}/crates/ngkg-api-client/Cargo.toml" \
  "${ROOT}/crates/ngkg-mcp-contracts/Cargo.toml" \
  "${ROOT}/services/mcp-gateway/Cargo.toml"; then
  echo "forbidden direct internal dependency found" >&2
  exit 1
fi
rg -q 'with_allowed_hosts' "${ROOT}/services/mcp-gateway/src/main.rs"
rg -q 'with_allowed_origins' "${ROOT}/services/mcp-gateway/src/main.rs"
rg -q 'Policy::none' "${client}"
rg -q 'FEDERATED_VOLATILE' "${ROOT}/contracts/reasoned-context-envelope.schema.json"
rg -q 'cpuTargetPercent: 80' "${ROOT}/charts/ngkg-agents/values.yaml"
rg -q 'memoryTargetPercent: 80' "${ROOT}/charts/ngkg-agents/values.yaml"
rg -q 'readOnlyRootFilesystem: true' "${ROOT}/charts/ngkg-agents/templates/gateway.yaml"
rg -q 'default-deny' "${ROOT}/charts/ngkg-agents/templates/network-policy.yaml"
rg -q 'runtimeSecret' "${ROOT}/charts/ngkg-agents/templates/gateway.yaml"
rg -q 'migrationSecret' "${ROOT}/charts/ngkg-agents/templates/catalog-migration.yaml"
rg -q 'AuditOutcome::Denied' "${ROOT}/services/mcp-gateway/src/main.rs"
rg -q 'append_audit_event' "${ROOT}/services/mcp-gateway/src/audit.rs"
rg -q 'subject: identity.subject.clone()' "${ROOT}/services/mcp-gateway/src/audit.rs"
rg -q 'actor: identity.actor.clone()' "${ROOT}/services/mcp-gateway/src/audit.rs"
rg -q 'version = "=11.0.0"' "${ROOT}/Cargo.toml"

echo "NGKG MCP Agent 0.10.0 static acceptance: PASS"
