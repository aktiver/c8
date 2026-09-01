#!/usr/bin/env python3
"""Static Phase 7 qualification for evidence-bound memory and API parity."""
from pathlib import Path
import json
import re
import sys
import yaml

ROOT = Path(__file__).resolve().parents[1]

def require(condition: bool, message: str) -> None:
    if not condition:
        print(f"Phase 7 qualification: FAIL: {message}", file=sys.stderr)
        raise SystemExit(1)

migration = (ROOT / "migrations-agents/0005_evidence_bound_agent_memory.sql").read_text()
memory = (ROOT / "crates/ngkg-agent-memory/src/lib.rs").read_text()
rest = (ROOT / "services/mcp-gateway/src/memory_api.rs").read_text()
query_rest = (ROOT / "services/mcp-gateway/src/query_api.rs").read_text()
gateway = (ROOT / "services/mcp-gateway/src/main.rs").read_text()
openapi = yaml.safe_load((ROOT / "contracts/mcp-agent-openapi.yaml").read_text())
values = yaml.safe_load((ROOT / "charts/ngkg-agents/values.yaml").read_text())
schema = json.loads((ROOT / "charts/ngkg-agents/values.schema.json").read_text())

tables = ["agent_memory", "agent_memory_version", "agent_memory_transition", "agent_memory_edge", "agent_memory_publication"]
for table in tables:
    require(f"CREATE TABLE ngkg_agents.{table}" in migration, f"missing {table}")
require("FORCE ROW LEVEL SECURITY" in migration and "current_tenant_id()" in migration, "forced tenant RLS missing")
require("reject_agent_memory_delete" in migration, "memory delete guard missing")
require("validate_agent_memory_transition" in migration and "state_version" in migration, "CAS lifecycle guard missing")
for state in ["PROPOSED", "UNKNOWN", "APPROVAL_REQUIRED", "PUBLISHED", "SUPERSEDED", "REVOKED"]:
    require(state in migration, f"missing lifecycle state {state}")

for class_name in ["Working", "Episodic", "Semantic", "Procedural", "Evidence"]:
    require(class_name in memory, f"missing memory class {class_name}")
for method in ["propose", "search", "validate", "approve", "publish", "revoke", "supersede", "explain"]:
    require(f"pub async fn {method}" in memory, f"missing service method {method}")
require("unknown_is_false" in memory and "FederatedVolatile" in memory, "open-world/federation guard missing")
require("semantic_statements" in memory and "answer_certificate_sha256" in memory, "certificate-bound RDF subset validation missing")
require("published_snapshot_id" in memory and "entail_all" in memory, "published-snapshot re-entailment missing")
require(not any(term in (ROOT / "crates/ngkg-agent-memory/Cargo.toml").read_text() for term in ["jena", "hermit", "ngkg-query", "ngkg-reason", "kube"]), "memory crate crosses the public API boundary")

rest_paths = {
    ("POST", "/v1/memories"), ("POST", "/v1/memories/search"),
    ("GET", "/v1/memories/{memoryId}"), ("GET", "/v1/memories/{memoryId}/explain"),
    ("POST", "/v1/memories/{memoryId}/validate"), ("POST", "/v1/memories/{memoryId}/approve"),
    ("POST", "/v1/memories/{memoryId}/publish"), ("POST", "/v1/memories/{memoryId}/supersede"),
    ("POST", "/v1/memories/{memoryId}/revoke"),
}
observed = {(method.upper(), path) for path, item in openapi["paths"].items() for method in item if method.lower() in {"get", "post", "put", "patch", "delete"}}
require(rest_paths <= observed, f"OpenAPI missing memory operations: {sorted(rest_paths-observed)}")
for path, item in openapi["paths"].items():
    for method, operation in item.items():
        if isinstance(operation, dict) and operation.get("x-mcp-tool"):
            require(operation["x-mcp-tool"] in gateway, f"OpenAPI MCP mapping absent in server: {operation['x-mcp-tool']}")
        for tool in operation.get("x-mcp-tools", []) if isinstance(operation, dict) else []:
            require(tool in gateway, f"OpenAPI MCP mapping absent in server: {tool}")
documented_tools = set()
for item in openapi["paths"].values():
    for operation in item.values():
        if not isinstance(operation, dict):
            continue
        if operation.get("x-mcp-tool"):
            documented_tools.add(operation["x-mcp-tool"])
        documented_tools.update(operation.get("x-mcp-tools", []))
implemented_tools = set(re.findall(r"async fn (ngkg_[a-z0-9_]+)\(", gateway))
require(implemented_tools == documented_tools, f"MCP/REST parity drift: implemented={sorted(implemented_tools)}, documented={sorted(documented_tools)}")
for tool in ["ngkg_memory_propose", "ngkg_memory_search", "ngkg_memory_explain", "ngkg_memory_validate", "ngkg_memory_approve", "ngkg_memory_publish", "ngkg_memory_supersede", "ngkg_memory_revoke"]:
    require(tool in gateway, f"missing MCP tool {tool}")
require("memory:publish" in rest and "memory:approve" in rest and "memory:validate" in rest, "separate memory scopes missing")
require("query_tools_enabled" in query_rest and "AuditOutcome::Denied" in query_rest, "REST query routes bypass the MCP deployment policy")
require("/openapi.yaml" in (ROOT / "services/mcp-gateway/src/openapi.rs").read_text(), "served OpenAPI missing")
require("/swagger-ui" in (ROOT / "services/mcp-gateway/src/openapi.rs").read_text(), "Swagger UI missing")
require(values["memory"]["enabled"] is True, "memory must default enabled")
require(values["autoscaling"]["cpuTargetPercent"] == 80 and values["autoscaling"]["memoryTargetPercent"] == 80, "gateway 80% autoscaling missing")
require(schema["properties"]["memory"]["additionalProperties"] is False, "closed memory Helm schema missing")

print("Phase 7 evidence-bound memory and REST/OpenAPI parity qualification: PASS")
