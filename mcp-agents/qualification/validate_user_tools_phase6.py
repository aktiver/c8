#!/usr/bin/env python3
"""Static Phase 6 qualification for tenant MCP tools and trust boundaries."""
from pathlib import Path
import json,sys
ROOT=Path(__file__).resolve().parents[1];errors=[]
def require(path,*values):
 text=(ROOT/path).read_text()
 for value in values:
  if value not in text: errors.append(f"{path}: missing {value}")
def forbid(path,*values):
 text=(ROOT/path).read_text().lower()
 for value in values:
  if value.lower() in text: errors.append(f"{path}: forbidden {value}")
for name in ["tool-provider.schema.json","qualified-tool-catalog.schema.json","tool-call.schema.json","tool-approval.schema.json"]:
 with (ROOT/"contracts"/name).open() as stream: json.load(stream)
require("crates/ngkg-tool-broker/src/lib.rs","notifications/initialized","tools/list","tools/call","RedirectPolicy::none()","https_only(true)","lookup_host","mcp-protocol-version","safe_address","UNTRUSTED_EXTERNAL_TOOL","context_certificate_sha256","load_tool_execution_context","load_tool_catalog","load_approval","ApprovalDecision::Approved","readOnlyHint","allow_side_effects","validate_schema","validate_value","maximum_in_flight","bounded_body","parse_sse","record_qualified_tool_provider_and_catalog")
require("services/mcp-gateway/src/tool_api.rs","tools:providers:write","tools:approve","tools:execute","AuditOutcome::Denied","USER_TOOL")
require("services/mcp-gateway/src/main.rs","tool_routes","DefaultBodyLimit::max")
require("charts/ngkg-agents/values.yaml","toolBroker:","cpuTargetPercent: 80","memoryTargetPercent: 80","externalEgressIpBlocks","allowClusterPrivateEndpoints: false")
require("charts/ngkg-agents/templates/network-policy.yaml","toolBroker.externalEgressIpBlocks")
require("docs/USER_TOOL_BROKER.md","never inserted into the reasoned graph","DNS resolution pinning","RKE/RKE2, EKS, AKS, GKE")
forbid("crates/ngkg-tool-broker/Cargo.toml","apache jena","hermit","kube","sqlx")
broker=(ROOT/"crates/ngkg-tool-broker/src/lib.rs").read_text()
compact_broker="".join(broker.split())
if ".resolve(&host,pinned)" not in compact_broker: errors.append("HTTP client must pin the qualified DNS resolution")
if "canonical_secret.parent()!=Some(credential_root.as_path())" not in compact_broker: errors.append("credential file must be a direct child of the configured credential root")
if compact_broker.index("load_tool_catalog")>compact_broker.index("tools.iter().find"): errors.append("qualified catalog must be loaded before tool lookup")
if "execution.result_sha256!=Some(context_hash)" not in compact_broker: errors.append("tool call must bind the Phase 5 answer certificate")
if "!read_only||policy.requires_approval" not in compact_broker: errors.append("side-effecting tools must require approval")
gateway_compact="".join((ROOT/"services/mcp-gateway/src/main.rs").read_text().split())
if "DefaultBodyLimit::max(configuration.maximum_mcp_request_bytes,)" not in gateway_compact: errors.append("MCP request body limit must be applied")
if errors:
 print("Phase 6 qualification: FAIL",file=sys.stderr)
 for error in errors: print(f"- {error}",file=sys.stderr)
 raise SystemExit(1)
print("Phase 6 qualified user-tool source qualification: PASS")
