#!/usr/bin/env python3
"""Static qualification of Phase 5 semantic and Kubernetes trust boundaries."""
from pathlib import Path
import json, sys

ROOT=Path(__file__).resolve().parents[1]
errors=[]
def require(path,*needles):
    text=(ROOT/path).read_text()
    for needle in needles:
        if needle not in text: errors.append(f"{path}: missing {needle}")
def forbid(path,*needles):
    text=(ROOT/path).read_text().lower()
    for needle in needles:
        if needle.lower() in text: errors.append(f"{path}: forbidden {needle}")

for name in ["managed-agent-request.schema.json","model-provider-config.schema.json","model-proposal.schema.json","answer-certificate.schema.json"]:
    with (ROOT/"contracts"/name).open() as stream: json.load(stream)

require("crates/ngkg-agent-orchestrator/src/lib.rs",
    "canonical_ntriple_to_ask","validate_same_snapshot",
    "ClaimVerdict::Unknown","OPEN_WORLD_UNKNOWN","COMPLETE_RDF_ANSWER","certificate_sha256",
    "context.reasoning.federated","SemanticStatus::FederatedVolatile","complete_answer_certificate")
require("crates/ngkg-model-provider/src/lib.rs",
    "Policy::none()","maximum_request_bytes","maximum_response_bytes","Semaphore",
    "from_checksum_bound_file","credential_file_sha256","Provider output is an untrusted proposal")
require("services/mcp-gateway/src/agent_api.rs","agents:execute","AGENT_EXECUTION","answer_not_certified")
require("migrations-agents/0004_managed_orchestrator.sql","ENABLE ROW LEVEL SECURITY","FORCE ROW LEVEL SECURITY","immutable_rows","agent_answer_certificate")
require("charts/ngkg-agents/values.yaml","nvidia.com/gpu","cpuTargetPercent: 80","memoryTargetPercent: 80","providerFileSha256")
require("charts/ngkg-agents/templates/vllm.yaml","topologySpreadConstraints","nodeSelector","tensor-parallel-size","image: '")
require("charts/ngkg-agents/templates/vllm-autoscaling.yaml","kind: ScaledObject","ngkg_inference_waiting_requests","type: cpu","type: memory")
require("charts/ngkg-agents/templates/vllm-network-policy.yaml","default-deny","inference-gateway","vllm-backend")
forbid("crates/ngkg-agent-orchestrator/Cargo.toml","apache jena","hermit","openmp","kube")
forbid("crates/ngkg-model-provider/Cargo.toml","apache jena","hermit")

orchestrator=(ROOT/"crates/ngkg-agent-orchestrator/src/lib.rs").read_text()
compact_orchestrator="".join(orchestrator.split())
if "snapshot_id:Some(context.snapshot_id)" not in compact_orchestrator: errors.append("validation query must pin the context snapshot")
if "ASK {{ {subject} {predicate} {object} . }}" not in orchestrator: errors.append("server-owned ASK builder is absent")
if "if!entailed{returnErr(OrchestratorError::UncertifiedClaim);}" not in compact_orchestrator: errors.append("unknown claim must prevent whole-answer certificate")
if orchestrator.index("record_claim_validation") > orchestrator.index("complete_answer_certificate"): errors.append("claim validation must precede atomic certificate completion")
if errors:
    print("Phase 5 qualification: FAIL",file=sys.stderr)
    for error in errors: print(f"- {error}",file=sys.stderr)
    raise SystemExit(1)
print("Phase 5 managed-agent source qualification: PASS")
