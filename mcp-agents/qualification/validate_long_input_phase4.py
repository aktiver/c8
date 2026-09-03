#!/usr/bin/env python3
"""Source qualification for the Phase 4 trust, determinism and scaling boundary."""
from pathlib import Path
import json, re, sys

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

for name in ["prompt-manifest.schema.json","prompt-chunk.schema.json","prompt-requirement.schema.json","prompt-requirement-coverage.schema.json","compiled-context.schema.json"]:
    with (ROOT/"contracts"/name).open() as stream: json.load(stream)

require("contracts/agent-input-openapi.yaml","/agent-inputs/{inputId}/parts/{ordinal}","x-ngkg-content-sha256","agent-inputs:write","agent-inputs:read")
require("migrations-agents/0003_prompt_input_context_compiler.sql","ENABLE ROW LEVEL SECURITY","FORCE ROW LEVEL SECURITY","SECURITY DEFINER SET search_path=pg_catalog,ngkg_agents","FOR UPDATE SKIP LOCKED","prompt_requirement_coverage","reject_prompt_mutation")
require("crates/ngkg-agent-input/src/compiler.rs","par_iter()","chunks.sort_by_key","requirements.sort_by","ngkg-prompt-compiled-part-v1","byte_start","heading_path")
require("crates/ngkg-agent-input/src/storage.rs","AmazonS3Builder::from_env()","MicrosoftAzureBuilder::from_env()","GoogleCloudStorageBuilder::from_env()","get_verified")
require("services/mcp-gateway/src/input_api.rs","agent-inputs:write","agent-inputs:read","x-ngkg-content-sha256",'"REDACTED".clone_into(&mut part.object_reference)')
require("services/prompt-compiler/src/main.rs","cgroup_cpu_count","ThreadPoolBuilder","claim_shard","finalize_compilation","SOURCE_VERIFICATION_FAILED")
require("charts/ngkg-agents/values.yaml","cpuTargetPercent: 80","memoryTargetPercent: 80","provider: s3","azure:","gcs:","serviceAccountAnnotations")
require("charts/ngkg-agents/templates/prompt-compiler.yaml","topologySpreadConstraints","ngkg-prompt-compiler","readOnlyRootFilesystem: true")
require("charts/ngkg-agents/templates/prompt-autoscaling.yaml","averageUtilization:","cpuTargetPercent","memoryTargetPercent")
forbid("crates/ngkg-agent-input/Cargo.toml","apache jena","hermit","openmp")

sql=(ROOT/"migrations-agents/0003_prompt_input_context_compiler.sql").read_text()
tables=re.findall(r"CREATE TABLE ngkg_agents\.([a-z_]+)",sql)
for table in tables:
    if f"'{table}'" not in sql and table not in {"prompt_input","prompt_compilation_queue"}: errors.append(f"{table}: no RLS registration")
if "REVOKE ALL ON ngkg_agents.prompt_compilation_queue" not in (ROOT/"crates/ngkg-agent-catalog/src/lib.rs").read_text(): errors.append("opaque queue must have all direct runtime privileges revoked")
if "Raw bytes are never stored" not in sql: errors.append("SQL must document raw-byte storage boundary")
if errors:
    print("Phase 4 qualification: FAIL",file=sys.stderr)
    for error in errors: print(f"- {error}",file=sys.stderr)
    raise SystemExit(1)
print(f"Phase 4 long-input source qualification: PASS ({len(tables)} tenant tables)")
