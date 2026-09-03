#!/usr/bin/env python3
"""Static Phase 8 qualification for the CPU work plane and Kubernetes split."""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]

def require(path: str, *needles: str) -> None:
    value = (ROOT / path).read_text(encoding="utf-8")
    for needle in needles:
        if needle not in value:
            print(f"Phase 8 qualification: FAIL: {path} lacks {needle!r}", file=sys.stderr)
            raise SystemExit(1)

require("migrations-agents/0006_cpu_hpc_work_plane.sql", "FORCE ROW LEVEL SECURITY", "SKIP LOCKED", "claim_cpu_partition", "checkpoint_cpu_partition", "cpu_ready_partition_count", "terminal CPU workload is immutable")
require("crates/ngkg-hpc-runtime/src/lib.rs", "cgroup_cpu_count", "cgroup_memory_limit_bytes", "maximum_spill", "par_sort_unstable", "deterministic_partition_root")
require("crates/ngkg-cpu-work-plane/src/lib.rs", "CreateQualificationWorkload", "finalize_if_complete", "cpu_ready_partition_count")
require("services/qualification-worker/src/main.rs", "spawn_blocking", "get_verified", "ResourceBudget::from_cgroup", "checkpoint", "NGKG_QUALIFICATION_SPILL_ROOT")
require("services/mcp-gateway/src/qualification_api.rs", "/v1/qualification-workloads", "qualification:write", "qualification:read", "qualification:cancel")
require("contracts/mcp-agent-openapi.yaml", "createQualificationWorkload", "getQualificationCheckpoints", "cancelQualificationWorkload", "QualificationCheckpoint")
require("charts/ngkg-agents/templates/component-workloads.yaml", "NGKG_COMPONENT_ROLE", "kind: HorizontalPodAutoscaler", "averageUtilization", "topologySpreadConstraints", "podAntiAffinity")
require("charts/ngkg-agents/templates/qualification-worker.yaml", "emptyDir", "sizeLimit", "qualificationWorker.scheduling.nodeSelector", "safe-to-evict", "readOnlyRootFilesystem: true")
require("charts/ngkg-agents/templates/qualification-autoscaling.yaml", "kind: ScaledObject", "minReplicaCount: 0", "type: cpu", "type: memory", "ngkg_cpu_work_ready_partitions")
require("charts/ngkg-agents/templates/gateway-api-route.yaml", "ngkg-orchestrator", "ngkg-memory", "ngkg-tool-broker", "ngkg-mcp-gateway")

values = (ROOT / "charts/ngkg-agents/values.yaml").read_text(encoding="utf-8")
if values.count("cpuTargetPercent: 80") < 6 or values.count("memoryTargetPercent: 80") < 6:
    print("Phase 8 qualification: FAIL: every CPU workload does not declare 80% CPU and RAM scaling", file=sys.stderr)
    raise SystemExit(1)
if "maximumSpillBytes: 8589934592" not in values or "memoryFractionPercent: 70" not in values or "ngkg.io/workload-class: cpu-hpc" not in values:
    print("Phase 8 qualification: FAIL: bounded spill or cgroup memory reserve is missing", file=sys.stderr)
    raise SystemExit(1)

print("Phase 8 CPU Kubernetes deployment and HPC autoscaling qualification: PASS")
