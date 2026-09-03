#!/usr/bin/env python3
"""Deterministic source/configuration gate for Phase 9 vLLM/GPU deployment."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import yaml

ROOT = Path(__file__).resolve().parents[1]
ERRORS: list[str] = []


def require(path: str, *needles: str) -> None:
    value = (ROOT / path).read_text(encoding="utf-8")
    for needle in needles:
        if needle not in value:
            ERRORS.append(f"{path}: missing {needle!r}")


def forbid(path: str, *needles: str) -> None:
    value = (ROOT / path).read_text(encoding="utf-8").lower()
    for needle in needles:
        if needle.lower() in value:
            ERRORS.append(f"{path}: forbidden {needle!r}")


for contract in ["inference-gateway-openapi.yaml", "vllm-pod-agent-openapi.yaml"]:
    with (ROOT / "contracts" / contract).open(encoding="utf-8") as stream:
        document = yaml.safe_load(stream)
    if document.get("openapi") != "3.1.0":
        ERRORS.append(f"{contract}: must declare OpenAPI 3.1.0")

with (ROOT / "charts/ngkg-agents/values.schema.json").open(encoding="utf-8") as stream:
    json.load(stream)
with (ROOT / "charts/ngkg-agents/values.yaml").open(encoding="utf-8") as stream:
    values = yaml.safe_load(stream)

vllm = values["vllm"]
admission = values["inferenceGateway"]
if vllm["autoscaling"]["minReplicas"] != 0:
    ERRORS.append("vllm.autoscaling.minReplicas must default to zero")
for section in (vllm["autoscaling"], admission["autoscaling"]):
    if section["cpuTargetPercent"] != 80 or section["memoryTargetPercent"] != 80:
        ERRORS.append("GPU and admission workloads must retain 80% CPU-or-RAM scaling")
if admission["replicaCount"] < 2 or admission["maximumWaiting"] < admission["maximumInFlight"]:
    ERRORS.append("admission gateway must be HA with queue capacity no smaller than execution lanes")
if vllm["terminationGracePeriodSeconds"] <= vllm["drainTimeoutSeconds"]:
    ERRORS.append("termination grace must exceed the drain deadline")
if vllm["tensorParallelSize"] != int(vllm["resources"]["limits"]["nvidia.com/gpu"]):
    ERRORS.append("tensor parallel size must match GPU allocation")

require(
    "services/inference-gateway/src/main.rs",
    "INFERENCE_QUEUE_FULL",
    "GPU_COLD_START_TIMEOUT",
    "ngkg_inference_waiting_requests",
    "A POST is deliberately attempted once",
    "GaugeGuard",
    "SERVED_MODEL_MISMATCH",
)
require(
    "services/vllm-pod-agent/src/main.rs",
    "admin listener must be loopback-only",
    "upstream.host_str() == Some(\"127.0.0.1\")",
    "verify_upstream",
    "v1/models",
    "VLLM_BACKEND_NOT_READY",
    "InFlightGuard",
)
require(
    "charts/ngkg-agents/templates/inference-gateway.yaml",
    "kind: HorizontalPodAutoscaler",
    "averageUtilization",
    "ngkg-inference-gateway",
    "topologySpreadConstraints",
    "readOnlyRootFilesystem: true",
)
require(
    "charts/ngkg-agents/templates/vllm.yaml",
    "--host=127.0.0.1",
    "ngkg-vllm-pod-agent",
    "ngkg-vllm-backend",
    "/admin/drain",
    "safe-to-evict",
)
require(
    "charts/ngkg-agents/templates/vllm-autoscaling.yaml",
    "minReplicaCount:",
    "ngkg_inference_waiting_requests",
    "activationThreshold",
    "type: cpu",
    "type: memory",
)
require(
    "charts/ngkg-agents/templates/vllm-network-policy.yaml",
    "default-deny",
    "inference-gateway",
    "vllm-backend",
    "modelSourceEgressIpBlocks",
)
require(
    "charts/ngkg-agents/templates/phase9-validation.yaml",
    "tensorParallelSize must equal",
    "nvidia.com/gpu",
    "minReplicas=0 requires KEDA",
    "ports must be distinct",
)
require(
    "deploy/mcp-gateway/Dockerfile",
    "--package ngkg-inference-gateway",
    "--package ngkg-vllm-pod-agent",
)

for provider in ("eks", "aks", "gke", "rke2"):
    profile = f"charts/ngkg-agents/profiles/{provider}-gpu.yaml"
    require(profile, f"provider: {provider}", "ngkg.io/accelerator", "NoSchedule")
    with (ROOT / profile).open(encoding="utf-8") as stream:
        yaml.safe_load(stream)
require("charts/ngkg-agents/profiles/rke-gpu.yaml", "provider: rke2", "ngkg.io/accelerator", "NoSchedule")

for script in (
    "deploy/gpu-node-provisioning/aks-create-nodepool.sh",
    "deploy/gpu-node-provisioning/gke-create-nodepool.sh",
    "qualification/run_phase9_gpu_e2e.sh",
):
    check = subprocess.run(["bash", "-n", str(ROOT / script)], check=False, capture_output=True, text=True)
    if check.returncode:
        ERRORS.append(f"{script}: bash -n failed: {check.stderr.strip()}")

for path in (
    "charts/ngkg-agents/values.yaml",
    "charts/ngkg-agents/templates/vllm.yaml",
    "charts/ngkg-agents/profiles/eks-gpu.yaml",
    "charts/ngkg-agents/profiles/aks-gpu.yaml",
    "charts/ngkg-agents/profiles/gke-gpu.yaml",
    "charts/ngkg-agents/profiles/rke2-gpu.yaml",
    "charts/ngkg-agents/profiles/rke-gpu.yaml",
):
    forbid(path, "AKIA", "client_secret:", "password:", "hf_token:")

if ERRORS:
    print("Phase 9 vLLM/GPU qualification: FAIL", file=sys.stderr)
    for error in ERRORS:
        print(f"- {error}", file=sys.stderr)
    raise SystemExit(1)

print("Phase 9 vLLM/GPU source and configuration qualification: PASS")
