#!/usr/bin/env python3
"""Fail-closed source/deployment checks for Phase 40.13.20."""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(relative: str, *tokens: str) -> str:
    value = (ROOT / relative).read_text(encoding="utf-8")
    for token in tokens:
        if token not in value:
            raise RuntimeError(f"{relative} is missing {token!r}")
    return value


def main() -> int:
    core = require(
        "crates/ngkg-autoscaling/src/lib.rs",
        "PRODUCTION_SATURATION_TARGET_PERCENT: u8 = 80",
        "cpu_percent >= policy.cpu_target_percent",
        "memory_percent >= policy.memory_target_percent",
        "maximum_node_saturation",
        "ScaleFromZero",
        "ScaleInBlocked",
        "checkpoint-or-spill-active",
        "certify_autoscaling",
        "baseline_result_sha256 != evidence.scaled_result_sha256",
        "node_provisioner_observed",
        "kueue_admission_observed",
    )
    runtime = require(
        "crates/ngkg-hpc-runtime/src/lib.rs",
        "memory.max",
        "memory.current",
        "usable_memory_bytes",
        "validate_buffer_budget",
        "Guaranteed-QoS memory limit is required",
    )
    worker = require(
        "services/storage-recovery-worker/src/main.rs",
        "resource_envelope_report",
        "validate_runtime_envelope",
        "admitted_multipart_bytes",
    )
    operator = require(
        "services/storage-recovery-operator/src/main.rs",
        "NGKG_NODE_SATURATION_TARGET_PERCENT",
        "production 80-percent headroom target",
    )
    cloud_mounts = require(
        "services/operator/src/main.rs",
        "s3.csi.aws.com",
        "blob.csi.azure.com",
        "gcsfuse.csi.storage.gke.io",
        "required cloud source CSI driver is not registered",
        "gke-gcsfuse/volumes",
        "read_only: Some(true)",
    )
    for schema in (
        "autoscaling-pool-policy.schema.json",
        "autoscaling-decision.schema.json",
        "autoscaling-qualification-certificate.schema.json",
    ):
        document = json.loads((ROOT / "contracts" / schema).read_text(encoding="utf-8"))
        if document.get("additionalProperties") is not False:
            raise RuntimeError(f"{schema} is not fail closed")
    values = yaml.safe_load((ROOT / "charts/ngkg-workloads/values.yaml").read_text(encoding="utf-8"))
    production = values["productionAutoscaling"]
    if production["cpuTargetPercent"] != 80 or production["memoryTargetPercent"] != 80:
        raise RuntimeError("production CPU and memory thresholds are not exactly 80")
    platform_values = yaml.safe_load(
        (ROOT / "charts/ngkg-platform/values.yaml").read_text(encoding="utf-8")
    )
    if platform_values["api"]["autoscaling"]["cpuUtilizationTargetPercent"] != 80 \
            or platform_values["api"]["autoscaling"]["memoryUtilizationTargetPercent"] != 80:
        raise RuntimeError("control-plane API HPA thresholds are not exactly 80")
    if platform_values["storageRecovery"]["nodeSaturationTargetPercent"] != 80:
        raise RuntimeError("storage recovery threshold is not exactly 80")
    profile = yaml.safe_load(
        (ROOT / "charts/ngkg-workloads/profiles/phase40.13.20-production.yaml").read_text(encoding="utf-8")
    )
    if not profile["metricsApis"]["requireResourceMetrics"] \
            or not profile["metricsApis"]["requireCustomMetrics"]:
        raise RuntimeError("production overlay does not require portable metrics APIs")
    if not profile["metrics"]["workloadAwareAutoscalingEnabled"]:
        raise RuntimeError("production workload metrics are disabled")
    if profile["autoscaling"]["parquetHydration"]["owner"] != "keda":
        raise RuntimeError("production overlay does not exercise one KEDA-owned workload")
    providers = {
        "generic": ("external-cluster-autoscaler", "cluster-autoscaler"),
        "rke": ("rancher-cluster-autoscaler", "cluster-autoscaler"),
        "rke2": ("rancher-cluster-autoscaler", "cluster-autoscaler"),
        "eks": ("eks-karpenter", "karpenter"),
        "aks": ("aks-managed-cluster-autoscaler", "cluster-autoscaler"),
        "gke": ("gke-managed-cluster-autoscaler", "cluster-autoscaler"),
    }
    for provider, (node_provider, provisioner) in providers.items():
        provider_path = ROOT / f"charts/ngkg-workloads/profiles/phase40.13.20-{provider}.yaml"
        provider_profile = yaml.safe_load(provider_path.read_text(encoding="utf-8"))
        if provider_profile["platform"]["kubernetesDistribution"] != provider:
            raise RuntimeError(f"{provider} overlay has the wrong distribution")
        if provider_profile["nodeProvisioning"]["provider"] != node_provider:
            raise RuntimeError(f"{provider} overlay has the wrong node provider")
        if provider_profile["hpcNodeGroups"]["provisioner"] != provisioner:
            raise RuntimeError(f"{provider} overlay has the wrong provisioner")
        subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts/validate_helm_values.py"),
                str(ROOT / "charts/ngkg-workloads/values.yaml"),
                "--overlay",
                str(ROOT / "charts/ngkg-workloads/profiles/phase40.13.20-production.yaml"),
                "--overlay",
                str(provider_path),
            ],
            check=True,
        )
    platform_mounts = platform_values["cloudSourceMounts"]
    expected_drivers = {
        "awsS3": "s3.csi.aws.com",
        "azureBlob": "blob.csi.azure.com",
        "gcs": "gcsfuse.csi.storage.gke.io",
    }
    if not platform_mounts["readOnly"] or not platform_mounts["requireDriverDiscovery"]:
        raise RuntimeError("cloud source mounts are not fail-closed and read-only")
    for provider, driver in expected_drivers.items():
        contract = platform_mounts[provider]
        if not contract["enabled"] or contract["csiDriver"] != driver \
                or contract["identityMode"] != "workload-identity":
            raise RuntimeError(f"{provider} TriG bucket mount contract is incomplete")
    hpa = require(
        "charts/ngkg-workloads/templates/autoscaling.yaml",
        "cpuUtilizationTargetPercent",
        "memoryUtilizationTargetPercent",
    )
    policy = require(
        "charts/ngkg-workloads/templates/autoscaling-qualification.yaml",
        "cpuTargetPercent",
        "memoryTargetPercent",
        "scaleFromZeroPools",
        "nodeProvisioner",
    )
    keda = require(
        "charts/ngkg-workloads/templates/keda-autoscaling.yaml",
        "kind: ScaledObject",
        "type: cpu",
        "type: memory",
        "cpuTargetPercent",
        "memoryTargetPercent",
        "ngkg_hydration_pending",
    )
    live = require(
        "qualification/run_phase40_13_20_live.py",
        "cpu-at-80-scale-out",
        "memory-at-80-scale-out",
        "scale-from-zero",
        "node-loss",
        "checkpoint-replay",
        "scaledResultsDeterministic",
        "selectedCloudSourceCsiDriverAvailable",
    )
    cases = json.loads(
        (ROOT / "test-corpus/autoscaling/phase40.13.20-threshold-cases.json").read_text(encoding="utf-8")
    )["cases"]
    required = {
        "cpu-at-target", "memory-at-target", "one-node-at-target-other-node-idle", "scale-from-zero",
        "checkpoint-blocks-scale-in", "spill-blocks-scale-in", "node-loss-retry",
    }
    if not required <= {case["id"] for case in cases}:
        raise RuntimeError("the 80-percent/failure threshold matrix is incomplete")
    combined = core + runtime + worker + operator + cloud_mounts + hpa + policy + keda + live
    if "align_ontology" in combined or "raw_data_mapping" in combined:
        raise RuntimeError("ontology alignment or raw-data mapping entered autoscaling")
    print("phase 40.13.20 static qualification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"phase 40.13.20 static qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
