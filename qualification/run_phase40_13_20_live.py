#!/usr/bin/env python3
"""Read-only live-cluster evidence collector for Phase 40.13.20.

The harness never deletes, drains, scales, or mutates cluster resources. A
separate approved chaos run supplies checksum-bound event and determinism logs;
this process verifies live HPA/Kueue/metrics/node-provisioner state and binds
those inputs into one deterministic report.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import subprocess
import sys
from typing import Any

TARGET = 80
SOURCE_DRIVERS = {
    "aws-s3": "s3.csi.aws.com",
    "azure-blob": "blob.csi.azure.com",
    "gcs": "gcsfuse.csi.storage.gke.io",
}
REQUIRED_POOLS = {
    "source-ingestion",
    "semantic-projection",
    "semantic-artifact-build",
    "index-build",
    "reasoning",
    "online-reasoning",
    "sparql-query-processing",
    "sparql-fragment-processing",
    "parquet-hydration",
    "storage-recovery",
}


def kubectl_json(*args: str) -> Any:
    process = subprocess.run(
        ["kubectl", *args, "-o", "json"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=60,
    )
    if process.returncode:
        raise RuntimeError(f"kubectl {' '.join(args)} failed: {process.stderr.strip()}")
    return json.loads(process.stdout)


def kubectl_raw(path: str) -> Any:
    process = subprocess.run(
        ["kubectl", "get", "--raw", path],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=60,
    )
    if process.returncode:
        raise RuntimeError(f"kubectl raw {path} failed: {process.stderr.strip()}")
    return json.loads(process.stdout)


def digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def load(path: pathlib.Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def hpa_targets(document: dict[str, Any]) -> list[dict[str, Any]]:
    output = []
    for item in document.get("items", []):
        name = item.get("metadata", {}).get("name", "")
        if not name.startswith("ngkg-"):
            continue
        resource_targets = {
            metric.get("resource", {}).get("name"): metric.get("resource", {})
            .get("target", {})
            .get("averageUtilization")
            for metric in item.get("spec", {}).get("metrics", [])
            if metric.get("type") == "Resource"
        }
        output.append(
            {
                "namespace": item.get("metadata", {}).get("namespace"),
                "name": name,
                "cpu": resource_targets.get("cpu"),
                "memory": resource_targets.get("memory"),
                "currentReplicas": item.get("status", {}).get("currentReplicas", 0),
                "desiredReplicas": item.get("status", {}).get("desiredReplicas", 0),
            }
        )
    return output


def node_provisioner_state(provider: str) -> dict[str, Any]:
    if provider in {"rke", "rke2", "generic"}:
        deployment = kubectl_json("get", "deployment", "cluster-autoscaler", "-n", "kube-system")
        return {
            "kind": "Deployment",
            "name": "cluster-autoscaler",
            "available": deployment.get("status", {}).get("availableReplicas", 0) > 0,
        }
    if provider == "eks":
        pools = kubectl_json("get", "nodepools.karpenter.sh")
        return {"kind": "NodePool", "name": "karpenter", "available": bool(pools.get("items"))}
    # AKS and GKE node autoscaling is a managed control-plane feature. Its
    # qualification evidence is a provider-backed node plus a scale event.
    nodes = kubectl_json("get", "nodes")
    prefix = "azure://" if provider == "aks" else "gce://"
    provider_nodes = [
        item for item in nodes.get("items", [])
        if item.get("spec", {}).get("providerID", "").startswith(prefix)
    ]
    return {"kind": "ManagedNodePool", "name": provider, "available": bool(provider_nodes)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-log", required=True, type=pathlib.Path)
    parser.add_argument("--determinism", required=True, type=pathlib.Path)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    parser.add_argument("--provider", required=True, choices=("generic", "rke", "rke2", "eks", "aks", "gke"))
    parser.add_argument("--source-provider", required=True, choices=tuple(SOURCE_DRIVERS))
    args = parser.parse_args()

    events = load(args.event_log)
    determinism = load(args.determinism)
    hpas = hpa_targets(kubectl_json("get", "hpa", "--all-namespaces"))
    nodes = kubectl_json("get", "nodes")
    local_queues = kubectl_json("get", "localqueues.kueue.x-k8s.io", "--all-namespaces")
    cluster_queues = kubectl_json("get", "clusterqueues.kueue.x-k8s.io")
    scaled_objects = kubectl_json("get", "scaledobjects.keda.sh", "--all-namespaces")
    provisioner = node_provisioner_state(args.provider)
    csi_drivers = kubectl_json("get", "csidrivers.storage.k8s.io")
    node_metrics = kubectl_raw("/apis/metrics.k8s.io/v1beta1/nodes")
    custom_metrics = kubectl_raw("/apis/custom.metrics.k8s.io/v1beta1")

    observed_pools = {
        item.get("metadata", {}).get("labels", {}).get("ngkg.io/workload")
        for item in nodes.get("items", [])
    }
    observed_pools.discard(None)
    event_pools = {item.get("pool") for item in events.get("events", [])}
    event_pools.discard(None)
    wrong_hpa = [item for item in hpas if item["cpu"] != TARGET or item["memory"] != TARGET]
    keda_hydration = [
        item for item in scaled_objects.get("items", [])
        if item.get("metadata", {}).get("name") == "ngkg-hydration"
    ]
    keda_targets = {
        trigger.get("type"): trigger.get("metadata", {}).get("value")
        for item in keda_hydration
        for trigger in item.get("spec", {}).get("triggers", [])
    }
    required_events = {
        "cpu-at-80-scale-out",
        "memory-at-80-scale-out",
        "scale-from-zero",
        "kueue-admitted",
        "node-provisioned",
        "node-loss",
        "checkpoint-replay",
        "scale-down-after-drain",
    }
    event_names = {item.get("event") for item in events.get("events", [])}
    deterministic = all(
        item.get("baselineResultSha256") == item.get("scaledResultSha256")
        and item.get("baselineArtifactRootSha256") == item.get("scaledArtifactRootSha256")
        and item.get("nodeLossInjected") is True
        and item.get("retryInjected") is True
        for item in determinism.get("workloads", [])
    ) and bool(determinism.get("workloads"))
    registered_csi_drivers = {
        item.get("metadata", {}).get("name") for item in csi_drivers.get("items", [])
    }
    checks = {
        "hpaCpuAndMemoryTargetsExactly80": bool(hpas) and not wrong_hpa,
        "metricsServerAvailable": bool(node_metrics.get("items")),
        "customMetricsApiAvailable": "resources" in custom_metrics,
        "kueueLocalQueueAvailable": bool(local_queues.get("items")),
        "kueueClusterQueueAvailable": bool(cluster_queues.get("items")),
        "kedaCpuAndMemoryTargetsExactly80": bool(keda_hydration)
        and keda_targets.get("cpu") == str(TARGET)
        and keda_targets.get("memory") == str(TARGET),
        "nodeProvisionerAvailable": provisioner["available"],
        "selectedCloudSourceCsiDriverAvailable": SOURCE_DRIVERS[args.source_provider]
        in registered_csi_drivers,
        "responsibilityPoolsObserved": REQUIRED_POOLS <= (observed_pools | event_pools),
        "requiredScaleAndFailureEventsObserved": required_events <= event_names,
        "scaledResultsDeterministic": deterministic,
    }
    complete = all(checks.values())
    report = {
        "formatVersion": 1,
        "phase": "40.13.20",
        "targetPercent": TARGET,
        "kubernetesProvider": args.provider,
        "sourceProvider": args.source_provider,
        "sourceCsiDriver": SOURCE_DRIVERS[args.source_provider],
        "nodeProvisioner": provisioner,
        "checks": checks,
        "hpaTargets": hpas,
        "kedaTargets": keda_targets,
        "observedPools": sorted(observed_pools),
        "eventPools": sorted(event_pools),
        "eventLogSha256": digest(events),
        "determinismSha256": digest(determinism),
        "complete": complete,
    }
    report["reportSha256"] = digest(report)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if complete else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"Phase 40.13.20 live qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
