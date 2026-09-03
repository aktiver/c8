#!/usr/bin/env python3
"""Execute destructive Phase 6 capacity and chaos qualification on one HA cluster."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Any

from phase6_common import (
    EvidenceRecorder,
    atomic_json,
    canonical,
    epoch_ms,
    load_json,
    require,
    resolve,
    run,
    sha256_file,
    valid_sha256,
)

PROVIDERS = {"rke", "rke2", "eks", "aks", "gke"}
CHAOS_SCENARIOS = {
    "pod_kill",
    "worker_node_loss",
    "network_partition",
    "postgres_failover",
    "object_corruption",
    "duplicate_delivery",
    "checkpoint_recovery",
}


class ClusterQualification:
    def __init__(self, config_path: Path, output: Path) -> None:
        self.config_path = config_path.resolve()
        self.base = self.config_path.parent
        self.config = load_json(self.config_path)
        self.output = output.resolve() / "providers"
        self.provider = self.config.get("provider")
        self.context = self.config.get("kubectlContext")
        self.namespace = self.config.get("namespace")
        self.subject = self.config.get("subjectSha256")
        require(self.config.get("formatVersion") == 1, "unsupported provider configuration")
        require(self.provider in PROVIDERS, "unsupported provider")
        require(isinstance(self.context, str) and self.context, "kubectl context is required")
        require(isinstance(self.namespace, str) and self.namespace, "namespace is required")
        require(valid_sha256(self.subject), "invalid qualification subject")
        require(self.config.get("isolatedQualificationCluster") is True, "chaos requires an isolated qualification cluster")
        require(os.environ.get("NGKG_PHASE6_EXECUTE_LIVE") == "YES", "set NGKG_PHASE6_EXECUTE_LIVE=YES after change approval")
        approval = resolve(self.base, self.config["disruptiveApprovalFile"])
        require(approval.is_file(), "disruptive approval evidence is missing")
        require(sha256_file(approval) == self.config.get("disruptiveApprovalSha256"), "disruptive approval checksum mismatch")
        self.image_lock = resolve(self.base, self.config["imageLockFile"])
        require(self.image_lock.is_file(), "image lock is missing")
        require(sha256_file(self.image_lock) == self.config.get("imageLockSha256"), "image lock checksum mismatch")
        self.provider_output = self.output / self.provider
        self.recorder = EvidenceRecorder(self.provider_output / "scenarios", self.subject)
        self._pod_uids: dict[str, str] = {}
        self._pod_cpu_millis: dict[str, int] = {}
        self._node_uids: set[str] = set()
        self._hpa_uids: dict[str, str] = {}

    def kubectl(self, *args: str, timeout: int = 600) -> bytes:
        return run(["kubectl", "--context", self.context, *args], timeout=timeout)

    def kubectl_json(self, *args: str, timeout: int = 600) -> Any:
        return json.loads(self.kubectl(*args, timeout=timeout))

    def inventory(self) -> dict[str, Any]:
        version = self.kubectl_json("version", "-o", "json")["serverVersion"]["gitVersion"]
        nodes = self.kubectl_json("get", "nodes", "-o", "json")["items"]
        ready = []
        zones: set[str] = set()
        architectures: set[str] = set()
        total_cpu_millis = 0
        total_memory_kib = 0
        for node in nodes:
            conditions = {row["type"]: row["status"] for row in node["status"].get("conditions", [])}
            if conditions.get("Ready") != "True" or node["spec"].get("unschedulable", False):
                continue
            ready.append(node)
            metadata = node["metadata"]
            self._node_uids.add(metadata["uid"])
            labels = metadata.get("labels", {})
            zone = labels.get("topology.kubernetes.io/zone")
            require(isinstance(zone, str) and zone, "ready node lacks topology.kubernetes.io/zone")
            zones.add(zone)
            architectures.add(labels.get("kubernetes.io/arch", "unknown"))
            cpu = str(node["status"]["allocatable"]["cpu"])
            total_cpu_millis += int(cpu[:-1]) if cpu.endswith("m") else int(cpu) * 1000
            memory = str(node["status"]["allocatable"]["memory"])
            require(memory.endswith("Ki"), "node memory must be reported in Ki")
            total_memory_kib += int(memory[:-2])
        require(len(ready) >= 3, "provider qualification requires at least three ready nodes")
        require(len(zones) >= 2, "provider qualification requires at least two zones/failure domains")
        pods = self.kubectl_json("-n", self.namespace, "get", "pods", "-o", "json")["items"]
        selector = self.config.get("workerSelector")
        service_accounts = set(self.config.get("workerServiceAccounts", []))
        require(isinstance(selector, dict) and selector and service_accounts, "worker identity selectors are required")
        for pod in pods:
            metadata = pod["metadata"]
            labels = metadata.get("labels", {})
            if not all(labels.get(key) == value for key, value in selector.items()):
                continue
            require(pod["spec"].get("serviceAccountName") in service_accounts, "worker used an unexpected service account")
            require(bool(metadata.get("ownerReferences")), "worker pod is not bound to an owning workload")
            statuses = pod.get("status", {}).get("containerStatuses", [])
            require(statuses and all("@sha256:" in item.get("imageID", "") for item in statuses), "worker image is not digest observed")
            uid = metadata["uid"]
            node_name = pod["spec"].get("nodeName")
            if node_name:
                node = next((item for item in ready if item["metadata"]["name"] == node_name), None)
                if node:
                    self._pod_uids[uid] = node["metadata"]["uid"]
                    cpu_millis = 0
                    for container in pod["spec"].get("containers", []):
                        cpu = str(container.get("resources", {}).get("requests", {}).get("cpu", "0"))
                        cpu_millis += int(cpu[:-1]) if cpu.endswith("m") else int(cpu) * 1000
                    require(cpu_millis > 0, "worker pod has no CPU request")
                    self._pod_cpu_millis[uid] = cpu_millis
        require(bool(self._pod_uids), "qualification namespace has no scheduled pods")
        return {
            "provider": self.provider,
            "kubernetesVersion": version,
            "readyNodes": len(ready),
            "failureDomains": len(zones),
            "architectures": sorted(architectures),
            "allocatableCpuMillis": total_cpu_millis,
            "allocatableMemoryBytes": total_memory_kib * 1024,
            "nodeUidsSha256": __import__("hashlib").sha256(canonical(sorted(self._node_uids))).hexdigest(),
        }

    def verify_autoscalers(self) -> dict[str, Any]:
        names = self.config.get("hpas")
        require(isinstance(names, list) and names, "HPA inventory is empty")
        observed = []
        for name in names:
            hpa = self.kubectl_json("-n", self.namespace, "get", "hpa", name, "-o", "json")
            targets = {
                metric["resource"]["name"]: metric["resource"]["target"].get("averageUtilization")
                for metric in hpa["spec"].get("metrics", [])
                if metric.get("type") == "Resource"
            }
            require(targets.get("cpu") == 80 and targets.get("memory") == 80, f"{name} must scale at 80% CPU and RAM")
            uid = hpa["metadata"].get("uid")
            require(isinstance(uid, str) and uid, f"{name} has no Kubernetes UID")
            self._hpa_uids[name] = uid
            observed.append({"name": name, "uidSha256": __import__("hashlib").sha256(uid.encode()).hexdigest(), "cpuPercent": 80, "memoryPercent": 80, "currentReplicas": hpa.get("status", {}).get("currentReplicas", 0), "desiredReplicas": hpa.get("status", {}).get("desiredReplicas", 0), "maximumReplicas": hpa["spec"]["maxReplicas"]})
        return {"hpas": observed, "nodeAutoscaler": self.config["nodeAutoscaler"], "scaleFromZeroPools": self.config["scaleFromZeroPools"]}

    def driver(self, section: str, action: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        spec = self.config[section]
        executable = Path(spec["executable"])
        require(executable.is_absolute() and executable.is_file() and os.access(executable, os.X_OK), f"approved {section} executable is unavailable")
        require(valid_sha256(spec.get("sha256")) and sha256_file(executable) == spec["sha256"], f"{section} executable checksum mismatch")
        request = {
            "formatVersion": 1,
            "action": action,
            "provider": self.provider,
            "kubectlContext": self.context,
            "namespace": self.namespace,
            "subjectSha256": self.subject,
            "imageLockSha256": self.config["imageLockSha256"],
        }
        if extra:
            request.update(extra)
        response = json.loads(run([str(executable)], stdin=canonical(request), timeout=int(spec.get("timeoutSeconds", 7200))))
        require(response.get("formatVersion") == 1 and response.get("complete") is True, f"{section}/{action} returned incomplete evidence")
        require(response.get("synthetic") is False and response.get("subjectSha256") == self.subject, f"{section}/{action} evidence subject is invalid")
        require(valid_sha256(response.get("evidenceSha256")), f"{section}/{action} raw evidence digest is invalid")
        return response

    def validate_workers(self, response: dict[str, Any], minimum_nodes: int = 2) -> dict[str, Any]:
        workers = response.get("workers")
        require(isinstance(workers, list) and workers, "driver omitted measured workers")
        node_uids: set[str] = set()
        total_cpu_ns = 0
        peak_rss = 0
        total_cores = 0
        for worker in workers:
            pod_uid = worker.get("podUid")
            node_uid = worker.get("nodeUid")
            require(pod_uid in self._pod_uids, f"driver reported unknown pod UID: {pod_uid}")
            require(node_uid == self._pod_uids[pod_uid] and node_uid in self._node_uids, "worker node identity mismatch")
            cpu_ns = int(worker.get("cpuTimeNs", 0))
            rss = int(worker.get("peakRssBytes", 0))
            cores = int(worker.get("allocatedCores", 0))
            require(cpu_ns > 0 and rss > 0 and cores > 0, "worker resource evidence is not measured")
            expected_cores = max(1, (self._pod_cpu_millis[pod_uid] + 999) // 1000)
            require(cores == expected_cores, "driver allocation differs from Kubernetes pod resources")
            require(worker.get("measurementSource") in {"cgroup", "prometheus", "container-runtime"} and valid_sha256(worker.get("measurementEvidenceSha256")), "worker measurements lack independent source evidence")
            total_cpu_ns += cpu_ns
            peak_rss = max(peak_rss, rss)
            total_cores += cores
            node_uids.add(node_uid)
        require(len(node_uids) >= minimum_nodes, "work did not execute across enough physical Kubernetes nodes")
        return {"workerPods": len(workers), "physicalNodes": len(node_uids), "allocatedCores": total_cores, "measuredCpuTimeNs": total_cpu_ns, "maximumWorkerPeakRssBytes": peak_rss}

    def qualify_capacity(self) -> dict[str, Any]:
        response = self.driver("capacityDriver", "RUN_SATURATION_MATRIX", {"capacityPolicy": self.config["capacityPolicy"]})
        resources = self.validate_workers(response)
        trials = response.get("trials")
        policy = self.config["capacityPolicy"]
        require(isinstance(trials, list) and trials, "capacity driver omitted trials")
        required_points = {(int(n), int(c)) for n in policy["nodeCounts"] for c in policy["concurrencyLevels"]}
        trials_by_point: dict[tuple[int, int], list[dict[str, Any]]] = {}
        trial_keys: set[tuple[int, int, str, int]] = set()
        semantic_hashes: dict[str, set[str]] = {}
        for trial in trials:
            require(trial.get("status") == "PASS" and trial.get("partial") is False, "failed or partial capacity trial")
            require(int(trial.get("durationMs", 0)) > 0 and int(trial.get("requests", 0)) > 0, "capacity trial lacks monotonic measurements")
            require(int(trial.get("errors", -1)) == 0 and int(trial.get("semanticMismatches", -1)) == 0, "capacity trial returned errors or semantic mismatches")
            require(valid_sha256(trial.get("semanticResultSha256")), "capacity trial omitted semantic hash")
            point = (int(trial["nodes"]), int(trial["concurrency"]))
            kind = trial.get("trialKind")
            ordinal = int(trial.get("trialOrdinal", -1))
            require(kind in {"warmup", "measured"} and ordinal >= 0, "capacity trial identity is invalid")
            require((point[0], point[1], kind, ordinal) not in trial_keys, "duplicate capacity trial")
            trial_keys.add((point[0], point[1], kind, ordinal))
            trials_by_point.setdefault(point, []).append(trial)
            semantic_hashes.setdefault(str(trial["caseId"]), set()).add(trial["semanticResultSha256"])
        require(set(trials_by_point) == required_points, "capacity matrix has missing or unexpected points")
        for point, point_trials in trials_by_point.items():
            warmups = [trial for trial in point_trials if trial["trialKind"] == "warmup"]
            measured = [trial for trial in point_trials if trial["trialKind"] == "measured"]
            require(len(warmups) == int(policy["warmupTrials"]), f"wrong warmup cardinality at {point}")
            require(len(measured) == int(policy["measuredTrials"]), f"wrong measured cardinality at {point}")
            require(sum(int(trial["durationMs"]) for trial in measured) >= int(policy["minimumSaturationDurationSeconds"]) * 1000, f"saturation interval too short at {point}")
        require(all(len(hashes) == 1 for hashes in semantic_hashes.values()), "semantic result changed across capacity points")
        events = response.get("autoscalingEvents")
        require(isinstance(events, list) and events, "capacity evidence omitted autoscaling events")
        require(any(event.get("trigger") in {"cpu", "memory"} and int(event.get("observedPercent", 0)) >= 80 for event in events), "80% CPU/RAM scaling was not observed")
        require(all(
            event.get("hpaName") in self._hpa_uids
            and event.get("hpaUid") == self._hpa_uids[event["hpaName"]]
            and int(event.get("observedEpochMs", 0)) > 0
            and int(event.get("replicasAfter", 0)) > int(event.get("replicasBefore", -1))
            and bool(event.get("kubernetesEventUid"))
            for event in events
        ), "autoscaling events lack observed HPA identity/timestamps")
        require(response.get("saturationReached") is True, "saturation boundary was not measured")
        return {"resources": resources, "trialCount": len(trials), "capacityPoints": len(trials_by_point), "autoscalingEventCount": len(events), "saturation": response["saturation"], "rawEvidenceSha256": response["evidenceSha256"]}

    def qualify_chaos(self) -> dict[str, Any]:
        results = []
        for scenario in sorted(CHAOS_SCENARIOS):
            response = self.driver("chaosDriver", "INJECT_AND_RECOVER", {"scenario": scenario})
            resources = self.validate_workers(response, minimum_nodes=1)
            require(response.get("scenario") == scenario and response.get("recovered") is True, f"chaos scenario did not recover: {scenario}")
            require(response.get("partialResponses") == 0, f"chaos scenario exposed partial results: {scenario}")
            require(response.get("preSemanticResultSha256") == response.get("postSemanticResultSha256"), f"semantic identity changed after {scenario}")
            require(valid_sha256(response.get("postSemanticResultSha256")), f"chaos scenario omitted semantic evidence: {scenario}")
            require(int(response.get("recoveryTimeSeconds", 10**9)) <= int(self.config["chaosPolicy"]["maximumRecoveryTimeSeconds"]), f"RTO exceeded: {scenario}")
            require(int(response.get("recoveryPointSeconds", 10**9)) <= int(self.config["chaosPolicy"]["maximumRecoveryPointSeconds"]), f"RPO exceeded: {scenario}")
            results.append({"scenario": scenario, "recoveryTimeSeconds": response["recoveryTimeSeconds"], "recoveryPointSeconds": response["recoveryPointSeconds"], "resources": resources, "evidenceSha256": response["evidenceSha256"]})
        return {"scenarios": results, "failureCount": 0}

    def qualify_provider_integrations(self) -> dict[str, Any]:
        response = self.driver("providerDriver", "VERIFY_IDENTITY_STORAGE_AND_NODE_SCALING")
        require(response.get("workloadIdentity") is True and response.get("longLivedCloudCredentials") is False, "provider workload identity gate failed")
        require(response.get("trigIngestion") is True and response.get("artifactRoundTrip") is True, "provider object-store qualification failed")
        require(response.get("nodeScaleFromZero") is True and response.get("nodeScaleDown") is True, "provider node autoscaling qualification failed")
        require(response.get("highAvailability") is True, "provider HA qualification failed")
        require(response.get("gpuWorkloadObserved") is True and response.get("gpuScaleFromZero") is True and int(response.get("gpuTimeNs", 0)) > 0, "real GPU scale-from-zero qualification failed")
        require(response.get("postCutoverTenantIsolation") is True, "post-cutover tenant isolation qualification failed")
        return {key: response[key] for key in ("workloadIdentity", "longLivedCloudCredentials", "trigIngestion", "artifactRoundTrip", "nodeScaleFromZero", "nodeScaleDown", "highAvailability", "gpuWorkloadObserved", "gpuScaleFromZero", "gpuTimeNs", "postCutoverTenantIsolation", "evidenceSha256")}

    def scenario(self, scenario_id: str, action: Any) -> Any:
        started = epoch_ms()
        attempt = self.recorder.begin(scenario_id, started)
        try:
            detail = action()
        except BaseException as error:
            self.recorder.fail_attempt(attempt, error, epoch_ms())
            raise
        self.recorder.complete(attempt, detail, epoch_ms())
        return detail

    def execute(self) -> None:
        inventory = self.scenario("cluster_inventory", self.inventory)
        autoscaling = self.scenario("autoscaler_configuration", self.verify_autoscalers)
        capacity = self.scenario("capacity_saturation", self.qualify_capacity)
        chaos = self.scenario("chaos_recovery", self.qualify_chaos)
        integrations = self.scenario("provider_integration", self.qualify_provider_integrations)
        evidence = {
            "formatVersion": 1,
            "kind": "Phase6ProviderEvidence",
            "provider": self.provider,
            "subjectSha256": self.subject,
            "imageLockSha256": self.config["imageLockSha256"],
            "inventory": inventory,
            "autoscaling": autoscaling,
            "capacity": capacity,
            "chaos": chaos,
            "providerIntegrations": integrations,
            "scenarios": self.recorder.rows,
            "failureCount": 0,
            "synthetic": False,
            "status": "PASS",
            "complete": True,
        }
        atomic_json(self.provider_output / "provider-evidence.json", evidence)
        print(json.dumps({"provider": self.provider, "status": "PASS", "scenarioCount": len(self.recorder.rows)}, sort_keys=True))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    ClusterQualification(args.config, args.output).execute()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, KeyError, TypeError, ValueError, RuntimeError) as error:
        print(f"Phase 6 provider qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
