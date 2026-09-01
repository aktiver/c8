#!/usr/bin/env python3
"""Run one real, isolated HA Kubernetes provider qualification."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import threading
import time
from typing import Any, Callable
import urllib.error
import urllib.request


REQUIRED_PROVIDERS = {"rke2", "eks", "aks", "gke"}


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha_file(path: Path) -> str:
    return sha_bytes(path.read_bytes())


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def run(command: list[str], *, stdin: bytes | None = None, timeout: int = 300) -> bytes:
    result = subprocess.run(command, input=stdin, capture_output=True, timeout=timeout, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"command failed ({command[0]}): {result.stderr.decode(errors='replace')[:2000]}")
    return result.stdout


def resolve_path(base: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else (base / path).resolve()


def json_path(value: Any, path: str) -> Any:
    current = value
    for part in path.split("."):
        if isinstance(current, list):
            current = current[int(part)]
        else:
            current = current[part]
    return current


class Qualification:
    def __init__(self, config_path: Path, image_lock: Path, deployment_evidence: Path, approval: Path, output: Path) -> None:
        self.config_path = config_path.resolve()
        self.config_dir = self.config_path.parent
        self.config = json.loads(self.config_path.read_text(encoding="utf-8"))
        self.image_lock_path = image_lock.resolve()
        self.image_lock = json.loads(self.image_lock_path.read_text(encoding="utf-8"))
        self.deployment_evidence_path = deployment_evidence.resolve()
        self.deployment_evidence = json.loads(self.deployment_evidence_path.read_text(encoding="utf-8"))
        self.approval = approval.resolve()
        self.output = output.resolve()
        self.provider = self.config["provider"]
        self.context = self.config["kubectlContext"]
        self.namespace = self.config["namespace"]
        self.scenarios: list[dict[str, Any]] = []
        self.sessions: dict[str, str] = {}
        self.details_dir = self.output.parent / f"{self.provider}-details"
        self.details_dir.mkdir(parents=True, exist_ok=True)

    def kubectl(self, *args: str, timeout: int = 300) -> bytes:
        return run(["kubectl", "--context", self.context, *args], timeout=timeout)

    def validate_configuration(self) -> None:
        require(self.config.get("formatVersion") == 1, "unsupported cluster configuration")
        require(self.provider in REQUIRED_PROVIDERS, "unsupported provider")
        require(self.config.get("qualificationCluster") is True, "destructive qualification requires an isolated qualification cluster")
        require(sha_file(self.approval) == self.config.get("approvalEvidenceSha256"), "disruptive approval checksum mismatch")
        require(self.image_lock.get("formatVersion") == 1 and len(self.image_lock.get("images", [])) == 12, "image lock is incomplete")
        require(self.deployment_evidence.get("complete") is True and self.deployment_evidence.get("provider") == self.provider, "deployment evidence is incomplete")
        require(self.deployment_evidence.get("imageLockSha256") == sha_file(self.image_lock_path), "deployment used a different image lock")
        toolchain_path = self.deployment_evidence_path.with_name(f"{self.deployment_evidence_path.stem}-toolchain.json")
        require(toolchain_path.is_file() and self.deployment_evidence.get("toolchainEvidenceSha256") == sha_file(toolchain_path), "deployment toolchain evidence mismatch")
        for token_path in self.config.get("tokenFiles", {}).values():
            path = Path(token_path)
            require(path.is_file() and path.stat().st_mode & 0o077 == 0, f"token file is missing or too permissive: {path}")
        required = json.loads((Path(__file__).resolve().parents[1] / "config/required-scenarios.json").read_text())
        probes = set(self.config.get("probes", {}))
        require({"api_openapi_health","sparql_owl2dl_cross_domain","mcp_initialize_tools_query","hermit_exact_fallback","tenant_dataset_isolation","tenant_mcp_memory_tool_isolation"} <= probes, "HTTP qualification probes are incomplete")
        require(set(required["providers"]) == REQUIRED_PROVIDERS, "provider policy changed unexpectedly")

    def cluster_inventory(self) -> dict[str, Any]:
        namespace = json.loads(self.kubectl("get", "namespace", "kube-system", "-o", "json"))
        version = json.loads(self.kubectl("version", "-o", "json"))["serverVersion"]["gitVersion"]
        nodes = json.loads(self.kubectl("get", "nodes", "-o", "json"))["items"]
        ready = []
        zones = set()
        gpu_nodes = 0
        for node in nodes:
            conditions = {item["type"]: item["status"] for item in node["status"].get("conditions", [])}
            if conditions.get("Ready") != "True" or node["spec"].get("unschedulable", False):
                continue
            ready.append(node)
            labels = node["metadata"].get("labels", {})
            zone = labels.get("topology.kubernetes.io/zone")
            require(bool(zone), f"ready node lacks topology zone: {node['metadata']['name']}")
            zones.add(zone)
            if int(node["status"].get("allocatable", {}).get("nvidia.com/gpu", "0")) > 0:
                gpu_nodes += 1
        require(len(ready) >= 3, "qualification cluster has fewer than three ready schedulable nodes")
        require(len(zones) >= 3, "qualification cluster has fewer than three availability zones/failure domains")
        require(gpu_nodes >= 1, "qualification cluster has no ready GPU node")
        return {
            "clusterUid": namespace["metadata"]["uid"],
            "kubernetesVersion": version,
            "readyNodes": len(ready),
            "zones": len(zones),
            "gpuNodes": gpu_nodes,
        }

    def verify_running_images(self) -> None:
        allowed = {f"{item['repository']}@{item['digest']}" for item in self.image_lock["images"]}
        pods = json.loads(self.kubectl("-n", self.namespace, "get", "pods", "-l", self.config["workloadPodSelector"], "-o", "json"))["items"]
        require(bool(pods), "qualification namespace has no pods")
        for pod in pods:
            for status in pod["status"].get("containerStatuses", []):
                image_id = status.get("imageID", "").removeprefix("docker-pullable://")
                if any(image_id.startswith(reference) for reference in allowed):
                    continue
                declared = next((item for item in pod["spec"]["containers"] if item["name"] == status["name"]), None)
                if declared and declared["image"] in allowed:
                    continue
                raise RuntimeError(f"pod uses image outside the Phase 3 lock: {pod['metadata']['name']}/{status['name']}")

    def ready_node_count(self) -> int:
        nodes = json.loads(self.kubectl("get", "nodes", "-o", "json"))["items"]
        count = 0
        for node in nodes:
            conditions = {item["type"]: item["status"] for item in node["status"].get("conditions", [])}
            if conditions.get("Ready") == "True" and not node["spec"].get("unschedulable", False):
                count += 1
        return count

    def token(self, name: str) -> str:
        return Path(self.config["tokenFiles"][name]).read_text(encoding="utf-8").strip()

    def probe(self, spec: dict[str, Any]) -> dict[str, Any]:
        url = spec["url"]
        require(url.startswith("https://"), "qualification HTTP probes require TLS")
        headers = {"Accept": spec.get("accept", "application/json")}
        if "token" in spec:
            headers["Authorization"] = f"Bearer {self.token(spec['token'])}"
        if "session" in spec and spec["session"] in self.sessions:
            headers["Mcp-Session-Id"] = self.sessions[spec["session"]]
        body = None
        if "bodyFile" in spec:
            body_path = resolve_path(self.config_dir, spec["bodyFile"])
            require(body_path.is_file(), f"probe body does not exist: {body_path}")
            body = body_path.read_bytes()
            headers["Content-Type"] = spec.get("contentType", "application/json")
        request = urllib.request.Request(url, data=body, headers=headers, method=spec.get("method", "GET"))
        try:
            with urllib.request.urlopen(request, timeout=spec.get("timeoutSeconds", 300)) as response:
                status = response.status
                payload = response.read(spec.get("maximumResponseBytes", 16 * 1024 * 1024) + 1)
                response_headers = dict(response.headers.items())
        except urllib.error.HTTPError as error:
            status = error.code
            payload = error.read(spec.get("maximumResponseBytes", 16 * 1024 * 1024) + 1)
            response_headers = dict(error.headers.items())
        require(len(payload) <= spec.get("maximumResponseBytes", 16 * 1024 * 1024), "probe response exceeded its byte ceiling")
        require(status in spec["expectedStatus"], f"unexpected HTTP status {status} for {url}")
        if "captureHeader" in spec:
            lower_headers = {key.lower(): value for key, value in response_headers.items()}
            captured = lower_headers.get(spec["captureHeader"].lower())
            if spec.get("captureHeaderRequired", False):
                require(bool(captured), f"required response header is absent: {spec['captureHeader']}")
            if captured:
                self.sessions[spec["session"]] = captured
        text = payload.decode("utf-8", errors="strict") if payload else ""
        parsed = None
        content_type = response_headers.get("Content-Type", response_headers.get("content-type", ""))
        if payload and ("json" in content_type or spec.get("jsonEquals") or spec.get("expectedBodySha256")):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = None
        semantic_payload = canonical(parsed) if parsed is not None else payload
        body_sha256 = sha_bytes(semantic_payload)
        for needle in spec.get("contains", []):
            require(needle in text, f"required response marker is absent: {needle}")
        if "expectedBodySha256" in spec:
            expected = spec["expectedBodySha256"]
            require(len(expected) == 64 and body_sha256 == expected, "response checksum mismatch")
        if spec.get("jsonEquals"):
            require(parsed is not None, "JSON assertions require a JSON response")
            for path, expected in spec["jsonEquals"].items():
                require(json_path(parsed, path) == expected, f"JSON assertion failed: {path}")
        return {"method": spec.get("method", "GET"), "urlSha256": sha_bytes(url.encode()), "status": status, "bodySha256": body_sha256, "responseHeadersSha256": sha_bytes(canonical(sorted(response_headers.items())))}

    def record(self, scenario_id: str, operation: Callable[[], dict[str, Any]]) -> None:
        started = int(time.time() * 1000)
        detail = operation()
        ended = int(time.time() * 1000)
        self.record_completed(scenario_id, detail, started, ended)

    def record_completed(self, scenario_id: str, detail: dict[str, Any], started: int, ended: int) -> None:
        path = self.details_dir / f"{scenario_id}.json"
        path.write_bytes(canonical({"scenarioId": scenario_id, "detail": detail, "startedEpochMs": started, "endedEpochMs": ended, "complete": True}) + b"\n")
        self.scenarios.append({"id": scenario_id, "startedEpochMs": started, "endedEpochMs": ended, "evidenceSha256": sha_file(path), "complete": True})

    def probe_group(self, scenario_id: str) -> dict[str, Any]:
        return {"probes": [self.probe(spec) for spec in self.config["probes"][scenario_id]]}

    def autoscaling(self) -> tuple[dict[str, Any], dict[str, Any]]:
        config = self.config["autoscaling"]
        for name in config["hpas"]:
            hpa = json.loads(self.kubectl("-n", self.namespace, "get", "hpa", name, "-o", "json"))
            targets = {metric["resource"]["name"]: metric["resource"]["target"].get("averageUtilization") for metric in hpa["spec"]["metrics"] if metric["type"] == "Resource"}
            require(targets.get("cpu") == 80 and targets.get("memory") == 80, f"HPA does not use 80% CPU and memory: {name}")
        deployment = config["deployment"]
        before_nodes = self.ready_node_count()
        before_replicas = int(json.loads(self.kubectl("-n", self.namespace, "get", "deployment", deployment, "-o", "json"))["status"].get("readyReplicas", 0))
        manifest = resolve_path(self.config_dir, config["loadManifest"])
        require(manifest.is_file(), "autoscaling load manifest is unavailable")
        self.kubectl("-n", self.namespace, "apply", "-f", str(manifest))
        deadline = time.monotonic() + config["timeoutSeconds"]
        observed_replicas = before_replicas
        observed_nodes = before_nodes
        try:
            while time.monotonic() < deadline:
                deployment_json = json.loads(self.kubectl("-n", self.namespace, "get", "deployment", deployment, "-o", "json"))
                observed_replicas = max(observed_replicas, int(deployment_json["status"].get("readyReplicas", 0)))
                observed_nodes = max(observed_nodes, self.ready_node_count())
                if observed_replicas >= before_replicas + config["minimumReplicaIncrease"] and observed_nodes >= before_nodes + config["minimumNodeIncrease"]:
                    break
                time.sleep(10)
            require(observed_replicas >= before_replicas + config["minimumReplicaIncrease"], "HPA did not scale the deployment")
            require(observed_nodes >= before_nodes + config["minimumNodeIncrease"], "node autoscaler did not add capacity")
        finally:
            self.kubectl("-n", self.namespace, "delete", "-f", str(manifest), "--ignore-not-found=true")
        return ({"beforeReplicas": before_replicas, "maximumReadyReplicas": observed_replicas, "cpuTargetPercent": 80, "memoryTargetPercent": 80}, {"beforeNodes": before_nodes, "maximumNodes": observed_nodes})

    def external_driver(self, section: str, action: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        config = self.config[section]
        driver = Path(config["driver"])
        require(driver.is_absolute() and driver.is_file() and os.access(driver, os.X_OK), f"approved {section} driver is unavailable")
        request = {"formatVersion":1,"action":action,"provider":self.provider,"kubectlContext":self.context,"namespace":self.namespace,"clusterUid":self.inventory["clusterUid"],"imageLockSha256":sha_file(self.image_lock_path)}
        if extra:
            request.update(extra)
        result = json.loads(run([str(driver)], stdin=canonical(request), timeout=config.get("timeoutSeconds", 1800)))
        require(result.get("complete") is True and result.get("provider") == self.provider and result.get("clusterUid") == self.inventory["clusterUid"], f"{section} driver returned invalid identity/evidence")
        return result

    def node_loss(self) -> dict[str, Any]:
        config = self.config["nodeLoss"]
        before = self.probe(config["beforeProbe"])
        pod = json.loads(self.kubectl("-n", self.namespace, "get", "pods", "-l", config["podSelector"], "-o", "json"))["items"][0]
        node = pod["spec"]["nodeName"]
        result = self.external_driver("nodeLoss", "terminate-node", {"nodeName": node, "preFailureResultSha256": before["bodySha256"]})
        after = self.probe(config["beforeProbe"])
        require(after["bodySha256"] == before["bodySha256"] == result.get("postRecoveryResultSha256"), "node loss changed the semantic result")
        require(result.get("replacementNodeUid") != result.get("terminatedNodeUid"), "node loss driver did not replace the node")
        return {"before": before, "after": after, "driver": result}

    def recovery(self) -> tuple[dict[str, Any], dict[str, Any]]:
        result = self.external_driver("recovery", "checksum-backup-restore")
        require(result.get("checksumFailureRejected") is True and result.get("backupVerified") is True and result.get("restoreVerified") is True, "storage recovery invariants failed")
        require(result.get("preFailureResultSha256") == result.get("postRestoreResultSha256"), "restore changed semantic results")
        checksum = {key: result[key] for key in ("checksumFailureRejected","corruptObjectSha256","rejectedOperationId")}
        restore = {key: result[key] for key in ("backupVerified","restoreVerified","backupManifestSha256","preFailureResultSha256","postRestoreResultSha256")}
        return checksum, restore

    def gpu(self) -> tuple[dict[str, Any], dict[str, Any]]:
        config = self.config["gpu"]
        scaled = json.loads(self.kubectl("-n", self.namespace, "get", "scaledobject", config["scaledObject"], "-o", "json"))
        require(int(scaled["spec"].get("minReplicaCount", -1)) == 0, "vLLM ScaledObject does not permit scale from zero")
        self.kubectl("-n", self.namespace, "scale", "deployment", config["deployment"], "--replicas=0")
        scale_down_deadline = time.monotonic() + config["timeoutSeconds"]
        while time.monotonic() < scale_down_deadline:
            deployment = json.loads(self.kubectl("-n", self.namespace, "get", "deployment", config["deployment"], "-o", "json"))
            if int(deployment["status"].get("readyReplicas", 0)) == 0:
                break
            time.sleep(5)
        else:
            raise RuntimeError("vLLM did not reach zero ready replicas")
        driver = Path(config["requestDriver"])
        require(driver.is_absolute() and driver.is_file() and os.access(driver, os.X_OK), "approved inference request driver is unavailable")
        request_result: dict[str, Any] = {}
        request_error: list[BaseException] = []
        def invoke() -> None:
            try:
                request_result.update(json.loads(run([str(driver)], timeout=config["timeoutSeconds"])))
            except BaseException as error:  # transferred to the main thread
                request_error.append(error)
        thread = threading.Thread(target=invoke, daemon=True)
        thread.start()
        deadline = time.monotonic() + config["timeoutSeconds"]
        scheduled_node = None
        while time.monotonic() < deadline:
            pods = json.loads(self.kubectl("-n", self.namespace, "get", "pods", "-l", config["podSelector"], "-o", "json"))["items"]
            running = [pod for pod in pods if pod["status"].get("phase") == "Running" and pod["spec"].get("nodeName")]
            if running:
                scheduled_node = running[0]["spec"]["nodeName"]
                break
            time.sleep(10)
        thread.join(config["timeoutSeconds"])
        require(not request_error and request_result.get("complete") is True, "GPU inference request failed")
        require(bool(scheduled_node), "vLLM did not scale from zero onto a node")
        node = json.loads(self.kubectl("get", "node", scheduled_node, "-o", "json"))
        require(int(node["status"]["allocatable"].get("nvidia.com/gpu", "0")) > 0, "vLLM scheduled outside a GPU pool")
        drain = self.probe(config["drainProbe"])
        return ({"scheduledNodeUid": node["metadata"]["uid"], "requestResultSha256": sha_bytes(canonical(request_result)), "scaledFromZero": True}, {"drainProbe": drain, "drained": True})

    def execute(self) -> None:
        self.validate_configuration()
        self.inventory = self.cluster_inventory()
        require(self.deployment_evidence.get("clusterUid") == self.inventory["clusterUid"], "deployment evidence belongs to another cluster")
        self.verify_running_images()
        for scenario in ("api_openapi_health","sparql_owl2dl_cross_domain","mcp_initialize_tools_query","hermit_exact_fallback"):
            self.record(scenario, lambda scenario=scenario: self.probe_group(scenario))
        started = int(time.time() * 1000)
        hpa, nodes = self.autoscaling()
        ended = int(time.time() * 1000)
        self.record_completed("cpu_memory_hpa_80_percent", hpa, started, ended)
        self.record_completed("node_autoscaler_scale_up", nodes, started, ended)
        self.record("node_loss_query_recovery", self.node_loss)
        checksum, restore = self.recovery()
        self.record("storage_checksum_backup_restore", lambda: {"checksum":checksum,"restore":restore})
        started = int(time.time() * 1000)
        scale_zero, drain = self.gpu()
        ended = int(time.time() * 1000)
        self.record_completed("gpu_vllm_scale_from_zero", scale_zero, started, ended)
        self.record_completed("gpu_inference_drain", drain, started, ended)
        for scenario in ("tenant_dataset_isolation","tenant_mcp_memory_tool_isolation"):
            self.record(scenario, lambda scenario=scenario: self.probe_group(scenario))
        required = json.loads((Path(__file__).resolve().parents[1] / "config/required-scenarios.json").read_text())["scenarios"]
        require({item["id"] for item in self.scenarios} == set(required), "scenario execution set is incomplete")
        report = {"formatVersion":1,"provider":self.provider,**self.inventory,"imageLockSha256":sha_file(self.image_lock_path),"deploymentEvidenceSha256":sha_file(self.deployment_evidence_path),"scenarios":sorted(self.scenarios,key=lambda item:item["id"]),"complete":True}
        self.output.parent.mkdir(parents=True, exist_ok=True)
        self.output.write_bytes(canonical(report) + b"\n")
        print(json.dumps({"provider":self.provider,"scenarios":len(self.scenarios),"complete":True}, sort_keys=True))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--image-lock", type=Path, required=True)
    parser.add_argument("--deployment-evidence", type=Path, required=True)
    parser.add_argument("--approval-evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    Qualification(args.config, args.image_lock, args.deployment_evidence, args.approval_evidence, args.output).execute()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
