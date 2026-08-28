#!/usr/bin/env python3
"""Executable source qualification for Phase 40.13.23."""

from __future__ import annotations

import copy
import json
import pathlib
import py_compile
import subprocess
import sys
import tempfile

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(relative: str, *tokens: str) -> str:
    value = (ROOT / relative).read_text(encoding="utf-8")
    for token in tokens:
        if token not in value:
            raise RuntimeError(f"{relative} is missing {token!r}")
    return value


def run(*arguments: str, must_fail: bool = False) -> None:
    process = subprocess.run(arguments, cwd=ROOT, text=True, capture_output=True, check=False)
    if (process.returncode == 0) == must_fail:
        raise RuntimeError(f"{' '.join(arguments)} returned {process.returncode}: {process.stdout}{process.stderr}")


def dataset() -> dict[str, object]:
    return {
        "sha256": "1" * 64, "snapshotSha256": "2" * 64, "bytes": 100 * 1024**3,
        "namedGraphs": 30, "triples": 100_000_000, "propertyPathEdges": 100_000_000,
        "owl2DlQualified": True, "pinnedImports": True, "provenance": True,
    }


def executable_barrier() -> None:
    with tempfile.TemporaryDirectory(prefix="phase40-13-23-") as raw:
        temp = pathlib.Path(raw)
        inventory = yaml.safe_load((ROOT / "benchmarks/phase40.13.23/qualification-inventory.yaml").read_text(encoding="utf-8"))
        spec = inventory["spec"]
        spec["trialPolicy"]["minimumWarmupTrialsPerEngine"] = 0
        spec["trialPolicy"]["minimumMeasuredTrialsPerEngine"] = 3
        spec["representativeDatasetMinimums"]["distinctDatasets"] = 1
        spec["capacityPoints"]["minimumNodeCounts"] = [1]
        spec["capacityPoints"]["concurrencyLevels"] = [250]
        inventory_path = temp / "inventory.yaml"
        inventory_path.write_text(yaml.safe_dump(inventory, sort_keys=True), encoding="utf-8")
        autoscaling = {"formatVersion": 1, "phase": "40.13.20", "targetPercent": 80, "complete": True}
        hardware = {"formatVersion": 1, "collectedEpochSeconds": 1, "kubernetesProvider": "rke2", "region": "test", "nodeType": "test", "architecture": "amd64", "cpuModel": "test", "physicalCoresPerNode": 4, "numaNodesPerNode": 1, "memoryBytesPerNode": 16 * 1024**3, "networkBitsPerSecond": 1_000_000_000, "storageClass": "test", "kernelVersion": "test", "cgroupVersion": 2, "containerRuntime": "containerd", "complete": True}
        pricing = {"formatVersion": 1, "observedEpochSeconds": 1, "provider": "rke2", "region": "test", "currency": "USD", "nodeMicroUsdPerHour": 1, "objectReadMicroUsdPerMillion": 0, "objectWriteMicroUsdPerMillion": 0, "objectStorageMicroUsdPerGibMonth": 0, "egressMicroUsdPerGib": 0, "sourceUrlSha256": "e" * 64, "complete": True}
        for name, value in (("autoscaling.json", autoscaling), ("hardware.json", hardware), ("pricing.json", pricing)):
            (temp / name).write_text(json.dumps(value), encoding="utf-8")
        families = ["trig-ingestion", "semantic-compilation", "offline-reasoning", "property-path", "sparql-query", "concurrent-sparql", "recovery"]
        scenarios = []
        for index, family in enumerate(families):
            scenarios.append({
                "scenarioId": f"scenario-{index}-{family}", "family": family,
                "expectedResultSha256": "a" * 64, "capacityGroup": family, "scaleOrdinal": 0,
                "cacheState": "hot", "concurrency": 250 if family == "concurrent-sparql" else 1,
                "requestedNodes": 1, "requestedCpuMillis": 4000, "requestedMemoryBytes": 16 * 1024**3,
                "warmupTrials": 0, "measuredTrials": 3,
                "requireExternalJena": family in {"property-path", "sparql-query", "concurrent-sparql"},
                "maximumP95Nanoseconds": 2_000_000, "minimumThroughputPerSecond": 2_000_000 if family == "property-path" else 1,
                "minimumSpeedupMilliX": 20_000 if family in {"property-path", "sparql-query", "concurrent-sparql"} else 0,
                "maximumCostMicroUsdPerMillion": 1_000_000,
                "descriptor": {"dataset": copy.deepcopy(dataset()), "operation": family},
            })
        definitions = {"formatVersion": 1, "runId": "synthetic-barrier-only", "scenarios": scenarios}
        definitions_path = temp / "definitions.json"
        definitions_path.write_text(json.dumps(definitions), encoding="utf-8")
        fake = temp / "driver.py"
        fake.write_text(
            "import json,sys\n"
            "engine,version,path=sys.argv[1:]\n"
            "r=json.load(open(path,encoding='utf-8')); e=r['resourceEnvelope']; family=r['family']\n"
            "duration=100000000 if engine=='external-apache-jena' else 1000000\n"
            "work=3000 if family=='property-path' else (100*1024**3 if family=='trig-ingestion' else 1000)\n"
            "o={'formatVersion':1,'engine':engine,'engineVersion':version,'scenarioId':r['scenarioId'],'trialPhase':r['trialPhase'],'trial':r['trial'],'durationNanoseconds':duration,'operations':r['concurrency'],'workItems':work,'inputBytes':100*1024**3,'outputItems':10,'cpuTimeNanoseconds':duration,'peakRssBytes':1024,'bytesRead':100,'bytesWritten':10,'nodesActivated':e['nodes'],'cpuMillisActivated':e['cpuMillis'],'ramBytesActivated':e['memoryBytes'],'resultSha256':'a'*64,'artifactRootSha256':'b'*64 if engine=='ngkg-rust' else None,'autoscalingEvidenceSha256':r['autoscalingEvidenceSha256'],'costMicroUsd':1,'complete':True,'errorClass':None}\n"
            "print(json.dumps(o,separators=(',',':')))\n",
            encoding="utf-8",
        )
        plan, catalog = temp / "plan.json", temp / "catalog.json"
        run(sys.executable, "scripts/build_phase40_13_23_plan.py", "--inventory", str(inventory_path), "--definitions", str(definitions_path), "--hardware", str(temp / "hardware.json"), "--pricing", str(temp / "pricing.json"), "--autoscaling-evidence", str(temp / "autoscaling.json"), "--ngkg-image-sha256", "c" * 64, "--external-jena-image-sha256", "d" * 64, "--partition-count", "2", "--plan-output", str(plan), "--catalog-output", str(catalog))
        reports = temp / "reports"
        reports.mkdir()
        ngkg = f"{sys.executable} {fake} ngkg-rust 1.0.0"
        jena = f"{sys.executable} {fake} external-apache-jena 6.2.0"
        for partition in range(2):
            run(sys.executable, "scripts/run_phase40_13_23_partition.py", "--plan", str(plan), "--inventory", str(inventory_path), "--catalog", str(catalog), "--pricing", str(temp / "pricing.json"), "--output", str(reports / f"partition-{partition}.json"), "--partition", str(partition), "--worker-id", f"worker-{partition}", "--ngkg-driver", ngkg, "--external-jena-driver", jena)
        certificate = temp / "certificate.json"
        merge = (sys.executable, "scripts/merge_phase40_13_23_reports.py", "--plan", str(plan), "--inventory", str(inventory_path), "--reports", str(reports), "--output", str(certificate))
        run(*merge)
        result = json.loads(certificate.read_text(encoding="utf-8"))
        if result["failedThresholdCount"] != 0 or not result["complete"] or len(result["qualifiedFamilies"]) != 7:
            raise RuntimeError("synthetic performance certificate is invalid")
        populated = next(path for path in reports.glob("partition-*.json") if json.loads(path.read_text(encoding="utf-8"))["observations"])
        corrupted = json.loads(populated.read_text(encoding="utf-8"))
        corrupted["observations"][0]["complete"] = False
        populated.write_text(json.dumps(corrupted), encoding="utf-8")
        run(*merge, must_fail=True)


def main() -> int:
    rust = require(
        "crates/ngkg-performance-qualification/src/lib.rs",
        "NgkgRust", "ExternalApacheJena", "stable_partition", "certify_performance",
        "trials are missing, duplicated, or excluded", "failed_threshold_count: 0",
        "Apache Jena may provide", "never linked into an",
    )
    require(
        "crates/ngkg-performance-qualification/src/bin/ngkg-rust-performance-driver.rs",
        'engine: "ngkg-rust"', "x-ngkg-query-execution-id", "/v1/query_logs/",
        "cgroup_cpu_nanoseconds", "cgroup_memory_peak_bytes", "Policy::none()",
        "trial_cost", "concurrent calls returned unequal semantic or artifact identities",
    )
    require("Cargo.toml", '"crates/ngkg-performance-qualification"')
    inventory = yaml.safe_load((ROOT / "benchmarks/phase40.13.23/qualification-inventory.yaml").read_text(encoding="utf-8"))
    spec = inventory["spec"]
    if spec["ngkgRuntime"] != {"implementation": "rust", "version": "1.0.0", "javaLinkedOrEmbedded": False}:
        raise RuntimeError("NGKG runtime is not explicitly Rust-only")
    if spec["externalBaselines"]["apacheJena"]["runtimeDependency"] is not False or spec["externalBaselines"]["apacheJena"]["isolation"] != "external-process-only":
        raise RuntimeError("Apache Jena is not isolated from the product runtime")
    if spec["capacityPoints"]["autoscalingCpuPercent"] != 80 or spec["capacityPoints"]["autoscalingMemoryPercent"] != 80:
        raise RuntimeError("capacity qualification does not preserve the 80-percent scale boundary")
    if spec["representativeDatasetMinimums"]["prohibitSyntheticOnlyQualification"] is not True:
        raise RuntimeError("synthetic-only release qualification is permitted")
    for schema in ("performance-plan.schema.json", "performance-partition-report.schema.json", "performance-qualification-certificate.schema.json", "performance-hardware-evidence.schema.json", "performance-pricing-evidence.schema.json"):
        value = json.loads((ROOT / "contracts" / schema).read_text(encoding="utf-8"))
        if value.get("additionalProperties") is not False:
            raise RuntimeError(f"{schema} is not closed")
    job = require("deploy/performance-qualification/indexed-job.yaml.tpl", "completionMode: Indexed", "parallelism: 1", "maxFailedIndexes: 0", "ngkg-rust-performance-driver", "external-jena-client")
    product = "\n".join((ROOT / path).read_text(encoding="utf-8") for path in ("Cargo.toml", "charts/ngkg-platform/values.yaml", "charts/ngkg-workloads/values.yaml"))
    if "org.apache.jena" in product or "apache-jena-libs" in product or "jena-tdb" in product.lower():
        raise RuntimeError("Apache Jena entered the NGKG Rust runtime or production charts")
    for script in ("scripts/build_phase40_13_23_plan.py", "scripts/run_phase40_13_23_partition.py", "scripts/merge_phase40_13_23_reports.py"):
        py_compile.compile(str(ROOT / script), doraise=True)
    executable_barrier()
    if "align_ontology" in rust + job or "raw_data_mapping" in rust + job:
        raise RuntimeError("ontology alignment or raw-data mapping entered Phase 40.13.23")
    print("phase 40.13.23 static and executable performance barrier passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"phase 40.13.23 qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
