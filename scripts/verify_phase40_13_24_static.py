#!/usr/bin/env python3
"""Source and executable-harness checks for Phase 40.13.24."""

from __future__ import annotations

import hashlib
import json
import pathlib
import py_compile
import subprocess
import sys
import tempfile

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
PROVIDERS = ["rke", "rke2", "eks", "aks", "gke"]
GATES = ["semantic-context-graph", "multinode-soak", "compute-chaos", "network-chaos", "storage-chaos", "upgrade", "rollback", "backup-restore", "helm", "image-provenance", "sbom", "cve", "license", "reproducible-build", "provider-portability"]
DISRUPTIVE = {"compute-chaos", "network-chaos", "storage-chaos", "upgrade", "rollback", "backup-restore"}


def require(relative: str, *tokens: str) -> str:
    text = (ROOT / relative).read_text(encoding="utf-8")
    for token in tokens:
        if token not in text:
            raise RuntimeError(f"{relative} is missing {token!r}")
    return text


def run(*args: str, must_fail: bool = False) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, check=False)
    if (result.returncode == 0) == must_fail:
        raise RuntimeError(f"{' '.join(args)} returned {result.returncode}: {result.stdout}{result.stderr}")
    return result


def semantic_trace() -> None:
    trig = require("test-corpus/phase40_13_24/cross-domain-owl2dl.trig", "owl:propertyChainAxiom", "ex:detectedOn", "ex:hostedBy", "ex:ownedBy", "ex:requiresResponseFrom")
    query = require("test-corpus/phase40_13_24/cross-domain-context.rq", "CONSTRUCT", "WHERE", "ex:requiresResponseFrom")
    expected = require("test-corpus/phase40_13_24/cross-domain-context.expected.nt", "requiresResponseFrom", "incident-7", "team-blue")
    descriptor = json.loads((ROOT / "test-corpus/phase40_13_24/cross-domain-context.json").read_text(encoding="utf-8"))
    if len(descriptor["authorizedGraphs"]) != 4 or not all(value.endswith("/semkg") for value in descriptor["authorizedGraphs"]) or descriptor["hopCount"] != 3 or descriptor["resultType"] != "unified-reasoned-context-graph":
        raise RuntimeError("cross-domain semantic corpus is not a four-domain, three-hop graph result")
    if trig.count("<https://c8-next-generation.io/acme/") < 4 or query.count("?") < 8 or len([line for line in expected.splitlines() if line.strip()]) != 4:
        raise RuntimeError("cross-domain semantic corpus shape is incomplete")
    require("crates/ngkg-dataset/src/lib.rs", "Resolve the active dataset with SPARQL Protocol precedence and graph authorization", "authorized_graph_set_sha256", "default_graph_ids")
    require("crates/ngkg-offline-reasoner/src/lib.rs", "Stable logical partition count", "partition completion barrier is incomplete", "support_id", "arbitrary_owl2_dl_complete: false")
    require("crates/ngkg-direct-reasoner/src/lib.rs", "partition", "completion")
    require("services/online-serving/src/main.rs", "execute_distributed_query", "exact_distributed_owl2_direct_then_scalar_algebra", "distributed algebra replica set is incomplete or scalar-unequal", "serialize_sparql_graph(&certified.graph_ntriples")
    require("crates/ngkg-reference/src/query.rs", "QueryResults::Graph", "canonicalize_graph")
    run(sys.executable, "scripts/validate_phase40_13_24_semantic_corpus.py", "--trig", "test-corpus/phase40_13_24/cross-domain-owl2dl.trig", "--expected", "test-corpus/phase40_13_24/cross-domain-context.expected.nt")


def executable_barrier() -> None:
    with tempfile.TemporaryDirectory(prefix="phase40-13-24-") as raw:
        temp = pathlib.Path(raw)
        approval = temp / "approval.json"
        approval.write_text('{"approved":true,"isolatedQualificationCluster":true}\n', encoding="utf-8")
        approval_sha = hashlib.sha256(approval.read_bytes()).hexdigest()
        semantic = {
            "owl2DlQualificationSha256": "1" * 64, "snapshotSha256": "2" * 64,
            "authorizedGraphSetSha256": "3" * 64, "querySha256": "4" * 64,
            "resultGraphSha256": "5" * 64, "scalarOracleGraphSha256": "5" * 64,
            "reasoningCertificateSha256": "6" * 64, "domainCount": 4, "hopCount": 3,
            "reasonedOutputTriples": 1, "activatedNodes": 3, "activatedCpuMillis": 12000,
            "activatedMemoryBytes": 32 * 1024**3, "queryForm": "CONSTRUCT", "complete": True,
            "proofCoverage": "complete",
        }
        performance = {"formatVersion": 1, "phase": "40.13.23", "failedThresholdCount": 0, "complete": True}
        (temp / "semantic.json").write_text(json.dumps(semantic), encoding="utf-8")
        (temp / "performance.json").write_text(json.dumps(performance), encoding="utf-8")
        scenarios = []
        for provider in PROVIDERS:
            for gate in GATES:
                disruptive = gate in DISRUPTIVE
                scenarios.append({
                    "scenarioId": f"{provider}-{gate}", "provider": provider, "gate": gate,
                    "expectedOutputSha256": "a" * 64, "minimumNodes": 3,
                    "minimumCpuMillis": 12000, "minimumMemoryBytes": 48 * 1024**3,
                    "minimumDurationSeconds": 259200 if gate == "multinode-soak" else 0,
                    "disruptive": disruptive,
                    "approvalEvidenceSha256": approval_sha if disruptive else None,
                    "descriptor": {"syntheticHarnessOnly": True, "provider": provider, "gate": gate},
                })
        definitions = {"formatVersion": 1, "runId": "synthetic-harness-only", "scenarios": scenarios}
        (temp / "definitions.json").write_text(json.dumps(definitions), encoding="utf-8")
        plan, catalog = temp / "plan.json", temp / "catalog.json"
        run(sys.executable, "scripts/build_phase40_13_24_plan.py", "--inventory", "release/phase40.13.24/qualification-inventory.yaml", "--definitions", str(temp / "definitions.json"), "--performance-certificate", str(temp / "performance.json"), "--semantic-evidence", str(temp / "semantic.json"), "--release-sha256", "7" * 64, "--partition-count", "4", "--plan-output", str(plan), "--catalog-output", str(catalog))
        fake = temp / "driver.py"
        fake.write_text(
            "import hashlib,json,sys\n"
            "r=json.load(open(sys.argv[-1],encoding='utf-8')); s=r['scenario']; d=s['disruptive']\n"
            "o={'scenarioId':s['scenarioId'],'provider':s['provider'],'gate':s['gate'],'outputSha256':s['expectedOutputSha256'],'evidenceSha256':hashlib.sha256(s['scenarioId'].encode()).hexdigest(),'activatedNodes':s['minimumNodes'],'activatedCpuMillis':s['minimumCpuMillis'],'activatedMemoryBytes':s['minimumMemoryBytes'],'durationSeconds':s['minimumDurationSeconds'],'injectedFailures':1 if d else 0,'recoveredFailures':1 if d else 0,'postRecoveryResultSha256':s['expectedOutputSha256'],'complete':True}\n"
            "print(json.dumps(o,separators=(',',':')))\n",
            encoding="utf-8",
        )
        reports = temp / "reports"; reports.mkdir()
        for partition in range(4):
            run(sys.executable, "scripts/run_phase40_13_24_partition.py", "--plan", str(plan), "--catalog", str(catalog), "--partition", str(partition), "--worker-id", f"worker-{partition}", "--driver", f"{sys.executable} {fake}", "--allow-disruptive", "--approval-evidence", str(approval), "--output", str(reports / f"partition-{partition}.json"))
        certificate = temp / "certificate.json"
        merge = (sys.executable, "scripts/merge_phase40_13_24_reports.py", "--plan", str(plan), "--reports", str(reports), "--output", str(certificate))
        run(*merge)
        value = json.loads(certificate.read_text(encoding="utf-8"))
        if not value["complete"] or value["failureCount"] != 0 or len(value["qualifiedProviders"]) != 5 or len(value["qualifiedGates"]) != 15:
            raise RuntimeError("synthetic release barrier did not cover the complete matrix")
        corrupt = json.loads((reports / "partition-0.json").read_text(encoding="utf-8"))
        if corrupt["observations"]:
            corrupt["observations"][0]["complete"] = False
        else:
            corrupt["workerId"] = "worker-1"
        (reports / "partition-0.json").write_text(json.dumps(corrupt), encoding="utf-8")
        run(*merge, must_fail=True)


def main() -> int:
    semantic_trace()
    rust = require("crates/ngkg-release-qualification/src/lib.rs", "SemanticContextEvidence", "validate_semantic_context_evidence", "stable_partition", "certify_release", "minimum_nodes < 3", "failure_count: 0", "Apache Jena")
    require("Cargo.toml", '"crates/ngkg-release-qualification"')
    inventory = yaml.safe_load((ROOT / "release/phase40.13.24/qualification-inventory.yaml").read_text(encoding="utf-8"))["spec"]
    if set(inventory["kubernetes"]["providers"]) != set(PROVIDERS) or set(inventory["requiredGates"]) != set(GATES):
        raise RuntimeError("release inventory coverage is incomplete")
    if inventory["kubernetes"]["autoscalingCpuPercent"] != 80 or inventory["kubernetes"]["autoscalingMemoryPercent"] != 80:
        raise RuntimeError("80-percent CPU-or-memory autoscaling prerequisite changed")
    if inventory["productionRuntime"]["implementation"] != "rust" or inventory["productionRuntime"]["apacheJenaRuntimeDependency"] is not False:
        raise RuntimeError("production runtime is not explicitly Rust-only")
    for schema in ("semantic-context-evidence.schema.json", "release-qualification-plan.schema.json", "release-partition-report.schema.json", "release-qualification-certificate.schema.json"):
        if json.loads((ROOT / "contracts" / schema).read_text(encoding="utf-8")).get("additionalProperties") is not False:
            raise RuntimeError(f"{schema} is not closed")
    parallel = require("deploy/release-qualification/indexed-hpc-job.yaml.tpl", "completionMode: Indexed", "parallelism: ${NGKG_RELEASE_PARALLELISM}", "requiredDuringSchedulingIgnoredDuringExecution", "DoNotSchedule", "NGKG_RELEASE_WORKER_THREADS", "cpu: '8'", "maxFailedIndexes: 0")
    serial = require("deploy/release-qualification/serial-disruption-job.yaml.tpl", "parallelism: 1", "--allow-disruptive", "--approval-evidence", "maxFailedIndexes: 0")
    if "align_ontology" in rust + parallel + serial or "raw_data_mapping" in rust + parallel + serial:
        raise RuntimeError("ontology alignment or raw-data mapping entered Phase 40.13.24")
    for script in ("scripts/validate_phase40_13_24_semantic_corpus.py", "scripts/build_phase40_13_24_plan.py", "scripts/run_phase40_13_24_partition.py", "scripts/merge_phase40_13_24_reports.py"):
        py_compile.compile(str(ROOT / script), doraise=True)
    executable_barrier()
    print("phase 40.13.24 semantic prerequisite and Kubernetes release harness passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"phase 40.13.24 qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
