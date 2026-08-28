#!/usr/bin/env python3
"""Executable source qualification for Phase 40.13.22."""

from __future__ import annotations

import hashlib
import json
import pathlib
import py_compile
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(relative: str, *tokens: str) -> str:
    value = (ROOT / relative).read_text(encoding="utf-8")
    for token in tokens:
        if token not in value:
            raise RuntimeError(f"{relative} is missing {token!r}")
    return value


def run(*arguments: str) -> None:
    process = subprocess.run(arguments, cwd=ROOT, text=True, capture_output=True, check=False)
    if process.returncode:
        raise RuntimeError(f"{' '.join(arguments)} failed: {process.stdout}{process.stderr}")


def run_must_fail(*arguments: str) -> None:
    process = subprocess.run(arguments, cwd=ROOT, text=True, capture_output=True, check=False)
    if process.returncode == 0:
        raise RuntimeError(f"{' '.join(arguments)} unexpectedly accepted corrupted evidence")


def executable_barrier_test() -> None:
    with tempfile.TemporaryDirectory(prefix="phase40-13-22-") as raw:
        temporary = pathlib.Path(raw)
        fake = temporary / "fake_driver.py"
        fake.write_text(
            "import json,sys\n"
            "engine=sys.argv[1]; version=sys.argv[2]; request=json.load(open(sys.argv[3],encoding='utf-8'))\n"
            "failed=request['descriptor'].get('fail',False)\n"
            "value={'formatVersion':1,'engine':engine,'engineVersion':version,'caseId':request['caseId'],'outcome':'failure' if failed else 'success','resultSha256':None if failed else 'a'*64,'errorClass':'MALFORMED_QUERY' if failed else None,'complete':True}\n"
            "print(json.dumps(value,separators=(',',':')))\n",
            encoding="utf-8",
        )
        definitions = {
            "formatVersion": 1,
            "cases": [
                {"caseId": "case-a", "family": "sparql-evaluation", "oracle": "apache-jena", "expectedOutcome": "success", "descriptor": {"value": 1}},
                {"caseId": "case-b", "family": "owl-direct", "oracle": "hermit", "expectedOutcome": "success", "descriptor": {"value": 2}},
                {"caseId": "case-c", "family": "failure", "oracle": "w3c-expected", "expectedOutcome": "failure", "expectedErrorClass": "MALFORMED_QUERY", "descriptor": {"fail": True}},
            ],
        }
        definitions_path = temporary / "definitions.json"
        definitions_path.write_text(json.dumps(definitions), encoding="utf-8")
        plan, catalog = temporary / "plan.json", temporary / "catalog.json"
        run(sys.executable, "scripts/build_phase40_13_22_plan.py", "--suite-inventory", "conformance/phase40.13.22-suite.json", "--definitions", str(definitions_path), "--partition-count", "2", "--plan-output", str(plan), "--catalog-output", str(catalog))
        reports = temporary / "reports"
        reports.mkdir()
        versions = {"ngkg": "1.0.0", "w3c-expected": "8af71fed933539d09d5f4658fb1ea7ba4c8e30b9", "apache-jena": "6.2.0", "hermit": "1.4.5.519"}
        commands = {name: f"{sys.executable} {fake} {name} {versions[name]}" for name in versions}
        for partition in range(2):
            run(sys.executable, "scripts/run_phase40_13_22_partition.py", "--plan", str(plan), "--suite-inventory", "conformance/phase40.13.22-suite.json", "--case-catalog", str(catalog), "--output", str(reports / f"partition-{partition}.json"), "--partition", str(partition), "--worker-id", f"worker-{partition}", "--ngkg-driver", commands["ngkg"], "--w3c-driver", commands["w3c-expected"], "--jena-driver", commands["apache-jena"], "--hermit-driver", commands["hermit"])
        certificate = temporary / "certificate.json"
        run(sys.executable, "scripts/merge_phase40_13_22_reports.py", "--plan", str(plan), "--reports", str(reports), "--output", str(certificate))
        result = json.loads(certificate.read_text(encoding="utf-8"))
        if result["caseCount"] != 3 or result["mismatchCount"] != 0 or not result["complete"]:
            raise RuntimeError("executable all-partitions certificate is invalid")
        populated = next(path for path in sorted(reports.glob("partition-*.json")) if json.loads(path.read_text(encoding="utf-8"))["observations"])
        corrupt = json.loads(populated.read_text(encoding="utf-8"))
        corrupt["observations"][0]["complete"] = False
        populated.write_text(json.dumps(corrupt), encoding="utf-8")
        run_must_fail(sys.executable, "scripts/merge_phase40_13_22_reports.py", "--plan", str(plan), "--reports", str(reports), "--output", str(certificate))


def main() -> int:
    rust = require(
        "crates/ngkg-standards-qualification/src/lib.rs",
        "stable_partition", "certify_standards", "one complete report per dense partition",
        "successful NGKG and oracle canonical results differ", "negative-case failure classes differ",
        "mismatch_count: 0", "missing_case_count: 0",
    )
    require("Cargo.toml", '"crates/ngkg-standards-qualification"')
    suite = json.loads((ROOT / "conformance/phase40.13.22-suite.json").read_text(encoding="utf-8"))
    required_families = {"trig-syntax", "trig-evaluation", "sparql-syntax", "sparql-evaluation", "result-format", "sparql-protocol", "service-description", "federation", "owl-direct", "failure"}
    if set(suite["requiredFamilies"]) != required_families:
        raise RuntimeError("suite inventory does not close every required standards family")
    if suite["oracles"]["apacheJena"]["version"] != "6.2.0" or suite["oracles"]["hermit"]["version"] != "1.4.5.519":
        raise RuntimeError("oracle versions differ from their pins")
    if not all(value is False for key, value in suite["releasePolicy"].items() if key.startswith("allow")):
        raise RuntimeError("release policy permits incomplete or mismatched evidence")
    lock = json.loads((ROOT / "conformance/w3c-rdf-tests.lock.json").read_text(encoding="utf-8"))
    if suite["w3c"]["commit"] != lock["commit"] or suite["w3c"]["requiredManifests"] != lock["requiredManifests"]:
        raise RuntimeError("Phase 40.13.22 does not bind the existing W3C lock exactly")
    for schema in ("standards-qualification-plan.schema.json", "standards-partition-report.schema.json", "standards-qualification-certificate.schema.json"):
        parsed = json.loads((ROOT / "contracts" / schema).read_text(encoding="utf-8"))
        if parsed.get("additionalProperties") is not False:
            raise RuntimeError(f"{schema} is not closed")
    jena = require("adapters/jena-differential/pom.xml", "<version>6.2.0</version>", "maven.compiler.release")
    ET.fromstring(jena)
    require("adapters/jena-differential/src/main/java/io/ngkg/jena/Main.java", 'ENGINE = "apache-jena"', 'VERSION = "6.2.0"', "query.hasOrderBy()", "Collections.sort(rows)", "MALFORMED_QUERY")
    job = yaml.safe_load((ROOT / "deploy/standards-qualification/indexed-job.yaml").read_text(encoding="utf-8"))
    spec = job["spec"]
    if spec["completionMode"] != "Indexed" or spec["maxFailedIndexes"] != 0 or spec["parallelism"] > spec["completions"]:
        raise RuntimeError("distributed qualification Job is not a fail-closed dense Indexed Job")
    container = spec["template"]["spec"]["containers"][0]
    if container["resources"]["requests"] != container["resources"]["limits"]:
        raise RuntimeError("qualification CPU/RAM envelope is not deterministic")
    for script in ("scripts/build_phase40_13_22_plan.py", "scripts/run_phase40_13_22_partition.py", "scripts/merge_phase40_13_22_reports.py"):
        py_compile.compile(str(ROOT / script), doraise=True)
    executable_barrier_test()
    combined = rust + jena
    if "align_ontology" in combined or "raw_data_mapping" in combined:
        raise RuntimeError("ontology alignment or raw-data mapping entered Phase 40.13.22")
    print("phase 40.13.22 static and executable barrier qualification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"phase 40.13.22 qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
