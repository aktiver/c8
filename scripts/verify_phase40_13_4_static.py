#!/usr/bin/env python3
"""Static contract verification for Phase 40.13.4."""
from __future__ import annotations

import json
import pathlib
import sys

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(path: str, *tokens: str) -> str:
    target = ROOT / path
    if not target.is_file():
        raise RuntimeError(f"missing {path}")
    content = target.read_text(encoding="utf-8")
    for token in tokens:
        if token not in content:
            raise RuntimeError(f"{path} missing {token}")
    return content


def main() -> int:
    require(
        "crates/ngkg-reference/src/query.rs",
        "solution_results_equivalent",
        "canonical_solution_dataset",
        "comparison_term",
        "max_graph_blank_nodes",
    )
    require(
        "crates/ngkg-sparql-compiler/src/lib.rs",
        "normalize_token_separators",
        "textual_variable_order",
        "solution_variable_order",
    )
    require(
        "crates/ngkg-reference/src/bin/ngkg-w3c-case.rs",
        "csv_records_equivalent",
        "parse_csv_records",
        "with_base_iri",
    )
    require(
        "scripts/run_w3c_conformance.py",
        "assumed_test_base",
        "action_path.as_uri()",
        "ThreadPoolExecutor",
        "available_cpus",
    )
    require(
        "charts/ngkg-workloads/templates/autoscaling.yaml",
        "ngkg_admission_pending",
    )

    backlog = json.loads(require("conformance/sparql11-known-gaps-phase40.13.4.json"))
    baseline = backlog["baseline"]
    if baseline != {"total": 338, "pass": 326, "fail": 12}:
        raise RuntimeError("Phase 40.13.4 baseline counts changed without qualification")
    if sum(int(gap["count"]) for gap in backlog["gaps"]) != baseline["fail"]:
        raise RuntimeError("known-gap counts do not reconcile with the failing baseline")

    matrix = json.loads(require("conformance/sparql11-feature-matrix.json"))
    by_id = {feature["id"]: feature for feature in matrix["features"]}
    for identifier in [
        "pattern.minus",
        "pattern.graph",
        "pattern.values",
        "path.property",
        "solution.aggregate",
        "results.formats",
    ]:
        if by_id[identifier]["layers"]["reference"] not in {"partial", "implemented"}:
            raise RuntimeError(
                f"{identifier} must be partial in Phase 40.13.4 or implemented by a later gate"
            )
    if matrix["claim"] != "inventory":
        raise RuntimeError("SPARQL matrix cannot claim qualification with red cases")

    gates = yaml.safe_load(require("acceptance/phase-gates.yaml"))["phases"]
    if not any(str(gate.get("phase")) == "40.13.4" for gate in gates):
        raise RuntimeError("acceptance registry lacks Phase 40.13.4")
    print("Phase 40.13.4 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, RuntimeError, TypeError, ValueError) as error:
        print(f"Phase 40.13.4 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
