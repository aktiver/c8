#!/usr/bin/env python3
"""Static contract verification for Phase 40.13.3."""
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
        "scripts/run_w3c_conformance.py",
        "TestTrigEval",
        "ThreadPoolExecutor",
        "cpu.max",
        "OMP_NUM_THREADS",
        "case-timeout-seconds",
        "max-driver-output-bytes",
        "inventory-only",
        "atomic_write_json",
    )
    require(
        "crates/ngkg-reference/src/bin/ngkg-w3c-case.rs",
        "trig-evaluation",
        "CanonicalizationAlgorithm::Unstable",
        "with_base_iri",
    )
    require(
        "scripts/qualify_phase39_2.sh",
        "cargo build --locked",
        "target/debug/ngkg-w3c-case",
        "NGKG_W3C_JOBS",
    )
    require(
        "charts/ngkg-workloads/templates/autoscaling.yaml",
        "ngkg_admission_pending",
    )
    matrix = json.loads(
        require(
            "conformance/sparql11-feature-matrix.json",
            '"claim": "inventory"',
            "Ontology alignment is not database functionality",
        )
    )
    if not any(
        feature["layers"]["distributed"] in {"partial", "not-implemented"}
        for feature in matrix["features"]
    ):
        raise RuntimeError("feature matrix falsely represents complete distributed support")
    gates = yaml.safe_load(require("acceptance/phase-gates.yaml"))["phases"]
    if not any(str(gate.get("phase")) == "40.13.3" for gate in gates):
        raise RuntimeError("acceptance registry lacks Phase 40.13.3")
    print("Phase 40.13.3 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, RuntimeError, TypeError, ValueError) as error:
        print(f"Phase 40.13.3 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
