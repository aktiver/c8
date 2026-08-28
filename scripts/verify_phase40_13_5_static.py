#!/usr/bin/env python3
"""Static contract verification for Phase 40.13.5."""
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
        "Cargo.toml",
        '[patch.crates-io]',
        'oxigraph = { path = "vendor/oxigraph" }',
        'spareval = { path = "vendor/spareval" }',
        'sparopt = { path = "vendor/sparopt" }',
    )
    require(
        "vendor/spareval/src/eval.rs",
        "SPARQL 1.1 defines GROUP_CONCAT as a simple literal",
        "are_compatible_and_not_disjointed_outside_input",
        "Correlated input bindings are substitutions",
        "correlations",
    )
    require("vendor/spareval/src/expression.rs", "build_bnode")
    require(
        "vendor/sparopt/src/algebra.rs",
        "correlations: Vec<(Variable, Variable)>",
        "AlGraphPattern::Graph",
    )
    require(
        "vendor/oxigraph/src/storage/numeric_encoder.rs",
        "Storage is lossless: RDF term identity includes datatype and lexical form",
        "http://www.w3.org/2001/XMLSchema#negativeInteger",
    )
    require(
        "vendor/oxigraph/src/sparql/dataset.rs",
        "self.internalize_term(term.into())",
    )
    require(
        "crates/ngkg-reference/src/query.rs",
        "phase40_13_5_group_concat_is_a_simple_literal",
        "phase40_13_5_graph_scope_reaches_subqueries_and_minus",
        "phase40_13_5_bnode_string_is_solution_scoped",
        "phase40_13_5_zero_length_path_includes_an_absent_constant_node",
        "phase40_13_5_store_preserves_numeric_lexical_identity_and_derived_datatype",
    )
    require(
        "charts/ngkg-workloads/templates/autoscaling.yaml",
        "ngkg_admission_pending",
    )
    require(
        "crates/ngkg-hpc-runtime/src/lib.rs",
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "cpuset",
    )
    backlog = json.loads(require("conformance/sparql11-known-gaps-phase40.13.5.json"))
    if backlog["baseline"] != {"total": 338, "pass": 338, "fail": 0}:
        raise RuntimeError("Phase 40.13.5 baseline must be exactly 338/0")
    if backlog["gaps"]:
        raise RuntimeError("the executable query/result gap list must be empty")
    if backlog["separateUnsupportedSuites"] != {
        "entailmentRegime": 70,
        "protocolAndServiceDescription": 37,
    }:
        raise RuntimeError("separate unsupported suites must remain explicit")

    matrix = json.loads(require("conformance/sparql11-feature-matrix.json"))
    by_id = {feature["id"]: feature for feature in matrix["features"]}
    for identifier in [
        "query.select",
        "pattern.minus",
        "pattern.graph",
        "pattern.values",
        "path.property",
        "expression.functions",
        "solution.aggregate",
        "results.formats",
    ]:
        if by_id[identifier]["layers"]["reference"] != "implemented":
            raise RuntimeError(f"{identifier} scalar layer must be implemented")
    if matrix["claim"] != "inventory":
        raise RuntimeError("distributed, entailment, and protocol gates remain open")

    gates = yaml.safe_load(require("acceptance/phase-gates.yaml"))["phases"]
    if not any(str(gate.get("phase")) == "40.13.5" for gate in gates):
        raise RuntimeError("acceptance registry lacks Phase 40.13.5")
    vendor_readme = require("vendor/README.md", "does not implement ontology alignment")
    if "Exact OWL Direct-Semantics" not in vendor_readme:
        raise RuntimeError("scalar and exact-reasoner contracts are not separated")
    print("Phase 40.13.5 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, RuntimeError, TypeError, ValueError) as error:
        print(f"Phase 40.13.5 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
