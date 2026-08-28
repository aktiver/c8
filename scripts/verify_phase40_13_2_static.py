#!/usr/bin/env python3
"""Static recovery checks for Phase 40.13.2."""

from __future__ import annotations

import json
import pathlib


ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(path: str, tokens: tuple[str, ...] = ()) -> str:
    target = ROOT / path
    if not target.is_file():
        raise RuntimeError(f"missing required file: {path}")
    text = target.read_text(encoding="utf-8")
    for token in tokens:
        if token not in text:
            raise RuntimeError(f"{path} is missing required token: {token}")
    return text


def main() -> int:
    require(
        "crates/ngkg-catalog/src/lib.rs",
        ("get_active_serving_snapshot_owned", "self,"),
    )
    online = require(
        "services/online-serving/src/main.rs",
        (
            "self: Arc<Self>",
            "get_active_serving_snapshot_owned",
            "payload_load: Arc<Mutex<()>>",
            "lock_owned().await",
            "fragments.into_iter().enumerate()",
            "/sparql/direct/validate",
        ),
    )
    require(
        "crates/ngkg-query-executor/src/lib.rs",
        (
            '"literal" if !(datatype.is_some() && language.is_some()) => 3',
            "permits_qualifier && datatype.is_some() && language.is_some()",
        ),
    )
    require(
        "crates/ngkg-reference/src/query.rs",
        (
            "standardize_named_graph_blank_nodes",
            "scoped_blank_node",
            "union_default_uses_rdf_set_union_not_bag_concatenation",
            "source graph, which would incorrectly turn duplicate cross-graph triples",
        ),
    )

    status = json.loads(require("verification/phase-40.13.2.json"))
    if status["status"] != "native-workspace-repaired-candidate":
        raise RuntimeError("Phase 40.13.2 status is not fail-closed")
    if status["fullWorkspaceNativeTestsPassed"] is not True:
        raise RuntimeError("full workspace native tests are not recorded as passing")
    if status["sharedHpcRuntimeConsumedByOnlineServing"] is not True:
        raise RuntimeError("online serving does not record the shared HPC runtime")
    if status["workloadAwareHpaContractValidated"] is not True:
        raise RuntimeError("workload-aware HPA validation is absent")
    if status["ontologyAlignmentImplemented"] is not False:
        raise RuntimeError("ontology alignment is outside the database")
    if status["productionQualified"] is not False:
        raise RuntimeError("Phase 40.13.2 must not claim production qualification")

    print(
        "Phase 40.13.2 static verification passed: online futures are owned and Send-capable, payload loading is single-flight without cache-lock I/O, RDF union/blank-node semantics are repaired, and HPC/HPA contracts remain fail-closed"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
