#!/usr/bin/env python3
"""Static and contract acceptance for Phase 8 HPC optimization."""

from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def main() -> None:
    catalog = json.loads((ROOT / "docker_repos/images.json").read_text(encoding="utf-8"))
    names = {row["name"] for row in catalog["images"]}
    require(len(names) == 13 and "ngkg-hpc-worker" in names, "Phase 8 requires the exact 13-image catalog")
    require((ROOT / "docker_repos/build_all_local.sh").stat().st_mode & 0o111 != 0, "Linux all-image builder is not executable")
    for row in catalog["images"]:
        require((ROOT / "docker_repos" / row["dockerfile"]).is_file(), f"missing Dockerfile for {row['name']}")

    for contract in [
        "hpc-run-plan.schema.json", "hpc-rank-receipt.schema.json", "hpc-run-certificate.schema.json"
    ]:
        document = json.loads((ROOT / "NGKG_1_0_0_GA/contracts" / contract).read_text(encoding="utf-8"))
        require(document.get("additionalProperties") is False, f"{contract} must fail closed on unknown fields")

    hpc_execution = (ROOT / "NGKG_1_0_0_GA/crates/ngkg-native-runtime/src/hpc.rs").read_text(encoding="utf-8")
    require("ordinal % rank_count == rank" in hpc_execution, "stable rank assignment is absent")
    require("hard_link(&temporary, path)" in hpc_execution, "receipts are not published with no-overwrite semantics")
    require("authorized_graph_set_sha256" in hpc_execution, "HPC plan is not graph-authorization-bound")
    require("by_rank.keys().copied().ne(0..plan.rank_count)" in hpc_execution, "dense MPI rank barrier is absent")

    native_runtime = (ROOT / "NGKG_1_0_0_GA/crates/ngkg-native-runtime/src/lib.rs").read_text(encoding="utf-8")
    require("ProjectionMask::leaves" in native_runtime, "Parquet projection pushdown is absent")
    require("with_row_groups(row_groups)" in native_runtime, "Parquet row-group pruning is absent")
    require("statistics_exclude_set" in native_runtime, "authorized graph-set statistics pruning is absent")
    require("physically_scanned_rows" in native_runtime and "pruned_row_groups" in native_runtime,
            "Parquet pruning evidence is incomplete")

    hpc_runtime = (ROOT / "NGKG_1_0_0_GA/crates/ngkg-hpc-runtime/src/lib.rs").read_text(encoding="utf-8")
    require("resource_envelope_report" in hpc_runtime and "read_memory_controller" in hpc_runtime,
            "cgroup CPU/RAM admission is absent")
    require("requested_compute_threads" in hpc_runtime and "reserved_memory_bytes" in hpc_runtime,
            "bounded local parallel plan is absent")
    require("NGKG_MPI_RANK" in hpc_runtime and "InvalidMpiEnvironment" in hpc_runtime,
            "MPI rank identity does not fail closed")

    mpi = (ROOT / "hpc/native/ngkg_mpi_exec.c").read_text(encoding="utf-8")
    require("MPI_Allreduce" in mpi and "MPI_Barrier" in mpi and "MPI_Comm_split_type" in mpi, "MPI collectives or local-rank binding are absent")
    openmp = (ROOT / "hpc/native/ngkg_openmp_filter.c").read_text(encoding="utf-8")
    require("#pragma omp parallel for schedule(static)" in openmp, "deterministic OpenMP loop is absent")
    require("omp_set_dynamic(0)" in openmp and "omp_set_max_active_levels(1)" in openmp, "nested/dynamic OpenMP is not disabled")

    openmp_boundary = (ROOT / "NGKG_1_0_0_GA/crates/ngkg-native-runtime/src/openmp.rs").read_text(encoding="utf-8")
    require("NGKG_OPENMP_FILTER_EXECUTABLE" in openmp_boundary
            and "expected_output_bytes" in openmp_boundary
            and "NGKG_OPENMP_KERNEL_TIMEOUT_MS" in openmp_boundary
            and "child.kill()" in openmp_boundary,
            "bounded OpenMP subprocess boundary is absent")

    values = yaml.safe_load((ROOT / "NGKG_1_0_0_GA/charts/ngkg-platform/values.yaml").read_text(encoding="utf-8"))
    values_schema = json.loads((ROOT / "NGKG_1_0_0_GA/charts/ngkg-platform/values.schema.json").read_text(encoding="utf-8"))
    require(values["hpc"]["enabled"] is False, "MPI must remain opt-in before live qualification")
    require(values["hpc"]["rankCount"] >= 2, "MPI gang must contain multiple ranks")
    require(
        values["images"]["hpcWorker"]
        == {"repository": "", "digest": "", "pullPolicy": "IfNotPresent"},
        "HPC image must be digest-injected with an explicit pull policy",
    )
    artifact_pattern = values_schema["properties"]["artifactStore"]["properties"]["baseUrl"]["pattern"]
    require(all(scheme in artifact_pattern for scheme in ["s3", "az", "gs", "file"]),
            "platform values schema must accept every implemented artifact-store backend")
    template = (ROOT / "NGKG_1_0_0_GA/charts/ngkg-platform/templates/hpc-mpi.yaml").read_text(encoding="utf-8")
    for marker in ["kueue.x-k8s.io/queue-name", "slotsPerWorker: 1", "ppr:1:node", "NGKG_NODE_SATURATION_TARGET_PERCENT", "ephemeral-storage"]:
        require(marker in template, f"MPI Helm contract is missing {marker}")
    require("HorizontalPodAutoscaler" not in template, "active MPI ranks must never be HPA-scaled")

    hpc_dockerfile = (ROOT / "docker_repos/ngkg-hpc-worker/Dockerfile").read_text(encoding="utf-8")
    require("mpicc" in hpc_dockerfile and "-fopenmp" in hpc_dockerfile,
            "HPC image does not compile both native kernels")

    operation_count = 0
    for spec in sorted(ROOT.glob("**/*openapi*.yaml")):
        if "vendor" in spec.parts:
            continue
        document = yaml.safe_load(spec.read_text(encoding="utf-8"))
        if not isinstance(document, dict) or "paths" not in document:
            continue
        for route, item in document["paths"].items():
            for method, operation in item.items():
                if method.lower() not in {"get", "post", "put", "patch", "delete"}:
                    continue
                operation_count += 1
                description = " ".join(operation.get("description", "").split())
                sentence_count = len([part for part in re.split(r"(?<=[.!?])\s+", description) if part])
                require(3 <= sentence_count <= 4, f"Swagger description must be 3-4 sentences: {method} {route}")
                require(bool(operation.get("operationId")) and bool(operation.get("summary")), f"Swagger identity missing: {method} {route}")
    require(operation_count >= 113, "cumulative Swagger route coverage unexpectedly shrank")
    online = yaml.safe_load((ROOT / "NGKG_1_0_0_GA/api/online-openapi.yaml").read_text(encoding="utf-8"))
    require("/v1/hpc/capabilities" in online["paths"], "HPC capability route is absent from Swagger")
    print(json.dumps({"status": "PASS", "images": len(names), "openApiOperations": operation_count, "hpcContracts": 3}, sort_keys=True))


if __name__ == "__main__":
    main()
