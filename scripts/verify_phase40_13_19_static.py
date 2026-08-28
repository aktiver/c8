#!/usr/bin/env python3
"""Fail-closed source and deployment contracts for Phase 40.13.19."""

from __future__ import annotations

import json
import pathlib
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(relative: str, *tokens: str) -> str:
    text = (ROOT / relative).read_text(encoding="utf-8")
    for token in tokens:
        if token not in text:
            raise RuntimeError(f"{relative} is missing {token!r}")
    return text


def main() -> int:
    core = require(
        "crates/ngkg-storage-recovery/src/lib.rs",
        "select_replica_targets",
        "ngkg-storage-rendezvous-v1",
        "InsufficientFailureDomains",
        "stable_work_id",
        "execute_transfer",
        "verify_remote",
        "TransferState::Quarantined",
        "certify_recovery",
        "RecoveryCertificationAccumulator",
        "duplicate task result",
        "register_primary_replicas",
        "state='RETIRING'",
        "build_backup_manifest",
        "build_restore_plan",
        "build_restore_certificate",
        "discover_artifact_closure",
        "validate_recovery_plan",
        "pub async fn fail",
    )
    worker = require(
        "services/storage-recovery-worker/src/main.rs",
        '"transfer" => run_transfer()',
        '"finalize" => run_finalize()',
        "JOB_COMPLETION_INDEX",
        "NGKG_RECOVERY_ATTEMPT_ID",
        "attempts/{task_index:010}",
        "materialize_verified",
        "put_file_immutable",
        "RecoveryCertificationAccumulator",
    )
    operator = require(
        "services/storage-recovery-operator/src/main.rs",
        "Controller::new",
        "completion_mode: completions.map",
        '"Indexed"',
        "batch.kubernetes.io/job-completion-index",
        "backoff_limit_per_index",
        "max_failed_indexes",
        "kueue.x-k8s.io/queue-name",
        "max_parallelism",
        "max_in_flight_bytes",
        "largest_task_bytes",
        "parallel storage work exceeds maxInFlightBytes",
        '"ngkg.io/workload"',
        '"storage-recovery"',
        '"OMP_NUM_THREADS"',
        '"OPENBLAS_NUM_THREADS"',
        '"MKL_NUM_THREADS"',
        "VerificationBarrierFailedClosed",
        "TRANSFER_JOB_FAILED",
        "plan_reason_matches",
        "commit_success",
    )
    api = require(
        "services/api/src/main.rs",
        "/storage-operations",
        "/restores",
        "derive_operation_id",
        "build_and_publish_storage_manifest",
        "get_snapshot_recovery_roots",
        "discover_artifact_closure",
        "cap_storage_parallelism",
        "validate_storage_task_sizes",
        "register_primary_replicas",
        "ensure_storage_recovery_resource",
    )
    migration = require(
        "migrations/0008_multinode_storage_recovery.sql",
        "storage_recovery_operation",
        "snapshot_artifact_replica",
        "snapshot_backup",
        "ENABLE ROW LEVEL SECURITY",
        "ngkg_storage_recovery_guard",
        "ngkg_snapshot_replica_guard",
        "READY",
        "QUARANTINED",
        "RETIRING",
        "LOST",
    )
    for schema in (
        "snapshot-storage-manifest.schema.json",
        "storage-recovery-plan.schema.json",
        "storage-recovery-result.schema.json",
        "storage-recovery-certificate.schema.json",
        "snapshot-backup-manifest.schema.json",
        "snapshot-restore-certificate.schema.json",
    ):
        document = json.loads((ROOT / "contracts" / schema).read_text(encoding="utf-8"))
        if document.get("additionalProperties") is not False:
            raise RuntimeError(f"{schema} is not fail closed")
    crd = yaml.safe_load(
        (ROOT / "charts/ngkg-crds/crds/ngkg.io_ngkgstoragerecoveries.yaml").read_text(encoding="utf-8")
    )
    if crd["spec"]["versions"][0]["subresources"] != {"status": {}}:
        raise RuntimeError("storage recovery CRD lacks status subresource")
    values = yaml.safe_load((ROOT / "charts/ngkg-platform/values.yaml").read_text(encoding="utf-8"))
    registry = json.loads(values["storageRecovery"]["targetsJson"])
    targets = registry["targets"]
    if len({target["failureDomain"] for target in targets}) < 3:
        raise RuntimeError("default storage targets do not span three failure domains")
    if values["storageRecovery"]["operatorReplicas"] < 2:
        raise RuntimeError("recovery coordinator is not highly available")
    workload_values = yaml.safe_load(
        (ROOT / "charts/ngkg-workloads/values.yaml").read_text(encoding="utf-8")
    )
    if workload_values["hpcNodeGroups"].get("storage_recovery_num_of_nodes") != 0:
        raise RuntimeError("storage recovery workers must support scale from zero")
    scaling = workload_values["autoscaling"].get("storageRecovery", {})
    if scaling.get("owner") != "operator" or scaling.get("maxNodes", 0) < 2:
        raise RuntimeError("storage recovery has no qualified node-pool scaling envelope")
    require(
        "charts/ngkg-platform/templates/storage-recovery-operator.yaml",
        "PodDisruptionBudget",
        "topologySpreadConstraints",
        "NGKG_STORAGE_RECOVERY_WORKER_IMAGE",
    )
    require(
        "charts/ngkg-workloads/templates/kueue.yaml",
        "ngkg-storage-recovery",
        "storageRecovery.cpu",
        "storageRecovery.memory",
    )
    matrix = json.loads(
        (ROOT / "test-corpus/storage-recovery/phase40.13.19-failure-matrix.json").read_text(encoding="utf-8")
    )["cases"]
    required_failures = {
        "pod-evicted", "target-unavailable", "checksum-mismatch",
        "partial-worker-failure", "duplicate-delivery", "request-hash-mismatch",
    }
    observed = {case["failure"] for case in matrix if case["failure"]}
    if not required_failures <= observed:
        raise RuntimeError("storage recovery failure matrix is incomplete")
    combined = core + worker + operator + api + migration
    if "align_ontology" in combined or "raw_data_mapping" in combined:
        raise RuntimeError("ontology alignment or raw-data mapping entered storage recovery")
    print("phase 40.13.19 static qualification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"phase 40.13.19 static qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
