#!/usr/bin/env python3
"""Fail-closed structural checks that do not pretend to compile Rust or deploy Kubernetes."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sys
import tomllib

import yaml


FORBIDDEN = (
    "TO" + "DO",
    "FIX" + "ME",
    "CHANGE" + "ME",
    "image:" + " latest",
    ":" + "latest",
)

# Vendored upstream source is checksum-pinned and must be compiled/tested, but
# upstream maintenance comments are not NGKG release placeholders. Historical
# evidence that explicitly records that inherited condition is immutable input.
FORBIDDEN_SCAN_EXCLUDED_PREFIXES = ("vendor/",)
FORBIDDEN_SCAN_EXCLUDED_FILES = {
    "PHASE_40_13_16_DELIVERY_REPORT.md",
    "qualification/phase40.13.19-structural-status.json",
}


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_structured(path: pathlib.Path) -> None:
    if path.suffix == ".json":
        json.loads(path.read_text(encoding="utf-8"))
    elif path.suffix == ".toml":
        tomllib.loads(path.read_text(encoding="utf-8"))
    elif path.suffix in {".yaml", ".yml"} and "templates" not in path.parts:
        list(yaml.safe_load_all(path.read_text(encoding="utf-8")))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=pathlib.Path, default=pathlib.Path.cwd())
    args = parser.parse_args()
    root = args.root.resolve()
    required = [
        "Cargo.toml",
        "rust-toolchain.toml",
        "README.md",
        "contracts/query-workload.schema.json",
        "benchmarks/workloads/cross-domain-v1.yaml",
        "test-corpus/datasets/cross-domain.trig",
        "test-corpus/queries/manifest.json",
        "crates/ngkg-reference/src/lib.rs",
        "services/reference-worker/src/main.rs",
        "adapters/hermit-reasoner/src/main/java/io/ngkg/reasoner/Main.java",
        "docs/phases/PHASE_13.md",
        "docs/phases/PHASE_14.md",
        "contracts/compilation-bundle.schema.json",
        "migrations/0002_atomic_compilation.sql",
        "crates/ngkg-artifact-store/src/lib.rs",
        "services/reference-worker/src/object_compile.rs",
        "verification/phase-14.json",
        "docs/phases/PHASE_15.md",
        "migrations/0003_distributed_build.sql",
        "crates/ngkg-distributed-build/src/lib.rs",
        "services/distributed-worker/src/object_stage.rs",
        "services/distributed-operator/src/main.rs",
        "verification/phase-15.json",
        "docs/phases/PHASE_16.md",
        "crates/ngkg-distributed-artifacts/src/lib.rs",
        "contracts/artifact-partition-manifest.schema.json",
        "contracts/distributed-artifact-root.schema.json",
        "scripts/run_distributed_artifact_slice.sh",
        "test-corpus/distributed/artifact-equivalence-v1.json",
        "verification/phase-16.json",
        "docs/phases/PHASE_17.md",
        "migrations/0004_distributed_artifacts.sql",
        "contracts/distributed-artifact-plan.schema.json",
        "scripts/qualify_phase17.sh",
        "scripts/verify_phase17_static.py",
        "verification/phase-17.json",
        "docs/phases/PHASE_18.md",
        "scripts/qualify_phase18.sh",
        "scripts/verify_phase18_static.py",
        "verification/phase-18.json",
        "docs/phases/PHASE_19.md",
        "migrations/0005_distributed_serving_root.sql",
        "migrations/0006_named_datasets.sql",
        "contracts/serving-root.schema.json",
        "contracts/serving-equivalence-report.schema.json",
        "scripts/qualify_phase19.sh",
        "scripts/verify_phase19_static.py",
        "verification/phase-19.json",
        "docs/phases/PHASE_20.md",
        "api/online-openapi.yaml",
        "deploy/online-serving/Dockerfile",
        "services/online-serving/src/main.rs",
        "scripts/qualify_phase20.sh",
        "scripts/verify_phase20_static.py",
        "verification/phase-20.json",
        "docs/phases/PHASE_21.md",
        "contracts/graph-capability-index.schema.json",
        "contracts/query-routing-certificate.schema.json",
        "test-corpus/routing/q01-cross-domain.json",
        "scripts/qualify_phase21.sh",
        "scripts/verify_phase21_static.py",
        "verification/phase-21.json",
        "docs/phases/PHASE_22.md",
        "contracts/distributed-query-plan.schema.json",
        "scripts/qualify_phase22.sh",
        "scripts/verify_phase22_static.py",
        "verification/phase-22.json",
        "docs/phases/PHASE_23.md",
        "scripts/qualify_phase23.sh",
        "scripts/verify_phase23_static.py",
        "verification/phase-23.json",
        "docs/phases/PHASE_24.md",
        "scripts/qualify_phase24.sh",
        "scripts/verify_phase24_static.py",
        "verification/phase-24.json",
        "docs/phases/PHASE_25.md",
        "scripts/qualify_phase25.sh",
        "scripts/verify_phase25_static.py",
        "verification/phase-25.json",
        "docs/phases/PHASE_26.md",
        "crates/ngkg-shuffle-cache/src/lib.rs",
        "scripts/qualify_phase26.sh",
        "scripts/verify_phase26_static.py",
        "verification/phase-26.json",
        "docs/phases/PHASE_27.md",
        "scripts/qualify_phase27.sh",
        "scripts/verify_phase27_static.py",
        "verification/phase-27.json",
        "docs/phases/PHASE_28.md",
        "contracts/tenant-admission-policy.schema.json",
        "scripts/qualify_phase28.sh",
        "scripts/verify_phase28_static.py",
        "verification/phase-28.json",
        "docs/phases/PHASE_29.md",
        "crates/ngkg-query-cache/src/lib.rs",
        "scripts/qualify_phase29.sh",
        "scripts/verify_phase29_static.py",
        "verification/phase-29.json",
        "docs/phases/PHASE_30.md",
        "crates/ngkg-grace-join/src/lib.rs",
        "scripts/qualify_phase30.sh",
        "scripts/verify_phase30_static.py",
        "verification/phase-30.json",
        "docs/phases/PHASE_31.md",
        "scripts/qualify_phase31.sh",
        "scripts/verify_phase31_static.py",
        "verification/phase-31.json",
        "docs/phases/PHASE_32.md",
        "scripts/qualify_phase32.sh",
        "scripts/verify_phase32_static.py",
        "verification/phase-32.json",
        "docs/phases/PHASE_33.md",
        "scripts/qualify_phase33.sh",
        "scripts/verify_phase33_static.py",
        "verification/phase-33.json",
        "docs/phases/PHASE_34.md",
        "scripts/qualify_phase34.sh",
        "scripts/verify_phase34_static.py",
        "verification/phase-34.json",
        "docs/phases/PHASE_35.md",
        "scripts/qualify_phase35.sh",
        "scripts/verify_phase35_static.py",
        "verification/phase-35.json",
        "docs/phases/PHASE_36.md",
        "scripts/qualify_phase36.sh",
        "scripts/verify_phase36_static.py",
        "verification/phase-36.json",
        "docs/phases/PHASE_37.md",
        "scripts/qualify_phase37.sh",
        "scripts/verify_phase37_static.py",
        "verification/phase-37.json",
        "contracts/rdf-dataset-catalog.schema.json",
        "contracts/reasoner-report.schema.json",
        "contracts/trig-source-metadata.schema.json",
        "scripts/fetch_w3c_conformance.py",
        "conformance/w3c-rdf-tests.lock.json",
        "crates/ngkg-dataset/src/lib.rs",
        "charts/ngkg-platform/templates/api-autoscaling.yaml",
        "scripts/validate_platform_values.py",
        "contracts/owl-signature.schema.json",
        "docs/phases/PHASE_40_1.md",
        "docs/OWL_SIGNATURE_CONTRACT.md",
        "scripts/validate_owl_signature.py",
        "scripts/verify_phase40_1_static.py",
        "scripts/qualify_phase40_1.sh",
        "verification/phase-40.1.json",
        "verification/phase-40.1-requirements.json",
        "verification/phase-40.1-traceability.json",
        "contracts/datatype-policy.schema.json",
        "policies/owl-direct-datatype-policy.json",
        "docs/phases/PHASE_40_2.md",
        "docs/DATATYPE_POLICY_CONTRACT.md",
        "scripts/validate_datatype_policy.py",
        "scripts/verify_phase40_2_static.py",
        "scripts/qualify_phase40_2.sh",
        "verification/phase-40.2.json",
        "verification/phase-40.2-requirements.json",
        "verification/phase-40.2-traceability.json",
        "contracts/direct-bgp-result.schema.json",
        "docs/phases/PHASE_40_3.md",
        "docs/DIRECT_BGP_RESULT_CONTRACT.md",
        "scripts/validate_direct_bgp_result.py",
        "scripts/verify_phase40_3_static.py",
        "scripts/qualify_phase40_3.sh",
        "verification/phase-40.3.json",
        "verification/phase-40.3-requirements.json",
        "verification/phase-40.3-traceability.json",
        "contracts/direct-certificate.schema.json",
        "docs/phases/PHASE_40_4.md",
        "docs/DIRECT_CERTIFICATE_CONTRACT.md",
        "scripts/validate_direct_certificate.py",
        "scripts/verify_phase40_4_static.py",
        "scripts/qualify_phase40_4.sh",
        "verification/phase-40.4.json",
        "verification/phase-40.4-requirements.json",
        "verification/phase-40.4-traceability.json",
        "contracts/owl-profile-qualification.schema.json",
        "docs/phases/PHASE_40_5.md",
        "docs/OWL_PROFILE_IMPORT_QUALIFICATION.md",
        "scripts/validate_owl_profile_qualification.py",
        "scripts/verify_phase40_5_static.py",
        "scripts/qualify_phase40_5.sh",
        "verification/phase-40.5.json",
        "verification/phase-40.5-requirements.json",
        "verification/phase-40.5-traceability.json",
        "contracts/owl-consistency-qualification.schema.json",
        "docs/phases/PHASE_40_6.md",
        "docs/OWL_CONSISTENCY_QUALIFICATION.md",
        "scripts/validate_owl_consistency_qualification.py",
        "scripts/verify_phase40_6_static.py",
        "scripts/qualify_phase40_6.sh",
        "verification/phase-40.6.json",
        "verification/phase-40.6-requirements.json",
        "verification/phase-40.6-traceability.json",
        "docs/phases/PHASE_40_11.md",
        "docs/PHASE40_REFERENCE_WORKER_CEILINGS.md",
        "scripts/verify_phase40_11_static.py",
        "scripts/qualify_phase40_11.sh",
        "verification/phase-40.11.json",
        "verification/phase-40.11-requirements.json",
        "verification/phase-40.11-traceability.json",
        "charts/ngkg-platform/templates/phase40-reference-ceilings.yaml",
        "services/reference-worker/src/phase40_limits.rs",
        "docs/phases/PHASE_40_12.md",
        "scripts/validate_phase40_online_ceilings.py",
        "scripts/verify_phase40_12_static.py",
        "scripts/qualify_phase40_12.sh",
        "verification/phase-40.12.json",
        "verification/phase-40.12-requirements.json",
        "verification/phase-40.12-traceability.json",
        "charts/ngkg-workloads/templates/phase40-online-ceilings.yaml",
        "services/online-serving/src/phase40_limits.rs",
        "docs/phases/PHASE_40_13.md",
        "docs/PHASE40_OPERATOR_CEILING_PROPAGATION.md",
        "scripts/validate_phase40_operator_propagation.py",
        "scripts/verify_phase40_13_static.py",
        "scripts/qualify_phase40_13.sh",
        "verification/phase-40.13.json",
        "verification/phase-40.13-requirements.json",
        "verification/phase-40.13-traceability.json",
        "crates/ngkg-release-qualification/src/ga.rs",
        "contracts/ga-qualification-ledger.schema.json",
        "contracts/ga-defect-ledger.schema.json",
        "contracts/ga-production-runtime-audit.schema.json",
        "contracts/ga-artifact-manifest.schema.json",
        "contracts/ga-freeze-manifest.schema.json",
        "contracts/ga-readiness.schema.json",
        "contracts/ga-release-certificate.schema.json",
        "scripts/freeze_ga_versions.py",
        "scripts/build_ga_freeze.py",
        "scripts/assess_ga_readiness.py",
        "scripts/audit_ga_production_runtime.py",
        "scripts/certify_ga_release.py",
        "scripts/build_deterministic_ga_archive.py",
        "scripts/verify_ga_static.py",
        "scripts/ci_ga_release.sh",
        "acceptance/ga.sh",
        "release/1.0.0/release-spec.yaml",
        "release/1.0.0/support-matrix.yaml",
        "release/1.0.0/qualification-plan.yaml",
        "release/1.0.0/ACCEPTANCE_TEST_PLAN.md",
        "release/1.0.0/KNOWN_ISSUES.md",
        "release/1.0.0/MAINTENANCE_POLICY.md",
        "docs/GA_RELEASE_AND_OPERATIONS.md",
        "verification/phase-1.0.0.json",
    ]
    errors: list[str] = []
    for relative in required:
        if not (root / relative).is_file():
            errors.append(f"missing required file: {relative}")
    manifests: list[dict[str, str | int]] = []
    for path in sorted(
        p
        for p in root.rglob("*")
        if p.is_file()
        and ".git" not in p.parts
        and "__pycache__" not in p.parts
        and ".cache" not in p.parts
        and "target" not in p.parts
    ):
        relative = path.relative_to(root)
        try:
            load_structured(path)
        except Exception as exc:  # validation tool must report every malformed artifact
            errors.append(f"cannot parse {relative}: {exc}")
        relative_text = relative.as_posix()
        scan_forbidden = (
            relative_text not in FORBIDDEN_SCAN_EXCLUDED_FILES
            and not relative_text.startswith(FORBIDDEN_SCAN_EXCLUDED_PREFIXES)
        )
        if scan_forbidden and (path.suffix in {".rs", ".java", ".py", ".sh", ".md", ".yaml", ".yml", ".json", ".toml"} or path.name == "Dockerfile"):
            text = path.read_text(encoding="utf-8", errors="replace")
            for token in FORBIDDEN:
                if token in text:
                    errors.append(f"forbidden unresolved token {token!r} in {relative}")
        manifests.append({"path": str(relative), "bytes": path.stat().st_size, "sha256": sha256(path)})
    print(json.dumps({"root": str(root), "files": manifests, "errors": errors}, indent=2))
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
