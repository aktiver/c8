#!/usr/bin/env python3
"""Prove two isolated builders emitted the same release artifact set."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from phase6_common import atomic_json, canonical, load_json, require, sha256_bytes, sha256_file, valid_sha256

REQUIRED_CLASSES = {"rust-binary", "oci-index", "helm-chart", "crd", "openapi", "json-schema", "migration", "source-archive"}


def normalize(document: dict, manifest_path: Path) -> list[dict[str, str]]:
    require(document.get("formatVersion") == 1 and document.get("isolatedBuilder") is True, "invalid isolated builder manifest")
    require(document.get("networkControlled") is True and document.get("dependenciesLocked") is True and document.get("timestampsNormalized") is True, "builder was not reproducible and network controlled")
    artifacts = document.get("artifacts")
    require(isinstance(artifacts, list) and artifacts, "builder artifact inventory is empty")
    rows = []
    paths = set()
    artifact_root_value = document.get("artifactRoot")
    require(isinstance(artifact_root_value, str) and artifact_root_value, "builder artifactRoot is required")
    artifact_root = (manifest_path.parent / artifact_root_value).resolve()
    require(artifact_root.is_dir(), "builder artifactRoot does not exist")
    for artifact in artifacts:
        require(set(artifact) >= {"path", "class", "sha256"}, "builder artifact is incomplete")
        require(artifact["path"] not in paths, "builder artifact path is duplicated")
        require(artifact["class"] in REQUIRED_CLASSES and valid_sha256(artifact["sha256"]), "builder artifact class or digest is invalid")
        logical = Path(artifact["path"])
        require(not logical.is_absolute() and ".." not in logical.parts, "unsafe reproducibility artifact path")
        material = (artifact_root / logical).resolve()
        require(material.is_file() and not material.is_symlink() and material.is_relative_to(artifact_root), f"builder artifact does not exist: {artifact['path']}")
        require(sha256_file(material) == artifact["sha256"], f"builder artifact checksum mismatch: {artifact['path']}")
        paths.add(artifact["path"])
        rows.append({"path": artifact["path"], "class": artifact["class"], "sha256": artifact["sha256"]})
    require(REQUIRED_CLASSES <= {row["class"] for row in rows}, "builder artifact class coverage is incomplete")
    return sorted(rows, key=lambda row: (row["path"], row["class"]))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--builder-a", required=True, type=Path)
    parser.add_argument("--builder-b", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    a = load_json(args.builder_a.resolve())
    b = load_json(args.builder_b.resolve())
    require(a.get("builderId") != b.get("builderId"), "reproducible builds must use distinct builders")
    require(a.get("sourceSha256") == b.get("sourceSha256") and valid_sha256(a.get("sourceSha256")), "builders used different sources")
    rows_a, rows_b = normalize(a, args.builder_a.resolve()), normalize(b, args.builder_b.resolve())
    require(rows_a == rows_b, "isolated builder artifact digests differ")
    evidence = {
        "formatVersion": 1,
        "kind": "Phase6ReproducibleBuildEvidence",
        "subjectSha256": a["sourceSha256"],
        "builderAManifestSha256": sha256_file(args.builder_a.resolve()),
        "builderBManifestSha256": sha256_file(args.builder_b.resolve()),
        "artifactRootSha256": sha256_bytes(canonical(rows_a)),
        "artifactCount": len(rows_a),
        "distinctBuilders": True,
        "networkControlled": True,
        "dependenciesLocked": True,
        "timestampsNormalized": True,
        "synthetic": False,
        "status": "PASS",
        "complete": True,
    }
    atomic_json(args.output.resolve(), evidence)
    print(json.dumps({"status": "PASS", "artifactCount": len(rows_a), "artifactRootSha256": evidence["artifactRootSha256"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, KeyError, TypeError, ValueError, RuntimeError) as error:
        print(f"Phase 6 reproducible-build qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
