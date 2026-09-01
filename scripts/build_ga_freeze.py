#!/usr/bin/env python3
"""Inventory the final GA compatibility surfaces and bind them to RC1."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
from typing import Any

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
VERSION = "1.0.0"
GENERATED = {
    "FILE_MANIFEST_SHA256.txt",
    "release/1.0.0/freeze-manifest.json",
    "release/1.0.0/source-input-files.sha256",
    "release/1.0.0/ga-readiness.json",
    "release/1.0.0/ga-publication-certificate.json",
}


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha_file(path: pathlib.Path) -> str:
    return sha_bytes(path.read_bytes())


def files(pattern: str) -> list[pathlib.Path]:
    return sorted(path for path in ROOT.glob(pattern) if path.is_file())


def entry(surface: str, path: str, digest: str, count: int = 1) -> dict[str, Any]:
    if count < 1:
        raise ValueError(f"frozen entry {path} is empty")
    return {"surface": surface, "path": path, "sha256": digest, "itemCount": count}


def file_entries(surface: str, paths: list[pathlib.Path]) -> list[dict[str, Any]]:
    return [entry(surface, path.relative_to(ROOT).as_posix(), sha_file(path)) for path in paths]


def openapi_entries() -> list[dict[str, Any]]:
    methods = {"get", "put", "post", "delete", "options", "head", "patch", "trace"}
    output = []
    for path in files("api/*openapi.yaml"):
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        if document.get("info", {}).get("version") != VERSION:
            raise ValueError(f"{path.relative_to(ROOT)} is not GA-frozen")
        operations = sorted(f"{method.upper()} {route}" for route, item in document.get("paths", {}).items() for method in item if method.lower() in methods)
        if not operations:
            raise ValueError(f"{path.relative_to(ROOT)} has no operations")
        output.append(entry("open-api", path.relative_to(ROOT).as_posix(), sha_bytes(canonical({"fileSha256": sha_file(path), "operations": operations})), len(operations)))
    return output


def environment_entry() -> dict[str, Any]:
    candidates = files("crates/**/*.rs") + files("services/**/*.rs") + files("charts/**/*.yaml") + files("deploy/**/*.tpl")
    names: set[str] = set()
    for path in candidates:
        names.update(re.findall(r"\bNGKG_[A-Z][A-Z0-9_]+\b", path.read_text(encoding="utf-8")))
    return entry("environment", "inventory:NGKG_environment_variables", sha_bytes(canonical(sorted(names))), len(names))


def source_manifest(extra: set[str]) -> tuple[bytes, str]:
    rows = []
    for path in sorted(item for item in ROOT.rglob("*") if item.is_file() and "__pycache__" not in item.parts):
        relative = path.relative_to(ROOT).as_posix()
        if relative in GENERATED or relative in extra:
            continue
        rows.append(f"{sha_file(path)}  {relative}\n")
    value = "".join(rows).encode()
    return value, sha_bytes(value)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=pathlib.Path, default=ROOT / "release/1.0.0/freeze-manifest.json")
    parser.add_argument("--source-files-output", type=pathlib.Path, default=ROOT / "release/1.0.0/source-input-files.sha256")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    extra = set()
    for path in (args.output, args.source_files_output):
        try:
            extra.add(path.resolve().relative_to(ROOT).as_posix())
        except ValueError:
            pass
    source_bytes, source_sha = source_manifest(extra)
    entries = openapi_entries()
    entries += file_entries("json-schema", files("contracts/*.schema.json") + files("charts/*/values.schema.json"))
    entries += file_entries("crd", files("charts/ngkg-crds/crds/*.yaml"))
    helm = files("charts/*/Chart.yaml") + files("charts/*/values.yaml") + files("charts/*/values.schema.json") + files("charts/*/templates/*")
    entries += file_entries("helm", sorted(helm))
    for chart in files("charts/*/Chart.yaml"):
        value = yaml.safe_load(chart.read_text(encoding="utf-8"))
        if value.get("version") != VERSION or value.get("appVersion") != VERSION:
            raise ValueError(f"{chart.relative_to(ROOT)} is not GA-frozen")
    entries.append(environment_entry())
    entries += file_entries("database-migration", files("migrations/*.sql"))
    object_paths = sorted({*files("contracts/*artifact*.schema.json"), *files("contracts/*storage*.schema.json"), *files("contracts/*backup*.schema.json"), *files("contracts/*restore*.schema.json"), *files("contracts/*checkpoint*.schema.json")})
    semantic_paths = sorted({*files("contracts/*snapshot*.schema.json"), *files("contracts/*proof*.schema.json"), *files("contracts/*certificate*.schema.json"), *files("contracts/*reasoner*.schema.json"), *files("contracts/*qualification*.schema.json")})
    entries += file_entries("object-layout", object_paths)
    entries += file_entries("semantic-artifact", semantic_paths)
    entries.sort(key=lambda value: (value["surface"], value["path"]))
    if {value["surface"] for value in entries} != {"open-api", "json-schema", "crd", "helm", "environment", "database-migration", "object-layout", "semantic-artifact"}:
        raise ValueError("GA freeze surface coverage is incomplete")
    rc1 = ROOT / "release/1.0.0-rc1/freeze-manifest.json"
    freeze = {"formatVersion": 1, "releaseVersion": VERSION, "sourceManifestSha256": source_sha,
              "rc1FreezeSha256": sha_file(rc1), "entries": entries, "changesRequirePatchDefect": True, "complete": True}
    frozen_bytes = canonical(freeze) + b"\n"
    if args.check:
        if args.output.read_bytes() != frozen_bytes or args.source_files_output.read_bytes() != source_bytes:
            raise ValueError("GA source or public interface drifted after freeze")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(frozen_bytes)
        args.source_files_output.write_bytes(source_bytes)
    print(json.dumps({"entries": len(entries), "sourceFiles": len(source_bytes.splitlines()), "sourceManifestSha256": source_sha, "freezeSha256": sha_bytes(frozen_bytes.rstrip())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
