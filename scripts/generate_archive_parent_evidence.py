#!/usr/bin/env python3
"""Generate checksum-bound cumulative archive-parent evidence for a delivered NGKG phase."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import zipfile

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_manifest(data: bytes) -> dict[str, str]:
    manifest: dict[str, str] = {}
    for line_number, raw in enumerate(data.decode("utf-8").splitlines(), start=1):
        if not raw:
            continue
        try:
            digest, path = raw.split("  ", 1)
        except ValueError as error:
            raise RuntimeError(f"invalid parent manifest line {line_number}") from error
        if not SHA256_RE.fullmatch(digest) or not path or path.startswith("/") or ".." in pathlib.PurePosixPath(path).parts:
            raise RuntimeError(f"invalid parent manifest entry at line {line_number}")
        if path in manifest:
            raise RuntimeError(f"duplicate parent manifest path: {path}")
        manifest[path] = digest
    if not manifest:
        raise RuntimeError("parent archive manifest is empty")
    return manifest


def locate_parent_manifest(archive: zipfile.ZipFile) -> tuple[str, bytes]:
    matches = [name for name in archive.namelist() if name.endswith("/FILE_MANIFEST_SHA256.txt") or name == "FILE_MANIFEST_SHA256.txt"]
    if len(matches) != 1:
        raise RuntimeError("parent archive must contain exactly one FILE_MANIFEST_SHA256.txt")
    name = matches[0]
    return name, archive.read(name)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parent-archive", required=True, type=pathlib.Path)
    parser.add_argument("--parent-phase", required=True, type=int)
    parser.add_argument("--current-phase", required=True, type=int)
    parser.add_argument("--root", default=pathlib.Path(__file__).resolve().parents[1], type=pathlib.Path)
    args = parser.parse_args()

    root = args.root.resolve()
    archive_path = args.parent_archive.resolve()
    if args.current_phase != args.parent_phase + 1:
        raise RuntimeError("current phase must be exactly one greater than parent phase")
    if not archive_path.is_file():
        raise RuntimeError(f"parent archive does not exist: {archive_path}")

    parent_archive_sha256 = sha256_file(archive_path)
    with zipfile.ZipFile(archive_path) as archive:
        bad = archive.testzip()
        if bad is not None:
            raise RuntimeError(f"parent archive ZIP integrity failed at {bad}")
        manifest_member, manifest_bytes = locate_parent_manifest(archive)
    parent = parse_manifest(manifest_bytes)

    parents_dir = root / "verification" / "parents"
    parents_dir.mkdir(parents=True, exist_ok=True)
    embedded_manifest = parents_dir / f"phase-{args.parent_phase:02d}-files.sha256"
    embedded_manifest.write_bytes(manifest_bytes)

    changed: list[dict[str, str]] = []
    missing: list[str] = []
    for relative, parent_sha256 in sorted(parent.items()):
        current_path = root / relative
        if not current_path.is_file():
            missing.append(relative)
            continue
        current_sha256 = sha256_file(current_path)
        if current_sha256 != parent_sha256:
            changed.append(
                {
                    "path": relative,
                    "parentSha256": parent_sha256,
                    "currentSha256": current_sha256,
                }
            )
    if missing:
        raise RuntimeError(f"current tree deleted parent files: {missing[:10]}")

    evidence = {
        "formatVersion": 1,
        "currentPhase": args.current_phase,
        "parentPhase": args.parent_phase,
        "parentArchiveSha256": parent_archive_sha256,
        "parentFileManifestSha256": sha256_bytes(manifest_bytes),
        "parentPayloadFileCount": len(parent),
        "embeddedParentManifest": embedded_manifest.relative_to(root).as_posix(),
        "deletedFiles": [],
        "changedParentFiles": changed,
    }
    output = root / "verification" / "archive-parent.json"
    output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(output), "parentFiles": len(parent), "changedParentFiles": len(changed)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
