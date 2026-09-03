#!/usr/bin/env python3
"""Regenerate deterministic SHA-256 manifests for this candidate."""

from __future__ import annotations

import hashlib
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def write_manifest(root: Path, output: Path) -> None:
    files = sorted(
        path
        for path in root.rglob("*")
        if (
            path.is_file()
            and path != output
            and "target" not in path.parts
            and "__pycache__" not in path.parts
            and path.suffix != ".pyc"
        )
    )
    payload = "".join(f"{digest(path)}  ./{path.relative_to(root).as_posix()}\n" for path in files)
    output.write_text(payload, encoding="utf-8", newline="\n")


write_manifest(ROOT / "NGKG_1_0_0_GA", ROOT / "NGKG_1_0_0_GA/FILE_MANIFEST_SHA256.txt")
write_manifest(ROOT / "ngkg-agents", ROOT / "ngkg-agents/SOURCE_MANIFEST_SHA256.txt")
write_manifest(ROOT, ROOT / "BUNDLE_MANIFEST_SHA256.txt")
