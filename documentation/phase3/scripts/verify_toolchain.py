#!/usr/bin/env python3
"""Verify controlled-runner executables against an approved binary lock."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--require", action="append", default=[])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    policy = json.loads(args.lock.read_text(encoding="utf-8"))
    if policy.get("formatVersion") != 1:
        raise ValueError("unsupported toolchain lock")
    entries = {item["name"]: item["sha256"] for item in policy.get("tools", [])}
    if len(entries) != len(policy.get("tools", [])):
        raise ValueError("duplicate toolchain entry")
    observed = []
    for name in sorted(set(args.require)):
        expected = entries.get(name)
        if not isinstance(expected, str) or len(expected) != 64 or any(char not in "0123456789abcdef" for char in expected):
            raise ValueError(f"missing approved tool checksum: {name}")
        executable = shutil.which(name)
        if not executable:
            raise ValueError(f"tool is unavailable: {name}")
        path = Path(executable).resolve()
        actual = digest(path)
        if actual != expected:
            raise ValueError(f"tool checksum mismatch: {name}")
        observed.append({"name": name, "pathSha256": hashlib.sha256(str(path).encode()).hexdigest(), "binarySha256": actual})
    result = {"formatVersion": 1, "lockSha256": digest(args.lock), "tools": observed, "complete": True}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    print(json.dumps({"tools": len(observed), "complete": True}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
