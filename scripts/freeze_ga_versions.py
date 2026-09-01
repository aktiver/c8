#!/usr/bin/env python3
"""Set or verify the exact 1.0.0 version on active product surfaces."""

from __future__ import annotations

import argparse
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[1]
VERSION = "1.0.0"
OLD = "1.0.0-rc.1"

ACTIVE_FILES = [
    "Cargo.toml",
    "api/openapi.yaml",
    "api/online-openapi.yaml",
    "charts/ngkg-crds/Chart.yaml",
    "charts/ngkg-platform/Chart.yaml",
    "charts/ngkg-workloads/Chart.yaml",
    "benchmarks/phase40.13.23/qualification-inventory.yaml",
    "release/phase40.13.24/qualification-inventory.yaml",
    "conformance/phase40.13.22-suite.json",
    "scripts/verify_phase40_13_22_static.py",
    "scripts/verify_phase40_13_23_static.py",
]


def replace_active(path: pathlib.Path, check: bool) -> None:
    text = path.read_text(encoding="utf-8")
    if check:
        if OLD in text or VERSION not in text:
            raise ValueError(f"{path.relative_to(ROOT)} is not frozen at {VERSION}")
        return
    path.write_text(text.replace(OLD, VERSION), encoding="utf-8")


def update_lock(check: bool) -> None:
    path = ROOT / "Cargo.lock"
    text = path.read_text(encoding="utf-8")
    blocks = text.split("[[package]]")
    found = 0
    for index in range(1, len(blocks)):
        name = re.search(r'^\s*name = "([^"]+)"', blocks[index], re.MULTILINE)
        version = re.search(r'^version = "([^"]+)"', blocks[index], re.MULTILINE)
        if name and name.group(1).startswith("ngkg-"):
            found += 1
            if version is None:
                raise ValueError(f"Cargo.lock package {name.group(1)} has no version")
            if check and version.group(1) != VERSION:
                raise ValueError(f"Cargo.lock package {name.group(1)} is {version.group(1)}")
            if not check and version.group(1) != VERSION:
                blocks[index] = blocks[index][:version.start(1)] + VERSION + blocks[index][version.end(1):]
    if found == 0:
        raise ValueError("Cargo.lock contains no NGKG workspace packages")
    if not check:
        path.write_text("[[package]]".join(blocks), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    for relative in ACTIVE_FILES:
        replace_active(ROOT / relative, args.check)
    update_lock(args.check)
    print(f"GA version {'verified' if args.check else 'frozen'}: {VERSION}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
