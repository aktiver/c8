#!/usr/bin/env python3
"""Set or verify the exact 1.0.0-rc.1 version across frozen product surfaces."""

from __future__ import annotations

import argparse
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[1]
VERSION = "1.0.0-rc.1"

REPLACEMENTS = {
    "Cargo.toml": [('version = "0.7.0"', f'version = "{VERSION}"')],
    "api/openapi.yaml": [("  version: 0.5.0", f"  version: {VERSION}")],
    "api/online-openapi.yaml": [("  version: 2.6.0", f"  version: {VERSION}")],
    "charts/ngkg-platform/Chart.yaml": [("version: 0.4.0", f"version: {VERSION}"), ("appVersion: 0.4.0", f"appVersion: {VERSION}")],
    "charts/ngkg-workloads/Chart.yaml": [("version: 0.5.0", f"version: {VERSION}"), ("appVersion: 0.6.0", f"appVersion: {VERSION}")],
    "charts/ngkg-crds/Chart.yaml": [("version: 0.4.0", f"version: {VERSION}"), ("appVersion: 0.4.0", f"appVersion: {VERSION}")],
    "benchmarks/phase40.13.23/qualification-inventory.yaml": [("version: 0.7.0", f"version: {VERSION}")],
    "release/phase40.13.24/qualification-inventory.yaml": [("version: 0.7.0", f"version: {VERSION}")],
    "conformance/phase40.13.22-suite.json": [('"version": "0.7.0"', f'"version": "{VERSION}"')],
    "scripts/verify_phase40_13_23_static.py": [("ngkg-rust 0.7.0", f"ngkg-rust {VERSION}"), ('"version": "0.7.0"', f'"version": "{VERSION}"')],
    "scripts/verify_phase40_13_22_static.py": [('"ngkg": "0.7.0"', f'"ngkg": "{VERSION}"')],
}


def update_text(path: pathlib.Path, pairs: list[tuple[str, str]], check: bool) -> None:
    text = path.read_text(encoding="utf-8")
    if check:
        for _, desired in pairs:
            if desired not in text:
                raise ValueError(f"{path.relative_to(ROOT)} is not frozen at {VERSION}")
        return
    for old, desired in pairs:
        if desired in text:
            continue
        if old not in text:
            raise ValueError(f"{path.relative_to(ROOT)} lacks expected old or new version text")
        text = text.replace(old, desired)
    path.write_text(text, encoding="utf-8")


def update_lock(check: bool) -> None:
    path = ROOT / "Cargo.lock"
    text = path.read_text(encoding="utf-8")
    blocks = text.split("[[package]]")
    changed = 0
    for index in range(1, len(blocks)):
        name = re.search(r'^\s*name = "([^"]+)"', blocks[index], re.MULTILINE)
        if name and name.group(1).startswith("ngkg-"):
            version = re.search(r'^version = "([^"]+)"', blocks[index], re.MULTILINE)
            if version is None:
                raise ValueError(f"Cargo.lock package {name.group(1)} has no version")
            if check and version.group(1) != VERSION:
                raise ValueError(f"Cargo.lock package {name.group(1)} is {version.group(1)}")
            if not check and version.group(1) != VERSION:
                blocks[index] = blocks[index][:version.start(1)] + VERSION + blocks[index][version.end(1):]
                changed += 1
    if not check:
        path.write_text("[[package]]".join(blocks), encoding="utf-8")
    elif changed:
        raise ValueError("Cargo.lock RC1 check unexpectedly mutated state")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    for relative, pairs in REPLACEMENTS.items():
        update_text(ROOT / relative, pairs, args.check)
    update_lock(args.check)
    print(f"RC1 version {'verified' if args.check else 'frozen'}: {VERSION}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
