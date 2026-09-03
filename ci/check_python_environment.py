#!/usr/bin/env python3
"""Verify the controlled runner has the exact qualification Python packages."""
from importlib import metadata
from pathlib import Path
import re

root = Path(__file__).resolve().parents[2]
lock = root / "NGKG_1_0_0_GA/conformance/python-requirements.lock"
required = {}
for raw in lock.read_text(encoding="utf-8").splitlines():
    line = raw.strip()
    if not line or line.startswith("#"):
        continue
    match = re.fullmatch(r"([A-Za-z0-9_.-]+)==([A-Za-z0-9_.+-]+)", line)
    if not match:
        raise SystemExit(f"dependency is not exactly pinned: {line}")
    required[match.group(1)] = match.group(2)
failures = []
for name, expected in sorted(required.items()):
    try:
        observed = metadata.version(name)
    except metadata.PackageNotFoundError:
        failures.append(f"{name}: missing (expected {expected})")
        continue
    if observed != expected:
        failures.append(f"{name}: {observed} (expected {expected})")
if failures:
    raise SystemExit("controlled Python environment mismatch:\n" + "\n".join(failures))
print(f"controlled Python environment: PASS ({len(required)} exact packages)")
