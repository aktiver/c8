#!/usr/bin/env python3
"""Create one deterministic cumulative ZIP per immutable phase tag."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import subprocess
import zipfile


PHASES = ["00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11A", "11B", "11C", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35"]


def run(root: pathlib.Path, *command: str) -> str:
    completed = subprocess.run(command, cwd=root, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())
    return completed.stdout.strip()


def digest(path: pathlib.Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=pathlib.Path, required=True)
    args = parser.parse_args()
    root = pathlib.Path(__file__).resolve().parents[1]
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, object]] = []
    previous: str | None = None
    for phase in PHASES:
        tag = f"phase-{phase}"
        commit = run(root, "git", "rev-parse", f"{tag}^{{commit}}")
        if previous:
            run(root, "git", "merge-base", "--is-ancestor", previous, tag)
        archive = output / f"NGKG_PHASE_{phase}.zip"
        run(root, "git", "archive", "--format=zip", f"--prefix=NGKG_PHASE_{phase}/", f"--output={archive}", tag)
        with zipfile.ZipFile(archive) as bundle:
            names = bundle.namelist()
            required = f"NGKG_PHASE_{phase}/docs/phases/PHASE_{phase}.md"
            if required not in names or any("/.git/" in name for name in names):
                raise RuntimeError(f"archive verification failed for {archive.name}")
        records.append({
            "phase": phase,
            "tag": tag,
            "commit": commit,
            "fileName": archive.name,
            "bytes": archive.stat().st_size,
            "sha256": digest(archive),
            "entryCount": len(names),
            "cumulativeFrom": previous,
        })
        previous = tag
    index = output / "NGKG_PHASE_ARCHIVE_INDEX.json"
    index.write_text(json.dumps({"schemaVersion": 1, "archives": records}, indent=2) + "\n", encoding="utf-8")
    print(index)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
