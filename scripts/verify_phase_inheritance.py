#!/usr/bin/env python3
"""Prove cumulative phase inheritance from Git tags or checksum-bound archive ancestry."""

from __future__ import annotations

import hashlib
import json
import pathlib
import re
import subprocess
import sys

PHASES = ["00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11A", "11B", "11C", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35"]
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git(root: pathlib.Path, *args: str) -> str:
    completed = subprocess.run(["git", *args], cwd=root, text=True, capture_output=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())
    return completed.stdout.strip()


def verify_git(root: pathlib.Path) -> tuple[list[dict[str, object]], list[str]]:
    errors: list[str] = []
    evidence: list[dict[str, object]] = []
    for index, phase in enumerate(PHASES):
        tag = f"phase-{phase}"
        try:
            commit = git(root, "rev-parse", f"{tag}^{{commit}}")
            files = git(root, "ls-tree", "-r", "--name-only", tag).splitlines()
            required_doc = f"docs/phases/PHASE_{phase}.md"
            if required_doc not in files:
                errors.append(f"{tag} is missing {required_doc}")
            ancestor = None
            if index:
                previous = f"phase-{PHASES[index - 1]}"
                check = subprocess.run(["git", "merge-base", "--is-ancestor", previous, tag], cwd=root, check=False)
                ancestor = check.returncode == 0
                if not ancestor:
                    errors.append(f"{tag} is not a descendant of {previous}")
            evidence.append({"phase": phase, "tag": tag, "commit": commit, "fileCount": len(files), "previousIsAncestor": ancestor})
        except RuntimeError as error:
            errors.append(f"{tag}: {error}")
    return evidence, errors


def parse_manifest(path: pathlib.Path) -> dict[str, str]:
    output: dict[str, str] = {}
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw:
            continue
        try:
            digest, relative = raw.split("  ", 1)
        except ValueError as error:
            raise RuntimeError(f"invalid embedded parent manifest line {line_number}") from error
        if not SHA256_RE.fullmatch(digest) or not relative or relative.startswith("/") or ".." in pathlib.PurePosixPath(relative).parts:
            raise RuntimeError(f"invalid embedded parent manifest entry at line {line_number}")
        if relative in output:
            raise RuntimeError(f"duplicate embedded parent manifest path: {relative}")
        output[relative] = digest
    if not output:
        raise RuntimeError("embedded parent manifest is empty")
    return output


def require_phase_records(root: pathlib.Path, current_phase: int) -> None:
    for phase in range(36, current_phase + 1):
        doc = root / "docs" / "phases" / f"PHASE_{phase}.md"
        record = root / "verification" / f"phase-{phase}.json"
        if not doc.is_file() or not record.is_file():
            raise RuntimeError(f"archive is missing cumulative Phase {phase} documentation or verification")


def verify_archive(root: pathlib.Path) -> tuple[list[dict[str, object]], list[str]]:
    errors: list[str] = []
    evidence_rows: list[dict[str, object]] = []
    try:
        evidence_path = root / "verification" / "archive-parent.json"
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
        if evidence.get("formatVersion") != 1:
            raise RuntimeError("unsupported archive-parent formatVersion")
        current_phase = int(evidence["currentPhase"])
        parent_phase = int(evidence["parentPhase"])
        if current_phase != parent_phase + 1:
            raise RuntimeError("archive parent is not the immediately preceding phase")
        require_phase_records(root, current_phase)
        parent_manifest_path = root / evidence["embeddedParentManifest"]
        if not parent_manifest_path.is_file() or parent_manifest_path.is_symlink():
            raise RuntimeError("embedded parent manifest is missing or not a regular file")
        observed_manifest_sha256 = sha256_file(parent_manifest_path)
        if observed_manifest_sha256 != evidence.get("parentFileManifestSha256"):
            raise RuntimeError("embedded parent manifest checksum differs from archive-parent evidence")
        parent = parse_manifest(parent_manifest_path)
        if len(parent) != int(evidence.get("parentPayloadFileCount", -1)):
            raise RuntimeError("embedded parent manifest file count differs from archive-parent evidence")
        if evidence.get("deletedFiles") != []:
            raise RuntimeError("cumulative archive inheritance forbids deleted parent files")
        declared_changes = {
            row["path"]: (row["parentSha256"], row["currentSha256"])
            for row in evidence.get("changedParentFiles", [])
        }
        if len(declared_changes) != len(evidence.get("changedParentFiles", [])):
            raise RuntimeError("archive-parent evidence contains duplicate changed paths")
        observed_changes: set[str] = set()
        for relative, parent_sha256 in parent.items():
            current_path = root / relative
            if not current_path.is_file() or current_path.is_symlink():
                raise RuntimeError(f"parent file is missing from cumulative archive: {relative}")
            current_sha256 = sha256_file(current_path)
            if current_sha256 == parent_sha256:
                if relative in declared_changes:
                    raise RuntimeError(f"archive-parent declares unchanged file as changed: {relative}")
                continue
            observed_changes.add(relative)
            declared = declared_changes.get(relative)
            if declared != (parent_sha256, current_sha256):
                raise RuntimeError(f"undeclared or checksum-mismatched parent modification: {relative}")
        unexpected = set(declared_changes) - observed_changes
        if unexpected:
            raise RuntimeError(f"archive-parent declares changes not present in tree: {sorted(unexpected)[:10]}")
        parent_archive_sha256 = evidence.get("parentArchiveSha256", "")
        if not isinstance(parent_archive_sha256, str) or not SHA256_RE.fullmatch(parent_archive_sha256):
            raise RuntimeError("parent archive SHA-256 is invalid")
        evidence_rows.append(
            {
                "mode": "archive-manifest-chain",
                "phase": current_phase,
                "parentPhase": parent_phase,
                "parentArchiveSha256": parent_archive_sha256,
                "parentPayloadFileCount": len(parent),
                "changedParentFileCount": len(observed_changes),
                "deletedParentFileCount": 0,
            }
        )
    except (KeyError, OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as error:
        errors.append(str(error))
    return evidence_rows, errors


def main() -> int:
    root = pathlib.Path(__file__).resolve().parents[1]
    if (root / ".git").exists():
        phases, errors = verify_git(root)
        mode = "git-tags"
    else:
        phases, errors = verify_archive(root)
        mode = "archive-manifest-chain"
    print(json.dumps({"mode": mode, "phases": phases, "errors": errors}, indent=2))
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
