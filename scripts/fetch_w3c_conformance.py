#!/usr/bin/env python3
"""Fetch and verify the immutable W3C RDF/SPARQL conformance suite snapshot."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import uuid

HEX40 = re.compile(r"^[0-9a-f]{40}$")


def run(*args: str, cwd: Path | None = None, capture: bool = False) -> str:
    completed = subprocess.run(
        args,
        cwd=cwd,
        check=True,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )
    return completed.stdout.strip() if capture else ""


def load_lock(path: Path) -> tuple[str, str, tuple[str, ...]]:
    value = json.loads(path.read_text(encoding="utf-8"))
    expected = {"formatVersion", "repository", "commit", "requiredManifests"}
    if set(value) != expected or value["formatVersion"] != 1:
        raise ValueError("W3C suite lock has an unsupported schema")
    repository = value["repository"]
    commit = value["commit"]
    manifests = value["requiredManifests"]
    if repository != "https://github.com/w3c/rdf-tests.git":
        raise ValueError("W3C suite lock repository is not the approved upstream")
    if not isinstance(commit, str) or HEX40.fullmatch(commit) is None:
        raise ValueError("W3C suite lock commit must be a lowercase full Git SHA-1")
    if (
        not isinstance(manifests, list)
        or not manifests
        or any(not isinstance(item, str) or not item.endswith("/manifest.ttl") and "manifest-" not in item for item in manifests)
    ):
        raise ValueError("W3C suite lock requiredManifests is invalid")
    normalized: list[str] = []
    for item in manifests:
        candidate = Path(item)
        if candidate.is_absolute() or ".." in candidate.parts or item.startswith("/"):
            raise ValueError(f"W3C manifest path escapes the suite root: {item}")
        normalized.append(candidate.as_posix())
    if len(set(normalized)) != len(normalized):
        raise ValueError("W3C suite lock contains duplicate manifest paths")
    return repository, commit, tuple(normalized)


def verify_checkout(root: Path, repository: str, commit: str, manifests: tuple[str, ...]) -> None:
    if root.is_symlink() or not root.is_dir():
        raise ValueError("W3C suite checkout must be a real directory")
    git_dir = root / ".git"
    if git_dir.is_symlink() or not git_dir.exists():
        raise ValueError("W3C suite checkout is not a Git worktree")
    observed_commit = run("git", "rev-parse", "HEAD", cwd=root, capture=True)
    if observed_commit != commit:
        raise ValueError(
            f"W3C suite commit mismatch: expected {commit}, observed {observed_commit}"
        )
    origin = run("git", "remote", "get-url", "origin", cwd=root, capture=True)
    if origin != repository:
        raise ValueError(f"W3C suite origin mismatch: expected {repository}, observed {origin}")
    if run("git", "status", "--porcelain=v1", "--untracked-files=all", cwd=root, capture=True):
        raise ValueError("W3C suite checkout contains local modifications or untracked files")
    for relative in manifests:
        manifest = root / relative
        metadata = manifest.lstat()
        if manifest.is_symlink() or not manifest.is_file() or metadata.st_size == 0:
            raise ValueError(f"W3C manifest is absent, empty, or not a regular file: {relative}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--lock",
        type=Path,
        default=Path("conformance/w3c-rdf-tests.lock.json"),
    )
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()

    if shutil.which("git") is None:
        raise RuntimeError("git is required to fetch the pinned W3C conformance suite")
    lock_path = args.lock.resolve(strict=True)
    repository, commit, manifests = load_lock(lock_path)

    cache_root = args.cache_root.expanduser().resolve()
    if cache_root.exists():
        if cache_root.is_symlink() or not cache_root.is_dir():
            raise ValueError("W3C suite cache root must be a real directory")
    else:
        cache_root.mkdir(parents=True, mode=0o750)
    destination = cache_root / f"rdf-tests-{commit}"
    lock_path = cache_root / ".ngkg-w3c-suite-fetch.lock"
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    lock_fd = os.open(lock_path, flags, 0o640)
    with os.fdopen(lock_fd, "r+") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        if destination.exists():
            verify_checkout(destination, repository, commit, manifests)
            print(destination)
            return 0
        if args.verify_only:
            raise ValueError(f"pinned W3C suite checkout is absent: {destination}")

        staging = cache_root / f".rdf-tests-{commit}-{uuid.uuid4()}"
        staging.mkdir(mode=0o750)
        try:
            run("git", "init", "--quiet", staging.as_posix())
            run("git", "remote", "add", "origin", repository, cwd=staging)
            run(
                "git",
                "-c",
                "protocol.version=2",
                "fetch",
                "--quiet",
                "--depth=1",
                "--no-tags",
                "origin",
                commit,
                cwd=staging,
            )
            run("git", "checkout", "--quiet", "--detach", "FETCH_HEAD", cwd=staging)
            verify_checkout(staging, repository, commit, manifests)
            os.replace(staging, destination)
            verify_checkout(destination, repository, commit, manifests)
        finally:
            if staging.exists():
                shutil.rmtree(staging)

    print(destination)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"W3C conformance suite error: {error}", file=sys.stderr)
        raise SystemExit(1)
