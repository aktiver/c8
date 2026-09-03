#!/usr/bin/env python3
"""Fail-closed evidence path, hash, subject, and Sigstore verification."""
from __future__ import annotations

import os
import hashlib
from pathlib import Path, PurePosixPath
import stat
from typing import Any

from phase6_common import load_json, load_json_bytes, require, run, sha256_bytes, valid_sha256


def safe_evidence_path(root: Path, relative: str) -> Path:
    require(isinstance(relative, str) and relative, "empty evidence path")
    logical = PurePosixPath(relative)
    require(not logical.is_absolute() and ".." not in logical.parts and all(part not in {"", "."} for part in logical.parts), "unsafe evidence path")
    candidate = root.joinpath(*logical.parts)
    current = root
    for component in logical.parts:
        current = current / component
        metadata = os.lstat(current)
        require(not stat.S_ISLNK(metadata.st_mode), f"evidence path contains a symlink: {relative}")
    require(candidate.is_file() and stat.S_ISREG(os.lstat(candidate).st_mode), f"evidence is not a regular file: {relative}")
    return candidate


def secure_read(root: Path, relative: str, maximum_bytes: int = 256 * 1024 * 1024) -> bytes:
    """Open each path component beneath root with O_NOFOLLOW and read one descriptor."""
    require(isinstance(relative, str) and relative, "empty evidence path")
    logical = PurePosixPath(relative)
    require(not logical.is_absolute() and ".." not in logical.parts and all(part not in {"", "."} for part in logical.parts), "unsafe evidence path")
    descriptor = os.open(root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        for component in logical.parts[:-1]:
            next_descriptor = os.open(
                component,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = next_descriptor
        file_descriptor = os.open(
            logical.parts[-1],
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=descriptor,
        )
        try:
            metadata = os.fstat(file_descriptor)
            require(stat.S_ISREG(metadata.st_mode), f"evidence is not a regular file: {relative}")
            require(metadata.st_size <= maximum_bytes, f"evidence exceeds byte ceiling: {relative}")
            chunks = []
            total = 0
            while True:
                block = os.read(file_descriptor, min(1024 * 1024, maximum_bytes + 1 - total))
                if not block:
                    break
                total += len(block)
                require(total <= maximum_bytes, f"evidence exceeds byte ceiling: {relative}")
                chunks.append(block)
            return b"".join(chunks)
        finally:
            os.close(file_descriptor)
    finally:
        os.close(descriptor)


def verify_reference(root: Path, reference: dict[str, Any], subject: str, expected_id: str | None = None) -> dict[str, Any]:
    logical_id = reference.get("id")
    require(isinstance(logical_id, str) and logical_id, "evidence reference has no ID")
    if expected_id is not None:
        require(logical_id == expected_id, f"evidence ID mismatch: {logical_id}")
    expected_hash = reference.get("evidenceSha256", reference.get("sha256"))
    require(valid_sha256(expected_hash), f"invalid evidence hash for {logical_id}")
    payload = secure_read(root, reference["evidencePath"])
    require(sha256_bytes(payload) == expected_hash, f"evidence hash mismatch for {logical_id}")
    document = load_json_bytes(payload)
    require(document.get("subjectSha256") == subject, f"evidence subject mismatch for {logical_id}")
    observed_id = document.get("scenarioId", document.get("id", logical_id))
    require(observed_id == logical_id, f"evidence document ID mismatch for {logical_id}")
    return document


def verify_signed_statement(
    statement: Path,
    bundle: Path,
    *,
    subject: str,
    identity_regexp: str,
    oidc_issuer: str,
    cosign: str = "cosign",
) -> dict[str, Any]:
    require(statement.is_file() and bundle.is_file(), "signed statement or Sigstore bundle is missing")
    document = load_json(statement)
    require(document.get("subjectSha256") == subject, f"signed statement subject mismatch: {statement.name}")
    signature = document.get("signature", {})
    require(
        signature.get("bundleSha256") == hashlib.sha256(bundle.read_bytes()).hexdigest(),
        f"asserted Sigstore bundle hash mismatch: {statement.name}",
    )
    run([
        cosign, "verify-blob", "--bundle", str(bundle),
        "--certificate-identity-regexp", identity_regexp,
        "--certificate-oidc-issuer", oidc_issuer,
        str(statement),
    ], timeout=600)
    return document
