#!/usr/bin/env python3
"""Shared, dependency-free primitives for Phase 6 controlled qualification."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import time
from typing import Any, Iterable

SHA_CHARS = frozenset("0123456789abcdef")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def valid_sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and set(value) <= SHA_CHARS


def _reject_duplicate_keys(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as stream:
        return json.load(stream, object_pairs_hook=_reject_duplicate_keys)


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical(value) + b"\n"
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def require_private_file(path: Path, label: str) -> None:
    require(path.is_file(), f"{label} is missing: {path}")
    mode = stat.S_IMODE(path.stat().st_mode)
    require(mode & 0o077 == 0, f"{label} must not be group/world accessible: {path}")


def resolve(base: Path, value: str) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (base / path).resolve()


def run(
    command: list[str],
    *,
    stdin: bytes | None = None,
    timeout: int = 600,
    maximum_stdout_bytes: int = 64 * 1024 * 1024,
) -> bytes:
    require(bool(command) and all(isinstance(item, str) and item for item in command), "invalid command")
    result = subprocess.run(
        command,
        input=stdin,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
        close_fds=True,
    )
    require(
        result.returncode == 0,
        f"command failed ({Path(command[0]).name}): "
        f"{result.stderr.decode('utf-8', errors='replace')[:4096]}",
    )
    require(len(result.stdout) <= maximum_stdout_bytes, "command output exceeded its byte ceiling")
    return result.stdout


def epoch_ms() -> int:
    return time.time_ns() // 1_000_000


def monotonic_ms() -> int:
    return time.monotonic_ns() // 1_000_000


def evidence_root(rows: Iterable[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for row in sorted(rows, key=lambda item: (str(item.get("provider", "")), str(item.get("id", "")))):
        digest.update(canonical(row))
        digest.update(b"\0")
    return digest.hexdigest()


class EvidenceRecorder:
    """Writes immutable per-scenario records before adding them to a run ledger."""

    def __init__(self, directory: Path, subject_sha256: str) -> None:
        require(valid_sha256(subject_sha256), "invalid qualification subject SHA-256")
        self.directory = directory.resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.subject_sha256 = subject_sha256
        self.rows: list[dict[str, Any]] = []

    def record(
        self,
        scenario_id: str,
        detail: dict[str, Any],
        started_epoch_ms: int,
        ended_epoch_ms: int,
    ) -> dict[str, Any]:
        require(scenario_id and all(ch.isalnum() or ch in "-_" for ch in scenario_id), "invalid scenario id")
        require(ended_epoch_ms >= started_epoch_ms, "scenario clock moved backwards")
        path = self.directory / f"{scenario_id}.json"
        require(not path.exists(), f"scenario evidence already exists: {scenario_id}")
        document = {
            "formatVersion": 1,
            "scenarioId": scenario_id,
            "subjectSha256": self.subject_sha256,
            "startedEpochMs": started_epoch_ms,
            "endedEpochMs": ended_epoch_ms,
            "elapsedMs": ended_epoch_ms - started_epoch_ms,
            "status": "PASS",
            "synthetic": False,
            "detail": detail,
        }
        atomic_json(path, document)
        row = {
            "id": scenario_id,
            "status": "PASS",
            "synthetic": False,
            "evidencePath": path.name,
            "evidenceSha256": sha256_file(path),
            "startedEpochMs": started_epoch_ms,
            "endedEpochMs": ended_epoch_ms,
        }
        self.rows.append(row)
        return row
