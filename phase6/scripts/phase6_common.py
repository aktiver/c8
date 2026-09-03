#!/usr/bin/env python3
"""Shared, dependency-free primitives for Phase 6 controlled qualification."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile
import time
from typing import Any, Iterable

SHA_CHARS = frozenset("0123456789abcdef")
_SECRET_PATTERNS = (
    re.compile(r"(?i)(authorization\s*:\s*(?:bearer|basic)\s+)[^\s,;]+"),
    re.compile(r"(?i)(access[_-]?token|refresh[_-]?token|client[_-]?secret|password|passwd|api[_-]?key)\s*[=:]\s*[^\s,;]+"),
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"(?i)([?&](?:sig|signature|token|se|sp|sv|x-amz-signature|x-goog-signature)=)[^&\s]+"),
    re.compile(r"(?i)(https?://)[^/@\s]+:[^/@\s]+@"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----"),
)


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


def load_json_bytes(payload: bytes) -> Any:
    return json.loads(payload.decode("utf-8"), object_pairs_hook=_reject_duplicate_keys)


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


def redact_diagnostic(value: bytes, maximum_chars: int = 4096) -> tuple[str, str]:
    """Return a bounded, secret-redacted diagnostic and the hash of the raw bytes."""
    text = value.decode("utf-8", errors="replace")
    for pattern in _SECRET_PATTERNS:
        text = pattern.sub(lambda match: f"{match.group(1) if match.lastindex else ''}[REDACTED]", text)
    return text[:maximum_chars], sha256_bytes(value)


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
    diagnostic, diagnostic_sha256 = redact_diagnostic(result.stderr)
    require(
        result.returncode == 0,
        f"command failed ({Path(command[0]).name}, exit={result.returncode}, "
        f"stderrSha256={diagnostic_sha256}): {diagnostic}",
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


def config_root_sha256(root: Path) -> str:
    """Hash a complete config tree without the self-referential declared root."""
    digest = hashlib.sha256(b"ngkg-phase6-config-root-v1\0")
    files = sorted(path for path in root.rglob("*") if path.is_file())
    require(files, "configuration root is empty")
    for path in files:
        require(not path.is_symlink(), f"configuration contains a symlink: {path}")
        relative = path.relative_to(root).as_posix()
        if relative == "release.json":
            release = load_json(path)
            release.pop("configRootSha256", None)
            payload = canonical(release)
        else:
            payload = path.read_bytes()
        logical = relative.encode("utf-8")
        digest.update(len(logical).to_bytes(4, "big"))
        digest.update(logical)
        digest.update(hashlib.sha256(payload).digest())
    return digest.hexdigest()


class EvidenceRecorder:
    """Append-only scenario attempts with durable STARTED and terminal records."""

    def __init__(self, directory: Path, subject_sha256: str) -> None:
        require(valid_sha256(subject_sha256), "invalid qualification subject SHA-256")
        self.directory = directory.resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.subject_sha256 = subject_sha256
        self.rows: list[dict[str, Any]] = []

    def begin(self, scenario_id: str, started_epoch_ms: int) -> dict[str, Any]:
        require(scenario_id and all(ch.isalnum() or ch in "-_" for ch in scenario_id), "invalid scenario id")
        scenario_dir = self.directory / scenario_id
        scenario_dir.mkdir(parents=True, exist_ok=True)
        attempt = max(
            (int(item.name) for item in scenario_dir.iterdir() if item.is_dir() and item.name.isdigit()),
            default=0,
        ) + 1
        attempt_dir = scenario_dir / f"{attempt:06d}"
        attempt_dir.mkdir(mode=0o700)
        started_path = attempt_dir / "started.json"
        atomic_json(started_path, {
            "formatVersion": 1, "scenarioId": scenario_id, "attempt": attempt,
            "subjectSha256": self.subject_sha256,
            "startedEpochMs": started_epoch_ms, "state": "STARTED",
        })
        return {
            "scenarioId": scenario_id, "attempt": attempt, "attemptDir": attempt_dir,
            "startedPath": started_path, "startedEpochMs": started_epoch_ms,
        }

    def complete(self, attempt: dict[str, Any], detail: dict[str, Any], ended_epoch_ms: int) -> dict[str, Any]:
        scenario_id = attempt["scenarioId"]
        started_epoch_ms = int(attempt["startedEpochMs"])
        require(ended_epoch_ms >= started_epoch_ms, "scenario clock moved backwards")
        path = attempt["attemptDir"] / "terminal.json"
        require(not path.exists(), f"scenario attempt is already terminal: {scenario_id}")
        document = {
            "formatVersion": 1, "scenarioId": scenario_id, "attempt": attempt["attempt"],
            "subjectSha256": self.subject_sha256,
            "startedEpochMs": started_epoch_ms, "endedEpochMs": ended_epoch_ms,
            "elapsedMs": ended_epoch_ms - started_epoch_ms,
            "state": "PASS", "status": "PASS", "synthetic": False, "detail": detail,
        }
        atomic_json(path, document)
        row = {
            "id": scenario_id, "attempt": attempt["attempt"], "status": "PASS", "synthetic": False,
            "startedEvidencePath": attempt["startedPath"].relative_to(self.directory).as_posix(),
            "startedEvidenceSha256": sha256_file(attempt["startedPath"]),
            "evidencePath": path.relative_to(self.directory).as_posix(),
            "evidenceSha256": sha256_file(path),
            "startedEpochMs": started_epoch_ms, "endedEpochMs": ended_epoch_ms,
        }
        self.rows.append(row)
        return row

    def fail_attempt(self, attempt: dict[str, Any], error: BaseException, ended_epoch_ms: int, state: str = "FAIL") -> dict[str, Any]:
        require(state in {"FAIL", "CANCELLED", "TIMED_OUT"}, "invalid terminal state")
        path = attempt["attemptDir"] / "terminal.json"
        require(not path.exists(), "scenario attempt is already terminal")
        diagnostic, diagnostic_sha256 = redact_diagnostic(str(error).encode("utf-8", errors="replace"))
        atomic_json(path, {
            "formatVersion": 1, "scenarioId": attempt["scenarioId"], "attempt": attempt["attempt"],
            "subjectSha256": self.subject_sha256,
            "startedEpochMs": attempt["startedEpochMs"], "endedEpochMs": ended_epoch_ms,
            "elapsedMs": max(0, ended_epoch_ms - int(attempt["startedEpochMs"])),
            "state": state, "status": state, "synthetic": False,
            "error": {"diagnostic": diagnostic, "diagnosticSha256": diagnostic_sha256},
        })
        row = {
            "id": attempt["scenarioId"], "attempt": attempt["attempt"], "status": state, "synthetic": False,
            "evidencePath": path.relative_to(self.directory).as_posix(),
            "evidenceSha256": sha256_file(path),
            "startedEpochMs": attempt["startedEpochMs"], "endedEpochMs": ended_epoch_ms,
        }
        self.rows.append(row)
        return row

    def record(
        self,
        scenario_id: str,
        detail: dict[str, Any],
        started_epoch_ms: int,
        ended_epoch_ms: int,
    ) -> dict[str, Any]:
        return self.complete(self.begin(scenario_id, started_epoch_ms), detail, ended_epoch_ms)
