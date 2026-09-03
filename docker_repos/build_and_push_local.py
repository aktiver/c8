#!/usr/bin/env python3
"""Build all NGKG images, push to a local registry, and emit digest-pinned Helm values."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any

DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


def fail(message: str) -> None:
    raise RuntimeError(message)


def run(command: list[str], *, capture: bool = False) -> str:
    result = subprocess.run(
        command, check=False, text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )
    if result.returncode:
        diagnostic = (result.stderr or "")[-2000:]
        password = os.environ.get("NGKG_LOCAL_REGISTRY_PASSWORD", "")
        if password:
            diagnostic = diagnostic.replace(password, "[REDACTED]")
        fail(f"{command[0]} failed with exit {result.returncode}: {diagnostic}")
    return (result.stdout or "").strip()


def require_digest_reference(name: str) -> str:
    value = os.environ.get(name, "")
    if "@sha256:" not in value or not DIGEST.fullmatch(value.rsplit("@", 1)[1]):
        fail(f"{name} must be an immutable image reference containing @sha256:<64 lowercase hex>")
    return value


def set_path(target: dict[str, Any], dotted: str, repository: str, digest: str) -> None:
    cursor = target
    parts = dotted.split(".")
    for part in parts[:-1]:
        cursor = cursor.setdefault(part, {})
    cursor[parts[-1]] = {"repository": repository, "digest": digest, "pullPolicy": "IfNotPresent"}


def dump_yaml(value: Any, indent: int = 0) -> str:
    lines: list[str] = []
    for key, item in value.items():
        prefix = " " * indent + f"{key}:"
        if isinstance(item, dict):
            lines.append(prefix)
            lines.append(dump_yaml(item, indent + 2))
        else:
            lines.append(prefix + " " + json.dumps(item))
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", default=os.environ.get("NGKG_LOCAL_REGISTRY", "localhost:5000"))
    parser.add_argument("--namespace", default=os.environ.get("NGKG_LOCAL_REGISTRY_NAMESPACE", "ngkg"))
    parser.add_argument("--engine", default=os.environ.get("NGKG_CONTAINER_ENGINE", "docker"))
    parser.add_argument("--platform", default=os.environ.get("NGKG_BUILD_PLATFORM", "linux/amd64"))
    parser.add_argument("--output", type=Path, default=Path("docker_repos/generated"))
    parser.add_argument("--push", action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    catalog = json.loads((root / "docker_repos/images.json").read_text(encoding="utf-8"))
    build_args = {
        "BUILD_OFFLINE": os.environ.get("NGKG_BUILD_OFFLINE", "false"),
        "RUST_BUILDER_IMAGE": require_digest_reference("NGKG_RUST_BUILDER_IMAGE"),
        "RUNTIME_IMAGE": require_digest_reference("NGKG_RUNTIME_IMAGE"),
        "MAVEN_BUILDER_IMAGE": require_digest_reference("NGKG_MAVEN_BUILDER_IMAGE"),
        "JAVA_RUNTIME_IMAGE": require_digest_reference("NGKG_JAVA_RUNTIME_IMAGE"),
        "VLLM_SOURCE_IMAGE": require_digest_reference("NGKG_VLLM_SOURCE_IMAGE"),
        "MPI_BUILDER_IMAGE": require_digest_reference("NGKG_MPI_BUILDER_IMAGE"),
        "MPI_RUNTIME_IMAGE": require_digest_reference("NGKG_MPI_RUNTIME_IMAGE"),
    }
    if build_args["BUILD_OFFLINE"] not in {"true", "false"}:
        fail("NGKG_BUILD_OFFLINE must be exactly true or false")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    helm: dict[str, dict[str, Any]] = {"platform": {}, "workloads": {}, "agents": {}}
    lock: list[dict[str, str]] = []
    for image in catalog["images"]:
        repository = f"{args.registry.rstrip('/')}/{args.namespace.strip('/')}/{image['name']}"
        tag = os.environ.get("NGKG_LOCAL_IMAGE_TAG", "enterprise-remediation-phase8")
        reference = f"{repository}:{tag}"
        command = [
            args.engine, "buildx", "build", "--platform", args.platform,
            "--file", str(root / "docker_repos" / image["dockerfile"]),
            "--tag", reference,
        ]
        for key, value in build_args.items():
            command.extend(["--build-arg", f"{key}={value}"])
        command.append("--push" if args.push else "--load")
        command.append(str(root))
        run(command)
        if not args.push:
            fail("digest-pinned Helm values require --push to the configured local registry")
        raw = run([args.engine, "buildx", "imagetools", "inspect", "--format", "{{json .Manifest.Digest}}", reference], capture=True)
        digest = json.loads(raw)
        if not isinstance(digest, str) or not DIGEST.fullmatch(digest):
            fail(f"registry returned an invalid digest for {reference}: {digest!r}")
        lock.append({"name": image["name"], "repository": repository, "digest": digest, "reference": f"{repository}@{digest}"})
        for chart, path in image["helm"]:
            set_path(helm[chart], path, repository, digest)
    (output / "image-lock.json").write_text(json.dumps({"formatVersion": 1, "images": lock}, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    for chart, values in helm.items():
        (output / f"{chart}-local-registry-values.yaml").write_text(dump_yaml(values) + "\n", encoding="utf-8")
    print(json.dumps({"status": "PASS", "registry": args.registry, "imageCount": len(lock), "output": str(output)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, RuntimeError) as error:
        print(f"local image build failed: {error}", file=sys.stderr)
        raise SystemExit(1)
