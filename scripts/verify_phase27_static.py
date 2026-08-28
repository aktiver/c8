#!/usr/bin/env python3
"""Fail-closed static inspection for Phase 27 bounded admission."""

from __future__ import annotations

import pathlib
import sys

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(path: str, tokens: tuple[str, ...] = ()) -> str:
    target = ROOT / path
    if not target.is_file():
        raise RuntimeError(f"missing required file: {path}")
    text = target.read_text(encoding="utf-8")
    for token in tokens:
        if token not in text:
            raise RuntimeError(f"{path} is missing required token: {token}")
    return text


def main() -> int:
    serving = require(
        "services/online-serving/src/main.rs",
        (
            "struct AdmissionController",
            "struct AdmissionLease",
            "async fn admission_middleware",
            "acquire_before",
            "hold_admission_through_body",
            "StatusCode::TOO_MANY_REQUESTS",
            'HeaderValue::from_static("1")',
            'route("/metrics", get(metrics))',
            "ngkg_admission_in_flight",
            "ngkg_admission_pending",
            "ngkg_shuffle_cache_events_total",
            "admission_is_bounded_and_releases_after_response_body",
            "admission_pending_queue_has_a_hard_count_bound",
            "admission_overload_is_explicitly_retryable",
        ),
    )
    middleware = serving.index("async fn admission_middleware")
    authentication = serving.index("state.authorizer.authorize(request.headers())", middleware)
    next_run = serving.index("let response = next.run(request).await", authentication)
    wrap = serving.index("hold_admission_through_body(response, lease)", next_run)
    if not middleware < authentication < next_run < wrap:
        raise RuntimeError("admission permit is not retained through the response body")
    startup = serving.index("let admission = Arc::new(AdmissionController::new")
    router = serving.index("middleware::from_fn_with_state", startup)
    if startup > router:
        raise RuntimeError("admission middleware is not wired into the production router")

    openapi = yaml.safe_load(require("api/online-openapi.yaml"))
    if "/metrics" not in openapi["paths"]:
        raise RuntimeError("OpenAPI omits the metrics endpoint")
    for path, operations in openapi["paths"].items():
        if path.startswith("/v1/"):
            operation = next(iter(operations.values()))
            if "429" not in operation["responses"]:
                raise RuntimeError(f"{path} omits bounded-overload response")

    values = yaml.safe_load(require("charts/ngkg-workloads/values.yaml"))
    online = values["onlineServing"]
    required = {
        "maxQueryInFlight",
        "maxFragmentWorkerInFlight",
        "maxFragmentInFlight",
        "maxShuffleInFlight",
        "maxLocatorInFlight",
        "maxHydrationInFlight",
        "maxQueryPending",
        "maxFragmentPending",
        "maxShufflePending",
        "maxLocatorPending",
        "maxHydrationPending",
        "admissionWaitMilliseconds",
    }
    if not required.issubset(online):
        raise RuntimeError("Helm values omit an admission ceiling")
    if values["metrics"]["cpuUtilizationTargetPercent"] > 80 or values["metrics"]["memoryUtilizationTargetPercent"] > 80:
        raise RuntimeError("resource autoscaling exceeds the 80-percent boundary")
    require(
        "charts/ngkg-workloads/templates/network-policies.yaml",
        ("ngkg-metrics-client-ingress", "ngkg.io/metrics-client"),
    )
    require(
        "scripts/qualify_phase27.sh",
        ("ADMISSION_CAPACITY_EXHAUSTED", "ngkg_admission_in_flight", "cmp"),
    )
    require(
        "docs/phases/PHASE_27.md",
        ("Acceptance criteria", "Intentional boundary", "Parquet", "mmap", "OpenMP"),
    )
    require("verification/phase-27.json")
    print("Phase 27 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, RuntimeError, TypeError, ValueError) as error:
        print(f"phase 27 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
