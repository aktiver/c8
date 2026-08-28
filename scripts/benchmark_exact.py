#!/usr/bin/env python3
"""Run exact NGKG versus certified-baseline SPARQL comparisons without excluding failures."""

from __future__ import annotations

import argparse
import collections
import concurrent.futures
import json
import math
import os
import pathlib
import statistics
import time
import urllib.error
import urllib.request

import yaml


def request_json(url: str, body: bytes, token: str, content_type: str, timeout: float) -> tuple[dict, float]:
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": content_type, "Accept": "application/sparql-results+json"},
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read()
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"{url} returned HTTP {error.code}: {error.read()[:4096]!r}") from error
    elapsed = time.perf_counter() - started
    return json.loads(payload), elapsed


def multiset(result: dict) -> collections.Counter[str]:
    bindings = result.get("results", {}).get("bindings")
    if not isinstance(bindings, list):
        raise RuntimeError("SPARQL result lacks results.bindings")
    return collections.Counter(json.dumps(binding, sort_keys=True, separators=(",", ":")) for binding in bindings)


def reset_cache(url: str, token: str, state: str, timeout: float) -> None:
    payload, _ = request_json(url, json.dumps({"cacheState": state}).encode(), token, "application/json", timeout)
    if payload.get("status") != "ready":
        raise RuntimeError(f"cache controller did not certify {state} readiness")


def ordered_sequence(result: dict) -> list[str]:
    bindings = result.get("results", {}).get("bindings")
    if not isinstance(bindings, list):
        raise RuntimeError("SPARQL result lacks results.bindings")
    return [json.dumps(binding, sort_keys=True, separators=(",", ":")) for binding in bindings]


def run_one(args: tuple[str, str, str, bool, bool, str, str, str, float]) -> dict[str, object]:
    query_id, query, expected_path, ordered, proof_required, ngkg_url, baseline_url, snapshot_id, timeout = args
    ngkg_token = os.environ["NGKG_BENCHMARK_NGKG_TOKEN"]
    baseline_token = os.environ["NGKG_BENCHMARK_BASELINE_TOKEN"]
    expected_result = json.loads(pathlib.Path(expected_path).read_text(encoding="utf-8"))
    expected = multiset(expected_result)
    baseline, baseline_seconds = request_json(baseline_url, query.encode(), baseline_token, "application/sparql-query", timeout)
    ngkg_request = json.dumps({
        "datasetId": os.environ["NGKG_BENCHMARK_DATASET_ID"],
        "snapshotId": snapshot_id,
        "query": query,
        "entailmentRegime": "owl2-direct",
        "timeoutMs": int(timeout * 1000),
        "includeProofs": True,
        "resultFormat": "sparql-results-json",
    }).encode()
    ngkg, ngkg_seconds = request_json(ngkg_url, ngkg_request, ngkg_token, "application/json", timeout)
    baseline_set = multiset(baseline)
    ngkg_set = multiset(ngkg)
    if baseline_set != expected or ngkg_set != expected or baseline_set != ngkg_set:
        raise RuntimeError(f"exact multiset inequality for {query_id}")
    if ordered and (ordered_sequence(baseline) != ordered_sequence(expected_result) or ordered_sequence(ngkg) != ordered_sequence(expected_result)):
        raise RuntimeError(f"ordered solution-sequence inequality for {query_id}")
    if proof_required:
        expected_proof = expected_result.get("ngkgProof")
        if expected_proof is None or baseline.get("ngkgProof") != expected_proof or ngkg.get("ngkgProof") != expected_proof:
            raise RuntimeError(f"proof expectation inequality for {query_id}")
    return {"queryId": query_id, "baselineSeconds": baseline_seconds, "ngkgSeconds": ngkg_seconds, "speedup": baseline_seconds / ngkg_seconds}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=pathlib.Path, default=pathlib.Path.cwd())
    parser.add_argument("--workload", type=pathlib.Path, required=True)
    parser.add_argument("--ngkg-url", required=True)
    parser.add_argument("--baseline-url", required=True)
    parser.add_argument("--ngkg-cache-control-url", required=True)
    parser.add_argument("--baseline-cache-control-url", required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=300.0)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    required_env = ["NGKG_BENCHMARK_NGKG_TOKEN", "NGKG_BENCHMARK_BASELINE_TOKEN", "NGKG_BENCHMARK_DATASET_ID"]
    missing = [name for name in required_env if not os.environ.get(name)]
    if missing:
        raise SystemExit(f"missing required environment: {', '.join(missing)}")
    root = args.root.resolve()
    workload = yaml.safe_load(args.workload.read_text(encoding="utf-8"))
    manifest_path = root / workload["spec"]["querySets"]["production"]["manifest"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    queries = []
    for item in manifest["queries"]:
        query = (root / item["query"]).read_text(encoding="utf-8")
        queries.append((item["id"], query, str(root / item["expected"]), bool(item["ordered"]), bool(item["proofRequired"]), args.ngkg_url, args.baseline_url, args.snapshot_id, args.timeout_seconds))
    trials: list[dict[str, object]] = []
    for state in workload["spec"]["cacheStates"]:
        reset_cache(args.ngkg_cache_control_url, os.environ["NGKG_BENCHMARK_NGKG_TOKEN"], state, args.timeout_seconds)
        reset_cache(args.baseline_cache_control_url, os.environ["NGKG_BENCHMARK_BASELINE_TOKEN"], state, args.timeout_seconds)
        for concurrency in workload["spec"]["concurrencyLevels"]:
            expanded = queries * concurrency
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
                results = list(pool.map(run_one, expanded))
            for result in results:
                result.update({"cacheState": state, "concurrency": concurrency})
                trials.append(result)
    speedups = [float(trial["speedup"]) for trial in trials]
    production_geomean = math.exp(statistics.fmean(math.log(value) for value in speedups))
    hot = [float(trial["speedup"]) for trial in trials if trial["cacheState"] == "hot"]
    report = {
        "workload": workload["metadata"]["name"],
        "snapshotId": args.snapshot_id,
        "allQueriesExact": True,
        "trials": trials,
        "productionGeometricMeanSpeedup": production_geomean,
        "hotMedianSpeedup": statistics.median(hot),
        "target20xMet": production_geomean >= 20.0,
        "target50xHotMet": statistics.median(hot) >= 50.0,
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if report["target20xMet"] and report["target50xHotMet"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
