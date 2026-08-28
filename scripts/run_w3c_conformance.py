#!/usr/bin/env python3
"""Bounded, manifest-driven W3C RDF/SPARQL conformance executor for NGKG.

The harness executes the pinned W3C suite instead of treating its presence as
evidence.  It builds the native case driver once, fans independent cases out
across a cgroup-aware worker pool, prevents nested BLAS/OpenMP oversubscription,
and records unsupported test classes separately from failures.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import urllib.parse
from collections import Counter

try:
    import rdflib
    from rdflib import RDF, URIRef
    from rdflib.namespace import Namespace
    from rdflib.query import Result
except ImportError as exc:
    raise SystemExit("rdflib >=7,<8 is required for W3C manifest execution") from exc


MF = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")
QT = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-query#")
RDFT = Namespace("http://www.w3.org/ns/rdftest#")
RS = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/result-set#")
SD = Namespace("http://www.w3.org/ns/sparql-service-description#")
OWL_DIRECT = "http://www.w3.org/ns/entailment/OWL-Direct"

SUPPORTED = {
    str(RDFT.TestTrigPositiveSyntax): ("trig-syntax", True),
    str(RDFT.TestTrigNegativeSyntax): ("trig-syntax", False),
    str(RDFT.TestTrigEval): ("trig-evaluation", None),
    str(MF.PositiveSyntaxTest11): ("sparql-syntax", True),
    str(MF.NegativeSyntaxTest11): ("sparql-syntax", False),
    str(MF.QueryEvaluationTest): ("query-evaluation", None),
    str(MF.CSVResultFormatTest): ("csv-result-format", None),
}


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def cpu_set_count(value: str) -> int:
    """Counts Linux CPU-list syntax such as ``0-3,8,10-11``."""
    cpus: set[int] = set()
    for item in value.strip().split(","):
        item = item.strip()
        if not item:
            continue
        if "-" in item:
            start_text, end_text = item.split("-", 1)
            start, end = int(start_text), int(end_text)
            if start < 0 or end < start:
                raise ValueError(f"invalid CPU range {item!r}")
            cpus.update(range(start, end + 1))
        else:
            cpu = int(item)
            if cpu < 0:
                raise ValueError(f"invalid CPU {item!r}")
            cpus.add(cpu)
    if not cpus:
        raise ValueError("empty CPU set")
    return len(cpus)


def cgroup_cpu_limit(root: pathlib.Path = pathlib.Path("/sys/fs/cgroup")) -> int | None:
    """Returns the tightest cgroup CPU quota/cpuset visible to this process."""
    limits: list[int] = []
    cpu_max = root / "cpu.max"
    if cpu_max.is_file():
        quota, period, *_ = cpu_max.read_text(encoding="ascii").split()
        if quota != "max":
            limits.append(max(1, math.ceil(int(quota) / int(period))))
    for candidate in (root / "cpuset.cpus.effective", root / "cpuset.cpus"):
        if candidate.is_file():
            value = candidate.read_text(encoding="ascii").strip()
            if value:
                limits.append(cpu_set_count(value))
                break
    return min(limits) if limits else None


def available_cpus() -> int:
    candidates = [os.cpu_count() or 1]
    if hasattr(os, "sched_getaffinity"):
        candidates.append(len(os.sched_getaffinity(0)))
    cgroup = cgroup_cpu_limit()
    if cgroup is not None:
        candidates.append(cgroup)
    return max(1, min(candidates))


def default_jobs() -> int:
    # Keep one CPU for the coordinator/kubelet when the allocation is not tiny.
    cpus = available_cpus()
    return min(64, max(1, cpus - 1 if cpus > 2 else cpus))


def list_items(graph: rdflib.Graph, head: rdflib.term.Node) -> list[rdflib.term.Node]:
    values: list[rdflib.term.Node] = []
    seen: set[rdflib.term.Node] = set()
    node = head
    while node and node != RDF.nil:
        if node in seen:
            raise RuntimeError("cyclic RDF list in W3C manifest")
        seen.add(node)
        first = graph.value(node, RDF.first)
        rest = graph.value(node, RDF.rest)
        if first is None or rest is None:
            raise RuntimeError("malformed RDF list in W3C manifest")
        values.append(first)
        node = rest
    return values


def local_path(node: rdflib.term.Node, suite_root: pathlib.Path) -> pathlib.Path:
    uri = str(node)
    if uri.startswith("file:"):
        parsed = urllib.parse.urlparse(uri)
        if parsed.netloc not in {"", "localhost"}:
            raise RuntimeError(f"remote file authority is forbidden: {uri}")
        candidate = pathlib.Path(urllib.parse.unquote(parsed.path)).resolve()
    else:
        raw = pathlib.Path(uri)
        candidate = raw.resolve() if raw.is_absolute() else (suite_root / raw).resolve()
    try:
        candidate.relative_to(suite_root)
    except ValueError as exc:
        raise RuntimeError(f"manifest path escapes suite root: {candidate}") from exc
    if not candidate.is_file() or candidate.is_symlink():
        raise RuntimeError(f"manifest input is not a regular in-suite file: {candidate}")
    return candidate


def json_result_term(term: rdflib.term.Node) -> dict[str, str]:
    if isinstance(term, rdflib.URIRef):
        return {"type": "uri", "value": str(term)}
    if isinstance(term, rdflib.BNode):
        return {"type": "bnode", "value": str(term)}
    item = {"type": "literal", "value": str(term)}
    if getattr(term, "language", None):
        item["xml:lang"] = term.language
    elif getattr(term, "datatype", None):
        item["datatype"] = str(term.datatype)
    return item


def rdf_result_set(source: pathlib.Path) -> dict[str, object] | None:
    graph = rdflib.Graph()
    graph.parse(source.as_posix(), format="turtle")
    root = next(iter(graph.subjects(RDF.type, RS.ResultSet)), None)
    if root is None:
        return None
    boolean = graph.value(root, RS.boolean)
    if boolean is not None:
        return {"head": {}, "boolean": bool(boolean.toPython())}
    variables = [str(value) for value in graph.objects(root, RS.resultVariable)]
    solutions = list(graph.objects(root, RS.solution))
    solutions.sort(
        key=lambda solution: int(graph.value(solution, RS.index) or len(solutions))
    )
    rows: list[dict[str, dict[str, str]]] = []
    for solution in solutions:
        row: dict[str, dict[str, str]] = {}
        for binding in graph.objects(solution, RS.binding):
            variable = graph.value(binding, RS.variable)
            value = graph.value(binding, RS.value)
            if variable is None or value is None:
                raise RuntimeError(f"malformed RDF result-set binding in {source}")
            row[str(variable)] = json_result_term(value)
        rows.append(row)
    return {"head": {"vars": variables}, "results": {"bindings": rows}}


def normalize_result(source: pathlib.Path, destination: pathlib.Path) -> None:
    extension = source.suffix.lower()
    if extension in {".srj", ".json"}:
        destination.write_bytes(source.read_bytes())
        return
    result_format = {".srx": "xml", ".xml": "xml", ".csv": "csv", ".tsv": "tsv"}.get(
        extension
    )
    if result_format is None:
        if extension in {".ttl", ".turtle"}:
            rdf_result = rdf_result_set(source)
            if rdf_result is not None:
                destination.write_text(
                    json.dumps(rdf_result, separators=(",", ":")), encoding="utf-8"
                )
                return
        destination.write_bytes(source.read_bytes())
        return
    with source.open("rb") as stream:
        result = Result.parse(stream, format=result_format)
    if result.type == "ASK":
        payload = {"head": {}, "boolean": bool(result.askAnswer)}
    else:
        variables = [str(variable) for variable in result.vars]
        rows: list[dict[str, dict[str, str]]] = []
        for binding in result.bindings:
            row: dict[str, dict[str, str]] = {}
            for variable, term in binding.items():
                row[str(variable)] = json_result_term(term)
            rows.append(row)
        payload = {"head": {"vars": variables}, "results": {"bindings": rows}}
    destination.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")


def action_parts(
    graph: rdflib.Graph, action: rdflib.term.Node, suite_root: pathlib.Path
) -> tuple[pathlib.Path | None, list[pathlib.Path], list[dict[str, str]]]:
    if isinstance(action, URIRef):
        return local_path(action, suite_root), [], []
    query = graph.value(action, QT.query)
    defaults = [local_path(item, suite_root) for item in graph.objects(action, QT.data)]
    named: list[dict[str, str]] = []
    for item in graph.objects(action, QT.graphData):
        if isinstance(item, URIRef):
            named.append({"path": str(local_path(item, suite_root)), "graphIri": str(item)})
        else:
            data = graph.value(item, QT.data)
            name = graph.value(item, QT.graph)
            if data is None:
                raise RuntimeError("graphData entry lacks qt:data")
            named.append(
                {"path": str(local_path(data, suite_root)), "graphIri": str(name or data)}
            )
    return (local_path(query, suite_root) if query else None), defaults, named


def assumed_test_base(graph: rdflib.Graph) -> str | None:
    value = next(iter(graph.objects(None, MF.assumedTestBase)), None)
    return str(value) if value is not None else None


def entailment_regimes(graph: rdflib.Graph, action: rdflib.term.Node | None) -> list[str]:
    if action is None or isinstance(action, URIRef):
        return []
    value = graph.value(action, SD.entailmentRegime)
    if value is None:
        return []
    if (value, RDF.first, None) in graph:
        return [str(item) for item in list_items(graph, value)]
    return [str(value)]


def load_manifest(
    path: pathlib.Path, suite_root: pathlib.Path, seen: set[pathlib.Path]
) -> list[tuple[rdflib.Graph, rdflib.term.Node, pathlib.Path]]:
    path = path.resolve()
    try:
        path.relative_to(suite_root)
    except ValueError as exc:
        raise RuntimeError(f"manifest escapes suite root: {path}") from exc
    if path in seen:
        return []
    if not path.is_file() or path.is_symlink():
        raise RuntimeError(f"manifest is not a regular in-suite file: {path}")
    seen.add(path)
    graph = rdflib.Graph()
    graph.parse(path.as_posix(), format="turtle")
    tests: list[tuple[rdflib.Graph, rdflib.term.Node, pathlib.Path]] = []
    for manifest in graph.subjects(RDF.type, MF.Manifest):
        entries = graph.value(manifest, MF.entries)
        if entries:
            for test in list_items(graph, entries):
                tests.append((graph, test, path))
        for include in graph.objects(manifest, MF.include):
            for item in list_items(graph, include):
                tests.extend(load_manifest(local_path(item, suite_root), suite_root, seen))
    return tests


def bounded_environment() -> dict[str, str]:
    environment = os.environ.copy()
    for name in (
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "BLIS_NUM_THREADS",
        "VECLIB_MAXIMUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "RAYON_NUM_THREADS",
    ):
        environment[name] = "1"
    return environment


def run_case(
    driver: list[str],
    case: dict[str, object],
    work: pathlib.Path,
    timeout_seconds: float,
    max_output_bytes: int,
) -> tuple[str, str, int | None, int, int]:
    case_path = work / "case.json"
    stdout_path = work / "stdout.log"
    stderr_path = work / "stderr.log"
    case_path.write_text(json.dumps(case, indent=2), encoding="utf-8")
    try:
        with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
            completed = subprocess.run(
                driver + [str(case_path)],
                stdout=stdout,
                stderr=stderr,
                timeout=timeout_seconds,
                env=bounded_environment(),
                check=False,
            )
    except subprocess.TimeoutExpired:
        return "fail", f"driver timed out after {timeout_seconds:g} seconds", None, 0, 0
    stdout_size = stdout_path.stat().st_size
    stderr_size = stderr_path.stat().st_size
    if stdout_size > max_output_bytes or stderr_size > max_output_bytes:
        return (
            "fail",
            f"driver output exceeded {max_output_bytes} byte ceiling",
            completed.returncode,
            stdout_size,
            stderr_size,
        )
    stdout_text = stdout_path.read_text(encoding="utf-8", errors="replace").strip()
    stderr_text = stderr_path.read_text(encoding="utf-8", errors="replace").strip()
    status, message = "fail", ""
    if stdout_text:
        try:
            value = json.loads(stdout_text.splitlines()[-1])
            status = value.get("status", "fail")
            message = value.get("message", "")
        except json.JSONDecodeError:
            message = stdout_text
    if completed.returncode != 0 and not message:
        message = stderr_text or f"driver exit {completed.returncode}"
    return status, message, completed.returncode, stdout_size, stderr_size


def atomic_write_json(path: pathlib.Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = pathlib.Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--suite-root", type=pathlib.Path, required=True)
    parser.add_argument(
        "--lock",
        type=pathlib.Path,
        default=pathlib.Path("conformance/w3c-rdf-tests.lock.json"),
    )
    parser.add_argument("--driver", nargs="+", default=["target/debug/ngkg-w3c-case"])
    parser.add_argument(
        "--entailment-driver",
        nargs="+",
        help="online OWL-Direct case driver; required to execute applicable entailment cases",
    )
    parser.add_argument("--report", type=pathlib.Path, required=True)
    parser.add_argument("--fail-on-unsupported", action="store_true")
    parser.add_argument("--manifest", action="append", default=[])
    parser.add_argument("--inventory-only", action="store_true")
    parser.add_argument("--jobs", type=int, default=default_jobs())
    parser.add_argument("--case-timeout-seconds", type=float, default=120.0)
    parser.add_argument("--max-driver-output-bytes", type=int, default=1024 * 1024)
    args = parser.parse_args()
    if args.jobs < 1 or args.jobs > 256:
        parser.error("--jobs must be in [1, 256]")
    if args.case_timeout_seconds <= 0:
        parser.error("--case-timeout-seconds must be positive")
    if args.max_driver_output_bytes < 1024:
        parser.error("--max-driver-output-bytes must be at least 1024")

    started = time.monotonic()
    lock_path = args.lock.resolve()
    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    manifest_names = args.manifest or lock["requiredManifests"]
    suite_root = args.suite_root.resolve()
    all_tests: list[tuple[rdflib.Graph, rdflib.term.Node, pathlib.Path]] = []
    seen: set[pathlib.Path] = set()
    for relative in manifest_names:
        all_tests.extend(load_manifest(suite_root / relative, suite_root, seen))

    counts: Counter[str] = Counter()
    type_counts: Counter[str] = Counter()
    kind_counts: Counter[str] = Counter()
    records: list[dict[str, object]] = []
    tasks: list[tuple[int, dict[str, object], pathlib.Path, list[str]]] = []
    with tempfile.TemporaryDirectory(prefix="ngkg-w3c-") as temporary_name:
        work = pathlib.Path(temporary_name)
        for index, (graph, test, manifest) in enumerate(all_tests):
            types = sorted(str(value) for value in graph.objects(test, RDF.type))
            type_counts.update(types)
            supported = next((SUPPORTED[value] for value in types if value in SUPPORTED), None)
            name = str(graph.value(test, MF.name) or test)
            action = graph.value(test, MF.action)
            record: dict[str, object] = {
                "index": index,
                "id": str(test),
                "name": name,
                "manifest": str(manifest.relative_to(suite_root)),
                "types": types,
            }
            if supported is None:
                counts["unsupported"] += 1
                record["status"] = "unsupported"
                records.append(record)
                continue
            kind, expect_parse = supported
            regimes = entailment_regimes(graph, action)
            if regimes:
                record["entailmentRegimes"] = regimes
                if OWL_DIRECT not in regimes:
                    counts["unsupported"] += 1
                    record.update(
                        {
                            "status": "unsupported",
                            "unsupportedReason": "test does not declare OWL Direct Semantics",
                        }
                    )
                    records.append(record)
                    continue
                if not args.entailment_driver:
                    counts["unsupported"] += 1
                    record.update(
                        {
                            "status": "unsupported",
                            "unsupportedReason": "--entailment-driver is required for OWL Direct execution",
                        }
                    )
                    records.append(record)
                    continue
                kind = "owl-direct-query-evaluation"
            kind_counts[kind] += 1
            if args.inventory_only:
                counts["inventory"] += 1
                record.update({"kind": kind, "status": "inventory"})
                records.append(record)
                continue
            result = graph.value(test, MF.result)
            case: dict[str, object] = {
                "kind": kind,
                "action": None,
                "baseIri": None,
                "query": None,
                "defaultData": [],
                "namedData": [],
                "expected": None,
                "expectedParseSuccess": expect_parse,
                "entailmentRegime": OWL_DIRECT if regimes else None,
            }
            case_dir = work / f"{index:06d}"
            case_dir.mkdir()
            try:
                if action is None:
                    raise RuntimeError("test has no mf:action")
                if kind in {"trig-syntax", "sparql-syntax"}:
                    action_path = local_path(action, suite_root)
                    case["action"] = str(action_path)
                    assumed_base = assumed_test_base(graph)
                    case["baseIri"] = (
                        urllib.parse.urljoin(
                            assumed_base, action_path.relative_to(manifest.parent).as_posix()
                        )
                        if assumed_base is not None
                        else action_path.as_uri()
                    )
                elif kind == "trig-evaluation":
                    if result is None:
                        raise RuntimeError("TriG evaluation test has no mf:result")
                    action_path = local_path(action, suite_root)
                    case["action"] = str(action_path)
                    assumed_base = assumed_test_base(graph)
                    case["baseIri"] = (
                        urllib.parse.urljoin(
                            assumed_base, action_path.relative_to(manifest.parent).as_posix()
                        )
                        if assumed_base is not None
                        else action_path.as_uri()
                    )
                    case["expected"] = str(local_path(result, suite_root))
                else:
                    query, defaults, named = action_parts(graph, action, suite_root)
                    if query is None:
                        raise RuntimeError("query test has no query action")
                    if result is None:
                        raise RuntimeError("query test has no mf:result")
                    case["query"] = str(query)
                    assumed_base = assumed_test_base(graph)
                    case["baseIri"] = (
                        urllib.parse.urljoin(
                            assumed_base, query.relative_to(manifest.parent).as_posix()
                        )
                        if assumed_base is not None
                        else query.as_uri()
                    )
                    case["defaultData"] = [str(value) for value in defaults]
                    case["namedData"] = named
                    expected = local_path(result, suite_root)
                    if kind == "csv-result-format":
                        case["expected"] = str(expected)
                    else:
                        normalized = case_dir / (
                            "expected.json"
                            if expected.suffix.lower()
                            in {".srx", ".srj", ".json", ".xml", ".csv", ".tsv"}
                            else expected.name
                        )
                        normalize_result(expected, normalized)
                        case["expected"] = str(normalized)
                record.update(
                    {"kind": kind, "status": "pending", "baseIri": case["baseIri"]}
                )
                records.append(record)
                tasks.append(
                    (
                        index,
                        case,
                        case_dir,
                        list(args.entailment_driver) if regimes else list(args.driver),
                    )
                )
            except Exception as exc:  # noqa: BLE001 - every case must become evidence
                counts["fail"] += 1
                record.update(
                    {"kind": kind, "status": "fail", "message": str(exc), "driverExit": None}
                )
                records.append(record)

        def execute(
            task: tuple[int, dict[str, object], pathlib.Path, list[str]]
        ) -> tuple[int, dict[str, object]]:
            index, case, case_dir, driver = task
            case_started = time.monotonic()
            status, message, code, stdout_bytes, stderr_bytes = run_case(
                driver,
                case,
                case_dir,
                args.case_timeout_seconds,
                args.max_driver_output_bytes,
            )
            return index, {
                "status": status,
                "message": message,
                "driverExit": code,
                "stdoutBytes": stdout_bytes,
                "stderrBytes": stderr_bytes,
                "durationMs": round((time.monotonic() - case_started) * 1000, 3),
            }

        if tasks:
            record_by_index = {int(record["index"]): record for record in records}
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as executor:
                for index, outcome in executor.map(execute, tasks):
                    record_by_index[index].update(outcome)
                    counts[str(outcome["status"])] += 1

    records.sort(key=lambda record: int(record["index"]))
    report = {
        "formatVersion": 2,
        "qualification": "w3c-rdf-tests-execution",
        "suiteRoot": str(suite_root),
        "suiteCommit": lock.get("commit"),
        "lockSha256": sha256(lock_path),
        "manifests": manifest_names,
        "inventoryOnly": args.inventory_only,
        "executor": {
            "jobs": args.jobs,
            "availableCpus": available_cpus(),
            "caseTimeoutSeconds": args.case_timeout_seconds,
            "maxDriverOutputBytes": args.max_driver_output_bytes,
            "nestedNativeThreadsPerCase": 1,
        },
        "inventory": {
            "total": len(all_tests),
            "byType": dict(sorted(type_counts.items())),
            "byKind": dict(sorted(kind_counts.items())),
        },
        "summary": dict(sorted(counts.items())),
        "durationMs": round((time.monotonic() - started) * 1000, 3),
        "tests": records,
    }
    atomic_write_json(args.report, report)
    print(json.dumps(report["summary"], sort_keys=True))
    if counts["fail"] or (args.fail_on_unsupported and counts["unsupported"]):
        return 1
    if not args.inventory_only and counts["pass"] == 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
