#!/usr/bin/env python3
"""Independently evaluate the checked three-hop OWL property-chain corpus.

This deliberately small evaluator is not the NGKG runtime and makes no general
OWL claim. It independently proves that the release fixture really needs facts
from multiple named domains and that its declared OWL property chain produces
the exact canonical context graph expected by the release prerequisite.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re

BASE = "https://c8-next-generation.io/ontology/release/"


def iri(local: str) -> str:
    return f"<{BASE}{local}>"


def triple(subject: str, predicate: str, obj: str) -> str:
    return f"{iri(subject)} {iri(predicate)} {iri(obj)} ."


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trig", type=pathlib.Path, required=True)
    parser.add_argument("--expected", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    source = args.trig.read_text(encoding="utf-8")
    graph_blocks = re.findall(r"<(https://c8-next-generation\.io/[^>]+/semkg)>\s*\{(.*?)\}", source, re.DOTALL)
    if len(graph_blocks) < 3 or len({name.rsplit("/", 2)[-2] for name, _ in graph_blocks}) < 3:
        raise ValueError("fixture does not span at least three named semantic domains")
    chain_match = re.search(r"owl:propertyChainAxiom\s*\(\s*((?:ex:[A-Za-z0-9_-]+\s*)+)\)", source)
    target_match = re.search(r"ex:([A-Za-z0-9_-]+)\s+a\s+owl:ObjectProperty\s*;\s*owl:propertyChainAxiom", source, re.DOTALL)
    if chain_match is None or target_match is None:
        raise ValueError("fixture lacks an OWL object-property chain")
    chain = re.findall(r"ex:([A-Za-z0-9_-]+)", chain_match.group(1))
    if len(chain) < 2:
        raise ValueError("property chain is not multi-hop")
    relations: dict[str, set[tuple[str, str]]] = {predicate: set() for predicate in chain}
    contributing_graphs: set[str] = set()
    for graph, body in graph_blocks:
        for predicate in chain:
            for match in re.finditer(rf"ex:([A-Za-z0-9_-]+)[^.]*?ex:{re.escape(predicate)}\s+ex:([A-Za-z0-9_-]+)", body, re.DOTALL):
                relations[predicate].add((match.group(1), match.group(2)))
                contributing_graphs.add(graph)
    paths = [(subject, obj, [triple(subject, chain[0], obj)]) for subject, obj in relations[chain[0]]]
    for predicate in chain[1:]:
        expanded = []
        for start, current, context in paths:
            for subject, obj in relations[predicate]:
                if subject == current:
                    expanded.append((start, obj, [*context, triple(subject, predicate, obj)]))
        paths = expanded
    if not paths or len(contributing_graphs) < 3:
        raise ValueError("no cross-domain multi-hop property-chain consequence exists")
    target = target_match.group(1)
    result = sorted({line for start, end, context in paths for line in [*context, triple(start, target, end)]})
    expected = sorted(line.strip() for line in args.expected.read_text(encoding="utf-8").splitlines() if line.strip())
    if result != expected:
        raise ValueError("independently inferred context graph differs from expected canonical N-Triples")
    payload = ("\n".join(result) + "\n").encode()
    report = {
        "formatVersion": 1, "domainCount": len(contributing_graphs), "hopCount": len(chain),
        "reasonedOutputTriples": len(paths), "resultTripleCount": len(result),
        "resultGraphSha256": hashlib.sha256(payload).hexdigest(), "complete": True,
    }
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
