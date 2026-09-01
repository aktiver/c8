#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from run_differential import canonical_graph, canonical_sparql_json  # noqa: E402


class DifferentialCanonicalizationTests(unittest.TestCase):
    def test_select_preserves_multiset_and_ignores_binding_order(self) -> None:
        left = {"head": {"vars": ["x", "y"]}, "results": {"bindings": [
            {"x": {"type": "uri", "value": "urn:a"}, "y": {"type": "literal", "value": "1", "datatype": "urn:int"}},
            {"x": {"type": "uri", "value": "urn:a"}, "y": {"type": "literal", "value": "1", "datatype": "urn:int"}},
        ]}}
        right = {"results": {"bindings": list(reversed(left["results"]["bindings"]))}, "head": {"vars": ["y", "x"]}}
        self.assertEqual(canonical_sparql_json(json.dumps(left).encode())[0], canonical_sparql_json(json.dumps(right).encode())[0])

    def test_select_detects_bag_multiplicity(self) -> None:
        one = {"head": {"vars": ["x"]}, "results": {"bindings": [{"x": {"type": "uri", "value": "urn:a"}}]}}
        two = {"head": {"vars": ["x"]}, "results": {"bindings": one["results"]["bindings"] * 2}}
        self.assertNotEqual(canonical_sparql_json(json.dumps(one).encode())[0], canonical_sparql_json(json.dumps(two).encode())[0])

    def test_ask_is_canonical(self) -> None:
        digest, normalized = canonical_sparql_json(b'{"head":{},"boolean":true}')
        self.assertEqual(64, len(digest))
        self.assertEqual({"form": "ASK", "boolean": True}, normalized)

    def test_graph_without_blank_nodes_is_order_independent(self) -> None:
        left = b"<urn:s> <urn:p> <urn:o> .\n<urn:x> <urn:p> <urn:y> .\n"
        right = b"<urn:x> <urn:p> <urn:y> .\n<urn:s> <urn:p> <urn:o> .\n"
        self.assertEqual(canonical_graph(left, None, None)[0], canonical_graph(right, None, None)[0])

    def test_graph_blank_nodes_fail_without_canonicalizer(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "RDFC-1.0"):
            canonical_graph(b"_:a <urn:p> <urn:o> .\n", None, None)


if __name__ == "__main__":
    unittest.main()
