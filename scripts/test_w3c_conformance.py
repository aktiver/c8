#!/usr/bin/env python3
"""Unit tests for W3C harness resource and path safety helpers."""
from __future__ import annotations

import importlib.util
import pathlib
import tempfile
import unittest


RUNNER = pathlib.Path(__file__).with_name("run_w3c_conformance.py")
SPEC = importlib.util.spec_from_file_location("run_w3c_conformance", RUNNER)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("unable to load W3C conformance runner")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ResourceTests(unittest.TestCase):
    def test_cpu_set_count_handles_ranges_and_duplicates(self) -> None:
        self.assertEqual(MODULE.cpu_set_count("0-3,2,8,10-11"), 7)

    def test_cpu_set_count_rejects_reverse_range(self) -> None:
        with self.assertRaises(ValueError):
            MODULE.cpu_set_count("4-2")

    def test_local_path_rejects_escape(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary).resolve()
            outside = root.parent / "outside.ttl"
            outside.write_text("", encoding="utf-8")
            try:
                with self.assertRaises(RuntimeError):
                    MODULE.local_path(outside.as_uri(), root)
            finally:
                outside.unlink(missing_ok=True)

    def test_local_path_accepts_regular_in_suite_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary).resolve()
            fixture = root / "fixture.ttl"
            fixture.write_text("", encoding="utf-8")
            self.assertEqual(MODULE.local_path(fixture.as_uri(), root), fixture)


if __name__ == "__main__":
    unittest.main()
