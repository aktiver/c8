#!/usr/bin/env python3
"""Compile and differentially test the bounded OpenMP process ABI when GCC supports it."""

from __future__ import annotations

import os
from pathlib import Path
import struct
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="ngkg-openmp-") as directory:
        binary = Path(directory) / "ngkg-openmp-filter"
        subprocess.run([
            "gcc", "-std=c17", "-Wall", "-Wextra", "-Werror", "-fopenmp", "-O2",
            str(ROOT / "hpc/native/ngkg_openmp_filter.c"), "-o", str(binary),
        ], check=True)
        rows = [(1, 10, 2, 7, 1), (3, 11, 4, 8, 1), (5, 10, 6, 7, 0)]
        allowed = [7, 8]
        flags = 2 | 16
        payload = bytearray(b"NGKGOMP1")
        payload += struct.pack("<6Q", len(rows), flags, 0, 10, 0, 0)
        payload += struct.pack("<Q", len(allowed))
        payload += struct.pack(f"<{len(allowed)}Q", *allowed)
        for subject, predicate, obj, graph, queryable in rows:
            payload += struct.pack("<4QB", subject, predicate, obj, graph, queryable)
        environment = dict(os.environ, OMP_NUM_THREADS="2", OMP_DYNAMIC="FALSE", OMP_MAX_ACTIVE_LEVELS="1")
        output = subprocess.run([str(binary)], input=payload, stdout=subprocess.PIPE, check=True, env=environment).stdout
        assert output[:8] == b"NGKGOUT1"
        assert struct.unpack("<Q", output[8:16])[0] == len(rows)
        assert list(output[16:]) == [1, 0, 0]


if __name__ == "__main__":
    main()
