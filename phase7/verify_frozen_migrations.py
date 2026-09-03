#!/usr/bin/env python3
from pathlib import Path
import hashlib

root = Path(__file__).resolve().parents[1] / "NGKG_1_0_0_GA"
expected = {
    "migrations/0002_atomic_compilation.sql": "97a14756bf6a4c042ff2ffb407d529ad7890c1c168284b457f8d4c9c5fdf9c0d",
    "migrations/0006_named_datasets.sql": "076d7c5199bab29f32b92c8511dc064bd64b5a8f6c0269615bde2add536adda2",
}
for relative, digest in expected.items():
    observed = hashlib.sha256((root / relative).read_bytes()).hexdigest()
    if observed != digest:
        raise SystemExit(f"historical migration drift: {relative}: {observed}")
latest = sorted(path.name for path in (root / "migrations").glob("*.sql"))
if latest[-1] != "0011_forward_only_contract_repairs.sql":
    raise SystemExit("unexpected forward migration ordering")
print("frozen historical migrations: PASS")
