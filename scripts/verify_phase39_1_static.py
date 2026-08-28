#!/usr/bin/env python3
"""Static contract for Phase 39.1 reproducible Rust dependency resolution."""
from __future__ import annotations
import pathlib, sys, tomllib, yaml
ROOT=pathlib.Path(__file__).resolve().parents[1]

def require(path:str,*tokens:str)->str:
    p=ROOT/path
    if not p.is_file(): raise RuntimeError(f"missing {path}")
    text=p.read_text(encoding='utf-8')
    for token in tokens:
        if token not in text: raise RuntimeError(f"{path} missing {token}")
    return text

def main()->int:
    cargo=tomllib.loads(require('Cargo.toml'))
    if cargo['workspace']['package']['rust-version'] != '1.97.1':
        raise RuntimeError('workspace Rust version changed from the Phase 39 pinned toolchain')
    require('rust-toolchain.toml','1.97.1')
    require('scripts/generate_cargo_lock.sh','cargo generate-lockfile','cargo metadata --locked','RUSTC_VERSION','CARGO_VERSION')
    require('scripts/qualify_phase39_1.sh','scripts/qualify_phase39.sh','test -s Cargo.lock','cargo metadata --locked')
    gates=yaml.safe_load(require('acceptance/phase-gates.yaml'))['phases']
    if not any(str(x.get('phase'))=='39.1' for x in gates):
        raise RuntimeError('acceptance registry lacks Phase 39.1')
    print('Phase 39.1 static contract verification passed; Cargo.lock still requires the pinned Cargo resolver')
    return 0
if __name__=='__main__':
    try: raise SystemExit(main())
    except (KeyError,RuntimeError,TypeError,ValueError) as e:
        print(f'phase 39.1 static verification failed: {e}',file=sys.stderr); raise SystemExit(1)
