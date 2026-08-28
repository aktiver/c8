#!/usr/bin/env python3
"""Governance and cumulative-gate checks for the Phase 39 stabilization release."""
from __future__ import annotations
import pathlib,sys,yaml
ROOT=pathlib.Path(__file__).resolve().parents[1]
def req(path,*tokens):
 p=ROOT/path
 if not p.is_file(): raise RuntimeError(f'missing {path}')
 t=p.read_text(encoding='utf-8')
 for token in tokens:
  if token not in t: raise RuntimeError(f'{path} missing {token}')
 return t
def main():
 registry=yaml.safe_load(req('acceptance/phase-gates.yaml'))['phases']
 values={str(x['phase']):x for x in registry}
 for phase in [str(x) for x in range(17,34)]+['39.1','39.2','39.3','39.4','39.5']:
  if phase not in values: raise RuntimeError(f'acceptance registry lacks {phase}')
 for phase in range(17,34):
  expected=f'scripts/qualify_phase{phase}.sh'
  if values[str(phase)]['command']!=expected: raise RuntimeError(f'Phase {phase} command mismatch')
 req('scripts/run_cumulative_static_gates.py','verify_phase','through-phase','report')
 req('scripts/run_acceptance_gates.py','acceptance/phase-gates.yaml','dry-run')
 req('docs/PHASE_39_STABILIZATION_SUPERSESSION.md','Unknown query byte hash','W3C suite checkout','Phase 17–33')
 release=req('scripts/ci_release.sh','verify_phase39_5_static.py','run_w3c_conformance.py','run_cumulative_static_gates.py')
 print('Phase 39.5 static contract verification passed; cumulative registry is complete through the stabilization phases')
 return 0
if __name__=='__main__':
 try: raise SystemExit(main())
 except (KeyError,RuntimeError,TypeError,ValueError) as e:
  print(f'phase 39.5 static verification failed: {e}',file=sys.stderr); raise SystemExit(1)
