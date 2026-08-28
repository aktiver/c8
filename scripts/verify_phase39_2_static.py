#!/usr/bin/env python3
"""Static checks that Phase 39.2 executes, rather than merely fetches, W3C tests."""
from __future__ import annotations
import pathlib,sys,yaml
ROOT=pathlib.Path(__file__).resolve().parents[1]
def req(path,*tokens):
 p=ROOT/path
 if not p.is_file(): raise RuntimeError(f'missing {path}')
 text=p.read_text(encoding='utf-8')
 for token in tokens:
  if token not in text: raise RuntimeError(f'{path} missing {token}')
 return text
def main():
 req('scripts/run_w3c_conformance.py','MF.QueryEvaluationTest','TestTrigPositiveSyntax','driverExit','fail-on-unsupported','qualification')
 req('crates/ngkg-reference/src/bin/ngkg-w3c-case.rs','query-evaluation','trig-syntax','sparql-syntax','DefaultDatasetPolicy::StoredDefault','verify_expected')
 req('crates/ngkg-reference/src/query.rs','execute_compiled_query_with_default_policy','load_rdf_fixture')
 req('scripts/qualify_phase39_2.sh','run_w3c_conformance.py','w3c-phase39.2.json','--fail-on-unsupported')
 gates=yaml.safe_load(req('acceptance/phase-gates.yaml'))['phases']
 if not any(str(x.get('phase'))=='39.2' for x in gates): raise RuntimeError('acceptance registry lacks 39.2')
 print('Phase 39.2 static contract verification passed; actual W3C execution remains a native qualification gate')
 return 0
if __name__=='__main__':
 try: raise SystemExit(main())
 except (KeyError,RuntimeError,TypeError,ValueError) as e:
  print(f'phase 39.2 static verification failed: {e}',file=sys.stderr); raise SystemExit(1)
