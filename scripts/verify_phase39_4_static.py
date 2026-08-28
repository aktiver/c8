#!/usr/bin/env python3
"""Static checks for Phase 39.4 general bounded scalar SPARQL admission."""
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
 runtime=req('crates/ngkg-reference/src/lib.rs',
  'execute_uncertified_compiled_with_dataset_bounded_cancellable',
  'DatasetSelectionSource::ServiceDefault',
  'phase39-exact-rdf-plus-qualified-finite-closure-v1')
 serving=req('services/online-serving/src/main.rs',
  'let Some(certificate) = certificate else',
  'execute_uncertified_exact_query(',
  'full exact scalar semantic runtime constructed',
  'exact_scalar_ad_hoc',
  'full_exact_active_dataset_v1',
  'data/query-dataset.nq')
 admission=serving.split('let Some(certificate) = certificate else',1)[0]
 if 'ok_or(ReferenceRuntimeError::UncertifiedQuery)' in admission[-1200:]:
  raise RuntimeError('online admission still rejects unknown query hashes before exact fallback')
 req('scripts/qualify_phase39_4.sh','cargo test --locked -p ngkg-reference','verify_phase39_4_static.py')
 gates=yaml.safe_load(req('acceptance/phase-gates.yaml'))['phases']
 if not any(str(x.get('phase'))=='39.4' for x in gates): raise RuntimeError('acceptance registry lacks 39.4')
 print('Phase 39.4 static contract verification passed; ad-hoc queries now have a bounded scalar exact-RDF path')
 return 0
if __name__=='__main__':
 try: raise SystemExit(main())
 except (KeyError,RuntimeError,TypeError,ValueError) as e:
  print(f'phase 39.4 static verification failed: {e}',file=sys.stderr); raise SystemExit(1)
