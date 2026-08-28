#!/usr/bin/env python3
"""Static evidence for the Phase 39.3 GRAPH ?g regression matrix."""
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
 req('crates/ngkg-reference/src/query.rs',
  'phase39_3_graph_variable_values_filter_and_bag_semantics',
  'phase39_3_from_named_limits_graph_variable_domain',
  'phase39_3_reused_graph_variable_joins_inside_the_same_named_graph',
  'phase39_3_authorization_and_protocol_dataset_bound_graph_variable_visibility',
  'GRAPH ?g must preserve SPARQL bag multiplicity',
  'ProtocolDatasetSpecification')
 req('scripts/qualify_phase39_3.sh','cargo test --locked -p ngkg-reference','verify_phase39_3_static.py')
 gates=yaml.safe_load(req('acceptance/phase-gates.yaml'))['phases']
 if not any(str(x.get('phase'))=='39.3' for x in gates): raise RuntimeError('acceptance registry lacks 39.3')
 print('Phase 39.3 static contract verification passed; Rust regression execution remains mandatory')
 return 0
if __name__=='__main__':
 try: raise SystemExit(main())
 except (KeyError,RuntimeError,TypeError,ValueError) as e:
  print(f'phase 39.3 static verification failed: {e}',file=sys.stderr); raise SystemExit(1)
