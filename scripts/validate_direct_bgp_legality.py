#!/usr/bin/env python3
"""Independent Phase 40.7 Direct-BGP legality report validator."""
from __future__ import annotations
import json, pathlib, re, sys
from jsonschema import Draft202012Validator
ROOT=pathlib.Path(__file__).resolve().parents[1]
SHA=re.compile(r'^[0-9a-f]{64}$')

def main(path: str) -> int:
    schema=json.loads((ROOT/'contracts/direct-bgp-legality.schema.json').read_text())
    Draft202012Validator.check_schema(schema)
    obj=json.loads(pathlib.Path(path).read_text())
    errors=sorted(Draft202012Validator(schema).iter_errors(obj), key=lambda e:list(e.path))
    if errors:
        raise ValueError('; '.join(f'{list(e.path)}: {e.message}' for e in errors[:8]))
    bgps=obj['bgps']
    if obj['bgpCount'] != len(bgps): raise ValueError('bgpCount mismatch')
    if [b['ordinal'] for b in bgps] != list(range(len(bgps))): raise ValueError('BGP ordinals must be contiguous preorder')
    if obj['allBgpsLegal'] != all(b['status']=='legal' for b in bgps): raise ValueError('aggregate legality mismatch')
    for b in bgps:
        forms=b['recognizedForms']
        if forms != sorted(set(forms)): raise ValueError('recognizedForms must be sorted unique')
        names=[v['variable'] for v in b['variables']]
        if names != sorted(set(names)): raise ValueError('variable typings must be sorted unique')
        if b['status']=='legal' and 'failure' in b: raise ValueError('legal BGP carries failure')
        if b['status']=='illegal' and 'failure' not in b: raise ValueError('illegal BGP lacks failure')
    print(f"valid Phase 40.7 Direct-BGP legality report: {path}")
    return 0

if __name__=='__main__':
    if len(sys.argv)!=2:
        print('usage: validate_direct_bgp_legality.py REPORT.json', file=sys.stderr); raise SystemExit(2)
    try: raise SystemExit(main(sys.argv[1]))
    except (OSError,ValueError,KeyError,json.JSONDecodeError) as exc:
        print(f'invalid Direct-BGP legality report: {exc}', file=sys.stderr); raise SystemExit(1)
