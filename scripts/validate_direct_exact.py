#!/usr/bin/env python3
from __future__ import annotations
import json,pathlib,sys
from jsonschema import Draft202012Validator
ROOT=pathlib.Path(__file__).resolve().parents[1]
def sorted_unique(values): return values==sorted(set(values))
def main(path:str)->int:
 p=pathlib.Path(path); value=json.loads(p.read_text())
 if 'snapshotManifestPath' in value: kind='job'; schema_name='direct-exact-job.schema.json'
 elif 'template' in value: kind='request'; schema_name='direct-exact-request.schema.json'
 else: kind='result'; schema_name='direct-exact-partition-result.schema.json'
 schema=json.loads((ROOT/'contracts'/schema_name).read_text()); Draft202012Validator(schema).validate(value)
 if kind=='request':
  if value['partition']['index']>=value['partition']['count']: raise ValueError('partition index must be less than count')
  if value['template']['bgpSha256']!=value['bgpSha256']: raise ValueError('template BGP hash mismatch')
 elif kind=='result':
  expected=value['partitionEndOrdinalExclusive']-value['partitionStartOrdinal']
  if expected!=value['checkedCandidateCount']: raise ValueError('partition is incomplete')
  if value['entailedCandidateCount']!=len(value['entailed']): raise ValueError('entailed count mismatch')
  if value['groundedOwl2dlCandidateCount']>value['checkedCandidateCount']: raise ValueError('grounded count exceeds checked count')
  if value['reasonerRequestCount']!=value['groundedOwl2dlCandidateCount']: raise ValueError('reasoner request count mismatch')
  ords=[x['candidateOrdinal'] for x in value['entailed']]
  if ords!=sorted(set(ords)): raise ValueError('entailed candidate ordinals must be sorted and unique')
  if any(o<value['partitionStartOrdinal'] or o>=value['partitionEndOrdinalExclusive'] for o in ords): raise ValueError('entailed ordinal outside partition')
  if value.get('adapterVersion')=='40.9':
   for row in value['entailed']:
    for key in ('groundedRdfSha256','logicalAxiomsSha256'):
     v=row.get(key,'')
     if len(v)!=64 or any(c not in '0123456789abcdef' for c in v): raise ValueError(f'{key} must be lowercase SHA-256')
    if row.get('logicalAxiomCount',-1)<0: raise ValueError('logicalAxiomCount must be non-negative')
 else:
  ds=value['resolvedDataset']
  for key in ('defaultGraphIds','namedGraphIds','authorizedGraphIds'):
   if not sorted_unique(ds[key]): raise ValueError(f'{key} must be sorted unique')
  auth=set(ds['authorizedGraphIds'])
  if not set(ds['defaultGraphIds']).issubset(auth) or not set(ds['namedGraphIds']).issubset(auth): raise ValueError('active graph IDs must be authorized')
  if ds['selectionSource']=='service_default' and (ds['defaultGraphIds']!=ds['authorizedGraphIds'] or ds['namedGraphIds']!=ds['authorizedGraphIds']): raise ValueError('service-default must expose all authorized graphs')
  if value.get('reasonerAdapterVersion')=='40.9' and not value.get('outputProofManifestPath'): raise ValueError('Phase 40.9 exact job requires outputProofManifestPath')
 print(f'validated Phase 40.8/40.9 exact {kind} contract: {p}'); return 0
if __name__=='__main__':
 try: raise SystemExit(main(sys.argv[1]))
 except Exception as exc:
  print(f'Phase 40.8/40.9 exact contract rejected: {exc}',file=sys.stderr); raise SystemExit(1)
