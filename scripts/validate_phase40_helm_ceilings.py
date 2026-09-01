#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, math, re, sys
from pathlib import Path
import yaml
from jsonschema import Draft202012Validator

def load_yaml(path: Path): return yaml.safe_load(path.read_text())
def load_json(path: Path): return json.loads(path.read_text())
def parse_bytes(value: str) -> int:
 m=re.fullmatch(r'([1-9][0-9]*)(Ki|Mi|Gi|Ti)',str(value))
 if not m: raise ValueError(f'unsupported quantity: {value}')
 n=int(m.group(1)); return n*{'Ki':1024,'Mi':1024**2,'Gi':1024**3,'Ti':1024**4}[m.group(2)]
def validate_phase40_subtree(values, schema):
 Draft202012Validator.check_schema(schema)
 subschema=schema['properties']['phase40']
 errors=sorted(Draft202012Validator(subschema).iter_errors(values['phase40']),key=lambda e:list(e.path))
 if errors: raise ValueError('; '.join(f"{'.'.join(map(str,e.path))}: {e.message}" for e in errors))
def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--root',default='.'); ap.add_argument('--report'); ap.add_argument('--platform-overlay'); ap.add_argument('--workloads-overlay'); args=ap.parse_args()
 root=Path(args.root).resolve()
 pv=load_yaml(root/'charts/ngkg-platform/values.yaml'); ps=load_json(root/'charts/ngkg-platform/values.schema.json')
 wv=load_yaml(root/'charts/ngkg-workloads/values.yaml'); ws=load_json(root/'charts/ngkg-workloads/values.schema.json')
 def merge(base, overlay):
  for k,v in overlay.items():
   if isinstance(v,dict) and isinstance(base.get(k),dict): merge(base[k],v)
   else: base[k]=v
 if args.platform_overlay: merge(pv,load_yaml(Path(args.platform_overlay)))
 if args.workloads_overlay: merge(wv,load_yaml(Path(args.workloads_overlay)))
 validate_phase40_subtree(pv,ps); validate_phase40_subtree(wv,ws)
 d=pv['phase40']['direct']; a=wv['phase40']['directAdmission']
 if d['maxPartitionCandidates']>d['maxCandidateBindings']: raise ValueError('maxPartitionCandidates exceeds maxCandidateBindings')
 needed=math.ceil(d['maxCandidateBindings']/d['maxPartitionCandidates'])
 if needed>d['maxExactPartitions']: raise ValueError(f'maxExactPartitions {d["maxExactPartitions"]} cannot cover {needed} required partitions')
 if d['maxExactPartitions']>4096: raise ValueError('maxExactPartitions exceeds Phase 40.8 hard safety cap 4096')
 if d['reasonerConcurrency']>32: raise ValueError('reasonerConcurrency exceeds Phase 40.8 hard safety cap 32')
 ref_cpu=float(pv['operator']['reference']['cpu'])
 if d['reasonerConcurrency']>ref_cpu: raise ValueError('reasonerConcurrency exceeds reference-worker CPU request')
 ref_mem=parse_bytes(pv['operator']['reference']['memory'])
 heap_bytes=d['reasonerConcurrency']*d['reasonerHeapMiBPerLane']*1024*1024
 if heap_bytes > int(ref_mem*0.80): raise ValueError('reasoner lane heap budget exceeds 80% of reference-worker memory')
 if d['reasonerTimeoutSeconds']>int(pv['operator']['reference']['ceilings']['reasonerSeconds']): raise ValueError('direct partition timeout exceeds reference reasoner ceiling')
 if d['maxCertificateBytes']>int(pv['operator']['reference']['ceilings']['outputBytes']): raise ValueError('certificate ceiling exceeds worker output ceiling')
 if d['maxProofSupportIds']>1_000_000: raise ValueError('proof support ceiling exceeds Phase 40.9 runtime hard cap')
 if a['maxClassificationCpuLanes']>32: raise ValueError('classification lanes exceed Phase 40.7 hard cap')
 report={
  'formatVersion':1,'phase':'40.10','status':'pass','requiredExactPartitions':needed,
  'referenceWorkerCpu':ref_cpu,'referenceWorkerMemoryBytes':ref_mem,
  'reasonerHeapBudgetBytes':heap_bytes,'reasonerHeapBudgetPercent':round(heap_bytes/ref_mem*100,3),
  'platformDirect':d,'workloadsDirectAdmission':a,
  'referenceWorkerRuntimeEnforcement':'40.11','onlineAdmissionRuntimeEnforcement':'40.12','runtimeWiringDeferredTo':['40.13']
 }
 if args.report: Path(args.report).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,sort_keys=True))
 return 0
if __name__=='__main__':
 try: raise SystemExit(main())
 except Exception as e: print(f'Phase 40.10 Helm ceiling validation failed: {e}',file=sys.stderr); raise SystemExit(1)
