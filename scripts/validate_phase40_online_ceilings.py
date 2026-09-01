#!/usr/bin/env python3
from __future__ import annotations
import argparse,json,re
from pathlib import Path
import yaml

HARD={'maxBgps':4096,'maxTriplesPerBgp':65536,'maxClassificationCpuLanes':32}
ENV={
 'maxBgps':'NGKG_PHASE40_DIRECT_ADMISSION_MAX_BGPS',
 'maxTriplesPerBgp':'NGKG_PHASE40_DIRECT_ADMISSION_MAX_TRIPLES_PER_BGP',
 'maxClassificationCpuLanes':'NGKG_PHASE40_DIRECT_ADMISSION_MAX_CLASSIFICATION_CPU_LANES',
}

def merge(dst,src):
 for k,v in (src or {}).items():
  if isinstance(v,dict) and isinstance(dst.get(k),dict): merge(dst[k],v)
  else: dst[k]=v

def cpu_number(value):
 s=str(value).strip()
 if s.endswith('m'): return int(s[:-1])/1000
 return float(s)

def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--root',default='.'); ap.add_argument('--overlay'); ap.add_argument('--report')
 a=ap.parse_args(); root=Path(a.root).resolve()
 values=yaml.safe_load((root/'charts/ngkg-workloads/values.yaml').read_text())
 if a.overlay: merge(values,yaml.safe_load(Path(a.overlay).read_text()))
 direct=values['phase40']['directAdmission']
 for k,hard in HARD.items():
  v=direct.get(k)
  if not isinstance(v,int) or isinstance(v,bool) or v<=0 or v>hard: raise ValueError(f'{k} must be an integer in 1..{hard}')
 query_cpu=cpu_number(values['resources']['query']['limits']['cpu'])
 if query_cpu < 1: raise ValueError('query CPU limit must be at least one core')
 online=(root/'charts/ngkg-workloads/templates/online-data-plane.yaml').read_text()
 m=re.search(r"NGKG_RUST_COMPUTE_THREADS, value: '([0-9]+)'",online)
 if not m: raise ValueError('query Rust compute-thread budget is not statically declared')
 rust_threads=int(m.group(1))
 effective=max(1,min(direct['maxClassificationCpuLanes'],int(query_cpu),rust_threads))
 template=(root/'charts/ngkg-workloads/templates/phase40-online-ceilings.yaml').read_text()
 for k,env in ENV.items():
  if f'.Values.phase40.directAdmission.{k}' not in template or env not in template: raise ValueError(f'ConfigMap mapping missing {k}')
 if online.count('configMapRef: {name: ngkg-phase40-online-ceilings}') != 4: raise ValueError('all four online-serving roles must consume the immutable Phase 40 admission ConfigMap')
 report={'phase':'40.12','configured':direct,'hardCaps':HARD,'queryCpuLimit':query_cpu,'rustComputeThreads':rust_threads,'effectiveClassificationCpuLanes':effective,'onlineServingRoleConsumers':4,'status':'PASS'}
 if a.report:
  p=Path(a.report); p.parent.mkdir(parents=True,exist_ok=True); p.write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,sort_keys=True)); return 0
if __name__=='__main__':
 try: raise SystemExit(main())
 except Exception as e:
  print(f'phase 40.12 online ceiling validation failed: {e}',file=__import__('sys').stderr); raise SystemExit(1)
