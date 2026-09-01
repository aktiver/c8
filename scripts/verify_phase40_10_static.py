#!/usr/bin/env python3
from pathlib import Path
import json, hashlib, subprocess, sys, yaml, re, math
ROOT=Path(__file__).resolve().parents[1]
def text(p): return (ROOT/p).read_text()
def load(p): return json.loads(text(p))
def sha(p): return hashlib.sha256((ROOT/p).read_bytes()).hexdigest()
def req(p,*tokens):
 s=text(p)
 for t in tokens:
  if t not in s: raise RuntimeError(f'{p} missing {t}')
 return s
def main():
 report=ROOT/'qualification/phase40.10-helm-ceilings.json'; report.parent.mkdir(exist_ok=True)
 cp=subprocess.run([sys.executable,str(ROOT/'scripts/validate_phase40_helm_ceilings.py'),'--root',str(ROOT),'--report',str(report)],cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 if cp.returncode: raise RuntimeError(cp.stdout.strip())
 for args in [
  ['--platform-overlay','test-corpus/phase40_10/platform-invalid-partition.yaml'],
  ['--platform-overlay','test-corpus/phase40_10/platform-invalid-memory.yaml'],
  ['--workloads-overlay','test-corpus/phase40_10/workloads-invalid-lanes.yaml'],
 ]:
  bad=subprocess.run([sys.executable,str(ROOT/'scripts/validate_phase40_helm_ceilings.py'),'--root',str(ROOT),*args],cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
  if bad.returncode==0: raise RuntimeError(f'unsafe Phase 40 Helm overlay was accepted: {args}')
 pv=yaml.safe_load(text('charts/ngkg-platform/values.yaml')); wv=yaml.safe_load(text('charts/ngkg-workloads/values.yaml'))
 expected={'maxCandidateBindings':10_000_000,'maxPartitionCandidates':250_000,'maxExactPartitions':4096,'maxGroundedAxiomsPerCandidate':65536,'maxGroundedRdfBytesPerCandidate':16777216,'reasonerConcurrency':8,'reasonerHeapMiBPerLane':4096,'reasonerTimeoutSeconds':300,'maxCertificateBytes':536870912,'maxProofSupportIds':1_000_000}
 if pv['phase40']['direct']!=expected: raise RuntimeError('platform phase40.direct defaults drifted')
 expected_a={'maxBgps':4096,'maxTriplesPerBgp':65536,'maxClassificationCpuLanes':32}
 if wv['phase40']['directAdmission']!=expected_a: raise RuntimeError('workloads directAdmission defaults drifted')
 dsrc=req('crates/ngkg-direct-reasoner/src/lib.rs','MAX_LOCAL_REASONER_LANES: usize = 8','MAX_EXACT_PARTITIONS: u64 = 4096','max_candidate_bindings: 10_000_000','max_partition_candidates: 250_000','max_grounded_axioms_per_candidate: 65_536','max_grounded_rdf_bytes_per_candidate: 16 * 1024 * 1024','reasoner_heap_mib_per_lane: 4096','Duration::from_secs(300)')
 osrc=req('crates/ngkg-owl-direct/src/lib.rs','MAX_CLASSIFICATION_LANES: usize = 32','DEFAULT_MAX_BGPS: usize = 4096','DEFAULT_MAX_TRIPLES_PER_BGP: usize = 65_536')
 req('crates/ngkg-types/src/direct_proof.rs','MAX_PROOF_RECORDS: usize = 1_000_000')
 req('crates/ngkg-types/src/direct_certificate.rs','MAX_SUPPORT_REFERENCES: usize = 1_000_000')
 # 40.10 is declaration-only: later phases own consumption/propagation.
 for rel in ['charts/ngkg-platform/templates/operator.yaml','charts/ngkg-platform/templates/distributed-operator.yaml','charts/ngkg-workloads/templates/online-data-plane.yaml']:
  if '.Values.phase40' in text(rel): raise RuntimeError(f'{rel} prematurely wires Phase 40 ceilings before 40.11-40.13')
 reg=load('verification/parents/phase-40.10-ceilings.json')
 if reg.get('phase')!='40.10' or len(reg.get('phase40HelmDeclared',[]))!=13: raise RuntimeError('Phase 40 ceiling registry incomplete')
 paths={x['helmPath'] for x in reg['phase40HelmDeclared']}
 required={f'phase40.direct.{x}' for x in expected}|{f'phase40.directAdmission.{x}' for x in expected_a}
 if paths!=required: raise RuntimeError('ceiling registry paths drifted')
 phase=load('verification/phase-40.10.json')
 for k in ['authoritativeHelmCeilingsDeclared','platformDirectExactCeilingsDeclared','workloadDirectAdmissionCeilingsDeclared','helmValuesSchemasSynchronized','runtimeDefaultsMatchedAtDeclaration','crossFieldSafetyChecksImplemented','reasonerMemoryBudgetSafetyChecked','certificateProofBoundsRegistered']:
  if phase.get(k) is not True: raise RuntimeError(f'40.10 missing {k}')
 for k in ['runtimeCeilingWiringImplemented','referenceWorkerCeilingWiringImplemented','distributedWorkerCeilingWiringImplemented','operatorCeilingWiringImplemented','nativeHelmQualificationExecuted','standardsClaimsEnabled']:
  if phase.get(k) is not False: raise RuntimeError(f'40.10 overclaims {k}')
 r=load('verification/phase-40.10-requirements.json'); t=load('verification/phase-40.10-traceability.json'); ids={x['id'] for x in r['requirements']}
 if ids!={f'P40-10-{i:03d}' for i in range(1,13)} or {x['requirementId'] for x in t['entries']}!=ids: raise RuntimeError('40.10 requirements/traceability incomplete')
 for e in t['entries']:
  for rel in e.get('implementation',[])+e.get('evidence',[]):
   if rel.endswith('phase-40.10.json') and 'stabilization' in rel: continue
   if not (ROOT/rel).is_file(): raise RuntimeError(f'traceability file missing: {rel}')
 cap=load('verification/parents/phase-40.10-capability-status.json')
 if cap['capabilities']['phase40HelmCeilings']['status']!='authoritative-helm-values-declared-not-yet-runtime-wired' or cap.get('standardsClaimsEnabled') is not False: raise RuntimeError('40.10 capability status invalid')
 gates=yaml.safe_load(text('acceptance/phase-gates.yaml'))['phases']; by={str(x['phase']):x for x in gates}
 if by.get('40.10',{}).get('command')!='scripts/qualify_phase40_10.sh': raise RuntimeError('40.10 acceptance gate missing')
 ev=load('verification/stabilization/phase-40.10.json'); embedded=ROOT/ev['embeddedParentManifest']
 if ev.get('parentLabel')!='phase-40.9' or ev.get('currentLabel')!='phase-40.10' or ev.get('deletedFiles')!=[]: raise RuntimeError('40.10 inheritance invalid')
 if not embedded.is_file() or sha(ev['embeddedParentManifest'])!=ev['parentFileManifestSha256']: raise RuntimeError('40.10 parent manifest mismatch')
 print('Phase 40.10 static verification passed; authoritative Helm ceilings are bounded, schema-valid, resource-safe, and intentionally not yet runtime-wired')
 return 0
if __name__=='__main__':
 try: raise SystemExit(main())
 except Exception as e: print(f'phase 40.10 static verification failed: {e}',file=sys.stderr); raise SystemExit(1)
