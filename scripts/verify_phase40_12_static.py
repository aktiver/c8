#!/usr/bin/env python3
from __future__ import annotations
import hashlib,json,subprocess,sys
from pathlib import Path
import yaml
ROOT=Path(__file__).resolve().parents[1]
def text(p): return (ROOT/p).read_text()
def load(p): return json.loads(text(p))
def sha(p): return hashlib.sha256((ROOT/p).read_bytes()).hexdigest()
def req(p,*tokens):
 s=text(p)
 for t in tokens:
  if t not in s: raise RuntimeError(f'{p} missing required Phase 40.12 token {t!r}')
 return s
def run(*args,ok=True):
 cp=subprocess.run(args,cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 if (cp.returncode==0)!=ok: raise RuntimeError(cp.stdout)
 return cp.stdout

def main():
 run(sys.executable,'scripts/validate_phase40_online_ceilings.py','--root',str(ROOT),'--report',str(ROOT/'qualification/phase40.12-online-ceilings.json'))
 for rel in ['test-corpus/phase40_12/workloads-invalid-bgps.yaml','test-corpus/phase40_12/workloads-invalid-triples.yaml','test-corpus/phase40_12/workloads-invalid-lanes.yaml']:
  run(sys.executable,'scripts/validate_phase40_online_ceilings.py','--root',str(ROOT),'--overlay',str(ROOT/rel),ok=False)
 values=yaml.safe_load(text('charts/ngkg-workloads/values.yaml'))['phase40']['directAdmission']
 expected={'maxBgps':4096,'maxTriplesPerBgp':65536,'maxClassificationCpuLanes':32}
 if values!=expected: raise RuntimeError('workload Direct admission values drifted')
 tmpl=req('charts/ngkg-workloads/templates/phase40-online-ceilings.yaml','kind: ConfigMap','immutable: true','ngkg.io/phase: "40.12"','NGKG_PHASE40_DIRECT_ADMISSION_MAX_BGPS','NGKG_PHASE40_DIRECT_ADMISSION_MAX_TRIPLES_PER_BGP','NGKG_PHASE40_DIRECT_ADMISSION_MAX_CLASSIFICATION_CPU_LANES')
 config_ref='configMapRef: {name: ngkg-phase40-online-ceilings-{{ toJson .Values.phase40.directAdmission | sha256sum | trunc 12 }}}'
 online=req('charts/ngkg-workloads/templates/online-data-plane.yaml',config_ref)
 if online.count(config_ref)!=4: raise RuntimeError('not all online-serving roles consume the content-addressed Phase 40.12 ConfigMap')
 runtime=req('services/online-serving/src/phase40_limits.rs','TrustedPhase40AdmissionCeilings','from_env','classifier_limits','available_parallelism','min(rust_compute_threads)','ngkg-phase40-online-admission-ceilings-v1','HARD_MAX_BGPS: usize = 4096','HARD_MAX_TRIPLES_PER_BGP: usize = 65_536','HARD_MAX_CLASSIFICATION_CPU_LANES: usize = 32')
 main=req('services/online-serving/src/main.rs','TrustedPhase40AdmissionCeilings::from_env','phase40_admission.classifier_limits(rust_compute_threads)','direct_bgp_classification_limits','state.direct_bgp_classification_limits','phase40_admission_ceiling_sha256')
 handler=main[main.index('async fn validate_direct_bgps('):main.index('async fn query(',main.index('async fn validate_direct_bgps('))]
 if 'DirectBgpClassificationLimits::default()' in handler: raise RuntimeError('Direct-BGP endpoint still uses Phase 40.7 defaults')
 # The offline build worker does not execute SPARQL Direct BGPs and must not be coupled to these admission envs.
 build_worker=text('services/distributed-worker/src/main.rs')
 if 'NGKG_PHASE40_DIRECT_ADMISSION_' in build_worker: raise RuntimeError('offline distributed build worker incorrectly coupled to Direct-BGP admission')
 # Exact reasoner ceilings from 40.11 remain unchanged; 40.13 legitimately completes operator propagation.
 req('services/reference-worker/src/phase40_limits.rs','TrustedPhase40DirectCeilings')
 if not (ROOT/'verification/phase-40.13.json').is_file():
  for rel in ['services/operator/src/main.rs','services/distributed-operator/src/main.rs']:
   if 'NGKG_PHASE40_DIRECT_MAX_CANDIDATE_BINDINGS' in text(rel): raise RuntimeError(f'{rel} prematurely implements 40.13')
 reg=load('verification/phase-40-ceilings.json')
 if reg.get('phase') not in {'40.12','40.13'}: raise RuntimeError('ceiling registry not a valid 40.12 descendant')
 workload=[x for x in reg['phase40HelmDeclared'] if x['helmChart']=='ngkg-workloads']
 if len(workload)!=3 or any(x.get('status')!='online-serving-enforced-static-qualified' for x in workload): raise RuntimeError('workload Phase 40 ceilings are not marked runtime-enforced')
 if reg.get('onlineWorkerEnforcement',{}).get('phase')!='40.12': raise RuntimeError('online worker enforcement evidence missing')
 phase=load('verification/phase-40.12.json')
 for k in ['authoritativeWorkloadAdmissionCeilingsConsumed','immutableOnlineAdmissionConfigMapImplemented','allOnlineServingRolesValidateAdmissionBundle','queryDirectBgpClassifierUsesTrustedCeilings','classificationCpuOversubscriptionPrevented','admissionCeilingBundleSha256Logged','distributedFragmentRoleConsumesPolicyBundle','offlineDistributedBuildWorkerDirectAdmissionNotApplicable','referenceWorkerExactCeilingsPreserved']:
  if phase.get(k) is not True: raise RuntimeError(f'40.12 missing {k}')
 for k in ['operatorJobPropagationImplemented','distributedOperatorJobPropagationImplemented','nativeHelmQualificationExecuted','standardsClaimsEnabled']:
  if phase.get(k) is not False: raise RuntimeError(f'40.12 overclaims {k}')
 r=load('verification/phase-40.12-requirements.json'); t=load('verification/phase-40.12-traceability.json'); ids={x['id'] for x in r['requirements']}
 if ids!={f'P40-12-{i:03d}' for i in range(1,13)} or {x['requirementId'] for x in t['entries']}!=ids: raise RuntimeError('40.12 requirements/traceability incomplete')
 for e in t['entries']:
  for rel in e.get('implementation',[])+e.get('evidence',[]):
   if rel=='verification/stabilization/phase-40.12.json': continue
   if not (ROOT/rel).is_file(): raise RuntimeError(f'traceability file missing: {rel}')
 cap=load('verification/phase-40-capability-status.json')
 if cap['capabilities']['phase40OnlineAdmissionCeilings']['status']!='implemented-static-qualified' or cap.get('standardsClaimsEnabled') is not False: raise RuntimeError('40.12 capability status invalid')
 gates=yaml.safe_load(text('acceptance/phase-gates.yaml'))['phases']; by={str(x['phase']):x for x in gates}
 if by.get('40.12',{}).get('command')!='scripts/qualify_phase40_12.sh': raise RuntimeError('40.12 acceptance gate missing')
 ev=load('verification/stabilization/phase-40.12.json'); embedded=ROOT/ev['embeddedParentManifest']
 if ev.get('parentLabel')!='phase-40.11' or ev.get('currentLabel')!='phase-40.12' or ev.get('deletedFiles')!=[]: raise RuntimeError('40.12 inheritance invalid')
 if not embedded.is_file() or sha(ev['embeddedParentManifest'])!=ev['parentFileManifestSha256']: raise RuntimeError('40.12 parent manifest mismatch')
 print('Phase 40.12 static verification passed; online/query and distributed-fragment roles consume trusted admission ceilings with CPU-safe Direct-BGP enforcement')
 return 0
if __name__=='__main__':
 try: raise SystemExit(main())
 except Exception as e: print(f'phase 40.12 static verification failed: {e}',file=sys.stderr); raise SystemExit(1)
