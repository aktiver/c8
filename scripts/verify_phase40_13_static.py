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
  if t not in s: raise RuntimeError(f'{p} missing Phase 40.13 token {t!r}')
 return s
def run(*args):
 cp=subprocess.run(args,cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 if cp.returncode: raise RuntimeError(cp.stdout)
 return cp.stdout

def main():
 run(sys.executable,'scripts/validate_phase40_operator_propagation.py','--root',str(ROOT),'--report',str(ROOT/'qualification/phase40.13-operator-propagation.json'))
 run(sys.executable,'scripts/validate_phase40_helm_ceilings.py','--root',str(ROOT),'--report',str(ROOT/'qualification/phase40.13-helm-ceilings.json'))
 core=req('crates/ngkg-operator-core/src/lib.rs','Phase40DirectCeilings','env_pairs','bundle_sha256','ngkg-phase40-reference-worker-ceilings-v1\\0','phase40_13_tests')
 if core.count('NGKG_PHASE40_DIRECT_') < 10: raise RuntimeError('shared operator core does not define all ten exact ceiling env names')
 for rel in ['charts/ngkg-platform/templates/operator.yaml','charts/ngkg-platform/templates/distributed-operator.yaml']:
  req(rel,'envFrom:','configMapRef:',"phase40-reference-ceilings")
 op=req('services/operator/src/main.rs','Phase40DirectCeilings::from_env','phase40_direct','.env_pairs()','NGKG_PHASE40_DIRECT_CEILINGS_SHA256','ngkg.io/phase40-direct-ceilings-sha256','phase40DirectCeilingsSha256','observed_phase40 != Some(&expected_phase40)')
 dist=req('services/distributed-operator/src/main.rs','Phase40DirectCeilings::from_env','(stage == Stage::Reasoner).then(|| config.phase40_direct.bundle_sha256())','.phase40_direct','.env_pairs()','NGKG_PHASE40_DIRECT_CEILINGS_SHA256','ngkg.io/phase40-direct-ceilings-sha256','expected_phase40 != observed_phase40','phase40DirectCeilingsSha256')
 req('services/reference-worker/src/phase40_limits.rs','ENV_CEILINGS_SHA256','Phase 40 direct ceiling bundle SHA mismatch')
 # Offline distributed stages must not receive exact ceiling envs: only the reasoner branch appends them.
 envpos=dist.index('.env_pairs()',dist.index('environment.extend('))
 branch=dist.rfind('if stage == Stage::Reasoner',0,envpos)
 if branch < 0 or envpos-branch > 1600: raise RuntimeError('exact ceiling env propagation escaped Stage::Reasoner branch')
 reg=load('verification/phase-40-ceilings.json')
 if reg.get('phase')!='40.13' or reg.get('plannedPhase40Unwired')!=[]: raise RuntimeError('Phase 40 ceiling registry not closed at 40.13')
 platform=[x for x in reg['phase40HelmDeclared'] if x['helmChart']=='ngkg-platform']
 if len(platform)!=10 or any(x.get('status')!='reference-worker-enforced-operator-propagated-static-qualified' for x in platform): raise RuntimeError('platform exact ceilings not marked operator-propagated')
 if reg.get('operatorPropagation',{}).get('phase')!='40.13': raise RuntimeError('operator propagation evidence missing from ceiling registry')
 phase=load('verification/phase-40.13.json')
 for k in ['operatorReadsImmutablePhase40ConfigMap','distributedOperatorReadsImmutablePhase40ConfigMap','sharedOperatorCeilingContractImplemented','allTenExactCeilingsPropagated','referenceJobsReceiveCeilingEnvironment','distributedReasonerJobsReceiveCeilingEnvironment','nonReasonerDistributedJobsRemainUncoupled','ceilingBundleSha256Propagated','workSpecHashBindsCeilings','existingJobPolicyDriftRejected','referenceWorkerVerifiesOperatorBundleSha256']:
  if phase.get(k) is not True: raise RuntimeError(f'40.13 missing {k}')
 for k in ['nativeCargoQualificationExecuted','nativeHelmQualificationExecuted','liveRke2QualificationExecuted','standardsClaimsEnabled']:
  if phase.get(k) is not False: raise RuntimeError(f'40.13 overclaims {k}')
 r=load('verification/phase-40.13-requirements.json'); t=load('verification/phase-40.13-traceability.json')
 ids={x['id'] for x in r['requirements']}
 if ids!={f'P40-13-{i:03d}' for i in range(1,13)} or {x['requirementId'] for x in t['entries']}!=ids: raise RuntimeError('40.13 requirements/traceability incomplete')
 cap=load('verification/phase-40-capability-status.json')
 if cap['capabilities']['phase40OperatorCeilingPropagation']['status']!='implemented-static-qualified' or cap.get('standardsClaimsEnabled') is not False: raise RuntimeError('40.13 capability status invalid')
 gates=yaml.safe_load(text('acceptance/phase-gates.yaml'))['phases']; by={str(x['phase']):x for x in gates}
 if by.get('40.13',{}).get('command')!='scripts/qualify_phase40_13.sh': raise RuntimeError('40.13 acceptance gate missing')
 ev=load('verification/stabilization/phase-40.13.json'); embedded=ROOT/ev['embeddedParentManifest']
 if ev.get('parentLabel')!='phase-40.12' or ev.get('currentLabel')!='phase-40.13' or ev.get('deletedFiles')!=[]: raise RuntimeError('40.13 inheritance invalid')
 if not embedded.is_file() or sha(ev['embeddedParentManifest'])!=ev['parentFileManifestSha256']: raise RuntimeError('40.13 parent manifest mismatch')
 print('Phase 40.13 static verification passed; both operators propagate one checksum-bound exact ceiling bundle into reference/reasoner Jobs and reject policy drift')
 return 0
if __name__=='__main__':
 try: raise SystemExit(main())
 except Exception as e: print(f'phase 40.13 static verification failed: {e}',file=sys.stderr); raise SystemExit(1)
