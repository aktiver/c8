#!/usr/bin/env python3
from __future__ import annotations
import hashlib, json, subprocess, sys
from pathlib import Path
import yaml

ROOT=Path(__file__).resolve().parents[1]

def text(rel): return (ROOT/rel).read_text()
def load(rel): return json.loads(text(rel))
def sha(path): return hashlib.sha256((ROOT/path).read_bytes()).hexdigest()
def req(rel,*needles):
 s=text(rel)
 for n in needles:
  if n not in s: raise RuntimeError(f'{rel} missing required Phase 40.11 token {n!r}')
 return s

def main():
 cp=subprocess.run([sys.executable,str(ROOT/'scripts/validate_phase40_helm_ceilings.py'),'--root',str(ROOT),'--report',str(ROOT/'qualification/phase40.11-helm-ceilings.json')],cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 if cp.returncode: raise RuntimeError(cp.stdout)
 bad=subprocess.run([sys.executable,str(ROOT/'scripts/validate_phase40_helm_ceilings.py'),'--root',str(ROOT),'--platform-overlay',str(ROOT/'test-corpus/phase40_11/platform-invalid-exact-partitions.yaml')],cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 if bad.returncode==0: raise RuntimeError('unsafe maxExactPartitions overlay was accepted')
 pv=yaml.safe_load(text('charts/ngkg-platform/values.yaml'))['phase40']['direct']
 expected={
  'maxCandidateBindings':'NGKG_PHASE40_DIRECT_MAX_CANDIDATE_BINDINGS',
  'maxPartitionCandidates':'NGKG_PHASE40_DIRECT_MAX_PARTITION_CANDIDATES',
  'maxExactPartitions':'NGKG_PHASE40_DIRECT_MAX_EXACT_PARTITIONS',
  'maxGroundedAxiomsPerCandidate':'NGKG_PHASE40_DIRECT_MAX_GROUNDED_AXIOMS_PER_CANDIDATE',
  'maxGroundedRdfBytesPerCandidate':'NGKG_PHASE40_DIRECT_MAX_GROUNDED_RDF_BYTES_PER_CANDIDATE',
  'reasonerConcurrency':'NGKG_PHASE40_DIRECT_REASONER_CONCURRENCY',
  'reasonerHeapMiBPerLane':'NGKG_PHASE40_DIRECT_REASONER_HEAP_MIB_PER_LANE',
  'reasonerTimeoutSeconds':'NGKG_PHASE40_DIRECT_REASONER_TIMEOUT_SECONDS',
  'maxCertificateBytes':'NGKG_PHASE40_DIRECT_MAX_CERTIFICATE_BYTES',
  'maxProofSupportIds':'NGKG_PHASE40_DIRECT_MAX_PROOF_SUPPORT_IDS',
 }
 template=req('charts/ngkg-platform/templates/phase40-reference-ceilings.yaml','kind: ConfigMap','immutable: true','ngkg.io/phase: "40.11"')
 runtime=req('services/reference-worker/src/phase40_limits.rs','TrustedPhase40DirectCeilings','from_env','enforce_job','available_parallelism','cgroup_memory_limit_bytes','saturating_mul(80) / 100','bundle_sha256','ngkg-phase40-reference-worker-ceilings-v1')
 for helm,env in expected.items():
  if f'.Values.phase40.direct.{helm}' not in template: raise RuntimeError(f'ConfigMap omits {helm}')
  if env not in template or env not in runtime: raise RuntimeError(f'Phase 40.11 runtime mapping omits {env}')
 req('services/reference-worker/src/direct_job.rs','TrustedPhase40DirectCeilings::from_env','trusted_phase40.enforce_job(&job.limits)','max_exact_partitions: trusted_phase40.max_exact_partitions','max_certificate_bytes: trusted_phase40.max_certificate_bytes','max_proof_support_ids: trusted_phase40.max_proof_support_ids','trustedPhase40CeilingsSha256')
 req('crates/ngkg-direct-reasoner/src/lib.rs','pub max_exact_partitions: u64','pub max_certificate_bytes: u64','pub max_proof_support_ids: u64','partition_count > limits.max_exact_partitions','required_support_ids > limits.max_proof_support_ids','max_certificate_bytes','ResourceCeiling("maxProofSupportIds")','ResourceCeiling("maxCertificateBytes")')
 # Historical 40.11 required propagation to remain deferred; 40.13 is the strict descendant that completes it.
 if not (ROOT/'verification/phase-40.13.json').is_file():
  for rel in ['charts/ngkg-platform/templates/operator.yaml','charts/ngkg-platform/templates/distributed-operator.yaml','services/operator/src/main.rs','services/distributed-operator/src/main.rs']:
   s=text(rel)
   if 'NGKG_PHASE40_DIRECT_MAX_CANDIDATE_BINDINGS' in s or 'phase40-reference-ceilings' in s:
    raise RuntimeError(f'{rel} prematurely implements Phase 40.13 propagation')
 reg=load('verification/phase-40-ceilings.json')
 if reg.get('phase') not in {'40.11','40.12','40.13'}: raise RuntimeError('ceiling registry is not a valid Phase 40.11 descendant')
 platform=[x for x in reg['phase40HelmDeclared'] if x['helmChart']=='ngkg-platform']
 if len(platform)!=10: raise RuntimeError('expected ten platform Direct ceilings')
 for x in platform:
  if x.get('status') not in {'reference-worker-enforced-operator-propagation-phase-40.13','reference-worker-enforced-operator-propagated-static-qualified'}: raise RuntimeError(f"{x['id']} not marked reference-worker enforced")
  if not x.get('runtimeEnv','').startswith('NGKG_PHASE40_DIRECT_'): raise RuntimeError(f"{x['id']} missing trusted runtime env")
 phase=load('verification/phase-40.11.json')
 for k in ['authoritativeHelmCeilingsPreserved','immutableReferenceWorkerConfigMapImplemented','referenceWorkerTrustedEnvironmentRequired','perJobSubCeilingEnforcementImplemented','candidatePartitionCrossFieldValidationImplemented','cpuVisibilityEnforcementImplemented','cgroupMemoryHeadroomEnforcementImplemented','maxExactPartitionsRuntimeEnforced','maxCertificateBytesRuntimeEnforced','maxProofSupportIdsRuntimeEnforced']:
  if phase.get(k) is not True: raise RuntimeError(f'40.11 missing {k}')
 for k in ['operatorConfigMapPropagationImplemented','distributedOperatorConfigMapPropagationImplemented','nativeHelmQualificationExecuted','standardsClaimsEnabled']:
  if phase.get(k) is not False: raise RuntimeError(f'40.11 overclaims {k}')
 r=load('verification/phase-40.11-requirements.json'); t=load('verification/phase-40.11-traceability.json')
 ids={x['id'] for x in r['requirements']}
 if ids!={f'P40-11-{i:03d}' for i in range(1,13)} or {x['requirementId'] for x in t['entries']}!=ids: raise RuntimeError('40.11 requirements/traceability incomplete')
 for e in t['entries']:
  for rel in e.get('implementation',[])+e.get('evidence',[]):
   if rel=='verification/stabilization/phase-40.11.json': continue
   if not (ROOT/rel).is_file(): raise RuntimeError(f'traceability file missing: {rel}')
 cap=load('verification/phase-40-capability-status.json')
 if cap['capabilities']['phase40ReferenceWorkerCeilings']['status']!='implemented-static-qualified' or cap.get('standardsClaimsEnabled') is not False: raise RuntimeError('40.11 capability status invalid')
 gates=yaml.safe_load(text('acceptance/phase-gates.yaml'))['phases']; by={str(x['phase']):x for x in gates}
 if by.get('40.11',{}).get('command')!='scripts/qualify_phase40_11.sh': raise RuntimeError('40.11 acceptance gate missing')
 ev=load('verification/stabilization/phase-40.11.json'); embedded=ROOT/ev['embeddedParentManifest']
 if ev.get('parentLabel')!='phase-40.10' or ev.get('currentLabel')!='phase-40.11' or ev.get('deletedFiles')!=[]: raise RuntimeError('40.11 inheritance invalid')
 if not embedded.is_file() or sha(ev['embeddedParentManifest'])!=ev['parentFileManifestSha256']: raise RuntimeError('40.11 parent manifest mismatch')
 # Preserved 40.10 governance copies must match the exact 40.10 parent manifest.
 manifest={line.split('  ',1)[1]:line.split('  ',1)[0] for line in embedded.read_text().splitlines() if '  ' in line}
 for current,parent_rel in [('verification/parents/phase-40.10-ceilings.json','verification/phase-40-ceilings.json'),('verification/parents/phase-40.10-capability-status.json','verification/phase-40-capability-status.json')]:
  if manifest.get(parent_rel)!=sha(current): raise RuntimeError(f'preserved 40.10 governance copy mismatch: {current}')
 print('Phase 40.11 static verification passed; reference-worker consumes trusted Helm ceilings, enforces HPC/resource sub-ceilings, and operator propagation remains deferred')
 return 0
if __name__=='__main__':
 try: raise SystemExit(main())
 except Exception as e: print(f'phase 40.11 static verification failed: {e}',file=sys.stderr); raise SystemExit(1)
