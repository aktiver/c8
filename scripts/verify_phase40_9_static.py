#!/usr/bin/env python3
from __future__ import annotations
import hashlib,json,pathlib,subprocess,sys,yaml
from jsonschema import Draft202012Validator
ROOT=pathlib.Path(__file__).resolve().parents[1]
def text(rel):
 p=ROOT/rel
 if not p.is_file(): raise RuntimeError(f'missing {rel}')
 return p.read_text()
def load(rel): return json.loads(text(rel))
def require(rel,*needles):
 t=text(rel)
 for n in needles:
  if n not in t: raise RuntimeError(f'{rel} missing {n!r}')
 return t
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''): h.update(b)
 return h.hexdigest()
def run(*args,ok=True):
 cp=subprocess.run(args,cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 if (cp.returncode==0)!=ok: raise RuntimeError(f'command expectation failed: {args}: {cp.stdout.strip()}')
 return cp.stdout
def main():
 for rel in ['contracts/direct-proof-manifest.schema.json','contracts/direct-certificate.schema.json','contracts/direct-exact-partition-result.schema.json','contracts/direct-exact-job.schema.json']:
  Draft202012Validator.check_schema(load(rel))
 run(sys.executable,'scripts/validate_direct_proof.py','test-corpus/phase40_9/direct-proof-manifest-valid.json','--result','test-corpus/phase40_9/direct-bgp-result-valid.json','--certificate','test-corpus/phase40_9/direct-certificate-valid.json')
 run(sys.executable,'scripts/validate_direct_proof.py','test-corpus/phase40_9/direct-proof-manifest-zero-answer.json','--result','test-corpus/phase40_9/direct-bgp-result-zero-answer.json','--certificate','test-corpus/phase40_9/direct-certificate-zero-answer.json')
 run(sys.executable,'scripts/validate_direct_proof.py','test-corpus/phase40_9/direct-proof-manifest-invalid-support-id.json','--result','test-corpus/phase40_9/direct-bgp-result-valid.json','--certificate','test-corpus/phase40_9/direct-certificate-valid.json',ok=False)
 run(sys.executable,'scripts/validate_direct_proof.py','test-corpus/phase40_9/direct-proof-manifest-invalid-multiplicity.json','--result','test-corpus/phase40_9/direct-bgp-result-valid.json','--certificate','test-corpus/phase40_9/direct-certificate-valid.json',ok=False)
 run(sys.executable,'scripts/validate_direct_exact.py','test-corpus/phase40_9/direct-exact-result-valid.json')
 run(sys.executable,'scripts/validate_direct_exact.py','test-corpus/phase40_9/direct-exact-job-valid.json')
 require('crates/ngkg-types/src/direct_proof.rs','DirectProofManifest','DirectReasonerCheckProof','direct_reasoner_support_id','direct_completion_support_id','validate_direct_proof_bundle','ResultCoverage','completion_support_id')
 require('crates/ngkg-types/src/direct_certificate.rs','PROOF_FORMAT_VERSION','proof_manifest_sha256','DirectProofCoverage::Complete','DirectSupportKind::ReasonerCheck')
 require('crates/ngkg-types/src/direct_exact.rs','grounded_rdf_sha256','logical_axioms_sha256','logical_axiom_count')
 require('adapters/hermit-reasoner/src/main/java/io/ngkg/reasoner/DirectBgpExecutor.java','ADAPTER_VERSION = "40.9"','groundedRdfSha256','logicalAxiomsSha256','canonicalLogicalAxiomsSha256','isEntailed(grounded.logicalAxioms())')
 require('crates/ngkg-direct-reasoner/src/lib.rs','DirectProofCoverage::Complete','DirectProofManifest','direct_binding_sha256','proof_manifest_sha256','direct_completion_support_id','validate_direct_proof_bundle','format_version: 2')
 require('services/reference-worker/src/direct_job.rs','output_proof_manifest_path','proofManifest','reasoner_adapter_version != "40.9"','proof_manifest_sha256')
 require('crates/ngkg-reasoner-client/src/lib.rs','validate_direct_proof_binding','DirectProofBindingMismatch','validate_direct_proof_bundle')
 manifest=load('test-corpus/phase40_9/direct-proof-manifest-valid.json')
 if manifest['completionSupportId']!='5a88190a11147a22496c113978af780ab8a600e1a3656054863de2f7db601471': raise RuntimeError('completion support test vector changed')
 if [x['supportId'] for x in manifest['answerProofs']]!=['17851afe006f5a9d9bec2172b2772dd6db39d1bed7206b151ea59ca183f521e7','284619b080d7738795815910664026315ac1f2c058b683a8ad0758447ab917ca']: raise RuntimeError('answer support test vectors changed')
 phase=load('verification/phase-40.9.json')
 for key in ['perEntailedCandidateReasonerCheckEvidenceImplemented','groundedRdfAndLogicalAxiomHashesImplemented','proofManifestRuntimeContractImplemented','answerMultiplicityProofCoverageImplemented','zeroAnswerCompletionSupportImplemented','directCertificateV2ProofBindingImplemented','reasonerClientProofVerificationImplemented','referenceWorkerProofManifestEmissionImplemented','deterministicProofSupportIdsImplemented','hpcProofAggregationDeterministic']:
  if phase.get(key) is not True: raise RuntimeError(f'Phase 40.9 missing {key}')
 for key in ['hermitDerivationDagImplemented','standardsClaimsEnabled','nativeCargoMavenQualificationExecuted']:
  if phase.get(key) is not False: raise RuntimeError(f'Phase 40.9 overclaims {key}')
 req=load('verification/phase-40.9-requirements.json'); trace=load('verification/phase-40.9-traceability.json'); ids={x['id'] for x in req['requirements']}
 if ids!={f'P40-9-{i:03d}' for i in range(1,13)} or {x['requirementId'] for x in trace['entries']}!=ids: raise RuntimeError('40.9 requirements/traceability incomplete')
 for e in trace['entries']:
  for rel in e.get('implementation',[])+e.get('evidence',[]):
   if not (ROOT/rel).is_file(): raise RuntimeError(f'traceability file missing: {rel}')
 reg=yaml.safe_load(text('acceptance/phase-gates.yaml'))['phases']; by={str(x['phase']):x for x in reg}
 if by.get('40.9',{}).get('command')!='scripts/qualify_phase40_9.sh': raise RuntimeError('40.9 acceptance gate missing')
 cap=load('verification/phase-40-capability-status.json')
 if 'answer-support-coverage-implemented' not in cap['capabilities']['directProofDag']['status'] or cap.get('standardsClaimsEnabled') is not False: raise RuntimeError('40.9 capability declaration invalid')
 ev=load('verification/stabilization/phase-40.9.json')
 if ev.get('parentLabel')!='phase-40.8' or ev.get('currentLabel')!='phase-40.9' or ev.get('deletedFiles')!=[]: raise RuntimeError('40.9 inheritance invalid')
 embedded=ROOT/ev['embeddedParentManifest']
 if not embedded.is_file() or sha(embedded)!=ev['parentFileManifestSha256']: raise RuntimeError('40.9 parent manifest mismatch')
 parent={}
 for line in embedded.read_text().splitlines():
  if line.strip(): digest,path=line.split('  ',1); parent[path]=digest
 # Reconstruct the immutable Phase 40.9 payload hashes from the Phase 40.8 parent plus
 # the recorded 40.9 delta. Descendants may legitimately edit those same files.
 expected=dict(parent)
 for item in ev['changedParentFiles']:
  if expected.get(item['path'])!=item['parentSha256']: raise RuntimeError('40.9 recorded parent SHA mismatch')
  expected[item['path']]=item['currentSha256']
 for path in ev['deletedFiles']: expected.pop(path,None)
 preserved=sorted((ROOT/'verification/stabilization').glob('phase-40.9-parent-for-*-FILE_MANIFEST_SHA256.txt'))
 if preserved:
  manifest={}
  for line in preserved[0].read_text().splitlines():
   if line.strip(): digest,path=line.split('  ',1); manifest[path]=digest
  for path,digest in expected.items():
   if manifest.get(path)!=digest: raise RuntimeError(f'40.9 immutable descendant manifest mismatch for {path}')
 else:
  observed=[]; deleted=[]
  for path,digest in parent.items():
   current=ROOT/path
   if not current.is_file(): deleted.append(path); continue
   cur=sha(current)
   if cur!=digest: observed.append({'path':path,'parentSha256':digest,'currentSha256':cur})
  observed.sort(key=lambda x:x['path']); deleted.sort()
  if observed!=ev['changedParentFiles'] or deleted!=ev['deletedFiles']: raise RuntimeError('40.9 parent delta mismatch')
 print('Phase 40.9 static verification passed; exact answers are proof-manifest bound with complete multiplicity support coverage')
 return 0
if __name__=='__main__':
 try: raise SystemExit(main())
 except Exception as exc: print(f'phase 40.9 static verification failed: {exc}',file=sys.stderr); raise SystemExit(1)
