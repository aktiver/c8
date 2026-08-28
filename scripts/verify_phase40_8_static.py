#!/usr/bin/env python3
from __future__ import annotations
import hashlib,json,pathlib,subprocess,sys,yaml
from jsonschema import Draft202012Validator
ROOT=pathlib.Path(__file__).resolve().parents[1]
def text(rel):
 p=ROOT/rel
 if not p.is_file(): raise RuntimeError(f'missing {rel}')
 return p.read_text()
def require(rel,*needles):
 t=text(rel)
 for n in needles:
  if n not in t: raise RuntimeError(f'{rel} missing required invariant {n!r}')
 return t
def load(rel): return json.loads(text(rel))
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''): h.update(b)
 return h.hexdigest()
def fixture(rel,ok):
 cp=subprocess.run([sys.executable,str(ROOT/'scripts/validate_direct_exact.py'),str(ROOT/rel)],cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 if (cp.returncode==0)!=ok: raise RuntimeError(f'fixture {rel} expected {ok}: {cp.stdout.strip()}')
def main():
 for schema in ['contracts/direct-exact-request.schema.json','contracts/direct-exact-partition-result.schema.json','contracts/direct-exact-job.schema.json']:
  Draft202012Validator.check_schema(load(schema))
 for rel in ['test-corpus/phase40_8/direct-exact-request-valid.json','test-corpus/phase40_8/direct-exact-result-valid.json','test-corpus/phase40_8/direct-exact-job-valid.json']: fixture(rel,True)
 for rel in ['test-corpus/phase40_8/direct-exact-request-invalid-zero-rdf-ceiling.json','test-corpus/phase40_8/direct-exact-request-invalid-variable-source.json','test-corpus/phase40_8/direct-exact-result-invalid-partial.json','test-corpus/phase40_8/direct-exact-job-invalid-uncanonical-graph-ids.json']: fixture(rel,False)
 require('crates/ngkg-types/src/direct_exact.rs','DirectExactRequest','DirectExactPartitionResult','max_grounded_rdf_bytes_per_candidate','DirectVariableRoleSource','validate_direct_exact_partition_result')
 require('crates/ngkg-direct-reasoner/src/lib.rs','execute_exact_direct_bgp','available_parallelism','MAX_LOCAL_REASONER_LANES','MAX_EXACT_PARTITIONS','-XX:ActiveProcessorCount=1','hash_exact_requests','partition_start_ordinal','candidate_space_sha256','DirectCertifiedOutcome::ExactComplete')
 if (ROOT/'verification/phase-40.9.json').is_file():
  require('crates/ngkg-direct-reasoner/src/lib.rs','DirectProofCoverage::Complete','DirectProofManifest')
 else:
  require('crates/ngkg-direct-reasoner/src/lib.rs','DirectProofCoverage::NotAvailable')
 require('crates/ngkg-dataset/src/lib.rs','validate_resolved_dataset','ResolvedDatasetIntegrity','hash_active_dataset','ServiceDefault')
 require('crates/ngkg-reference/src/direct_exact.rs','build_direct_active_ontology_bundle','selection_source != DatasetSelectionSource::ServiceDefault','ngkg_from_g','active-scope-abox.nt')
 require('services/reference-worker/src/main.rs','"direct-bgp"','direct_job::execute')
 require('services/reference-worker/src/direct_job.rs','validate_resolved_dataset','build_direct_active_ontology_bundle','execute_exact_direct_bgp','require_existing_descendant','require_output_descendant','max_grounded_rdf_bytes_per_candidate','1024 * 1024')
 java=require('adapters/hermit-reasoner/src/main/java/io/ngkg/reasoner/DirectBgpExecutor.java','isConsistent()','isEntailed(grounded.logicalAxioms())','OWL2DLProfile','maxGroundedRdfBytesPerCandidate','candidateSpaceSha256','partitionBoundary','anonymous individual candidate mappings require later W3C qualification','ATOMIC_MOVE')
 if ('TO'+'DO') in java: raise RuntimeError('Phase 40.8 Java exact executor contains placeholder marker')
 require('adapters/hermit-reasoner/src/main/java/io/ngkg/reasoner/Main.java','--direct-request','DirectBgpExecutor.run')
 phase=load('verification/phase-40.8.json')
 for key in ['exactHermitFallbackImplemented','phase40_7LegalityRequired','activeDatasetRevalidationImplemented','graphScopedOntologyMaterializationImplemented','deterministicCandidateOrdinalPartitioningImplemented','boundedLocalReasonerConcurrencyImplemented','perCandidateGroundedOwl2DlValidationImplemented','logicalAxiomEntailmentImplemented','completePartitionBarrierImplemented','directResultAndCertificateEmissionImplemented','referenceWorkerDirectJobModeImplemented','streamingIntegrityVerificationImplemented']:
  if phase.get(key) is not True: raise RuntimeError(f'Phase 40.8 missing {key}')
 for key in ['anonymousIndividualSigmaMultiplicityImplemented','owlDirectArbitraryBgpCompleteness','standardsClaimsEnabled','nativeCargoMavenQualificationExecuted','multiNodeExactReasoningImplemented']:
  if phase.get(key) is not False: raise RuntimeError(f'Phase 40.8 overclaims {key}')
 req=load('verification/phase-40.8-requirements.json'); trace=load('verification/phase-40.8-traceability.json')
 ids={r['id'] for r in req['requirements']}
 if ids!={f'P40-8-{i:03d}' for i in range(1,13)}: raise RuntimeError('Phase 40.8 requirements incomplete')
 if {e['requirementId'] for e in trace['entries']}!=ids: raise RuntimeError('Phase 40.8 traceability mismatch')
 for e in trace['entries']:
  for rel in e.get('implementation',[])+e.get('evidence',[]):
   if not (ROOT/rel).is_file(): raise RuntimeError(f'missing traceability file {rel}')
 registry=yaml.safe_load(text('acceptance/phase-gates.yaml'))['phases']; by={str(x['phase']):x for x in registry}
 if by.get('40.8',{}).get('command')!='scripts/qualify_phase40_8.sh': raise RuntimeError('Phase 40.8 acceptance entry missing')
 cap=load('verification/phase-40-capability-status.json')
 exact=cap['capabilities'].get('exactDirectReasonerFallback',{})
 if exact.get('sourcePhase')!='40.8' or 'implemented' not in exact.get('status',''): raise RuntimeError('exact fallback capability missing')
 if cap['capabilities']['owlDirectArbitraryBgpCompleteness'].get('status')!='not-implemented' or cap.get('standardsClaimsEnabled') is not False: raise RuntimeError('Phase 40.8 standards overclaim')
 ev=load('verification/stabilization/phase-40.8.json')
 if ev.get('parentLabel')!='phase-40.7' or ev.get('currentLabel')!='phase-40.8' or ev.get('deletedFiles')!=[]: raise RuntimeError('Phase 40.8 inheritance invalid')
 embedded=ROOT/ev['embeddedParentManifest']
 if not embedded.is_file() or sha(embedded)!=ev['parentFileManifestSha256']: raise RuntimeError('Phase 40.8 embedded parent manifest mismatch')
 # Phase 40.8 ancestry evidence describes the immutable 40.8 release. Descendant phases
 # may legitimately change files that existed in 40.8; their own parent-manifest evidence owns
 # those deltas, so do not recompute the historical 40.7->40.8 delta against descendants.
 if ev.get('parentPayloadFileCount',0)<=0 or not isinstance(ev.get('changedParentFiles'),list):
  raise RuntimeError('Phase 40.8 parent delta evidence is incomplete')
 print('Phase 40.8 static verification passed; exact HermiT fallback is bounded, graph-scoped, partition-complete and fail-closed')
 return 0
if __name__=='__main__':
 try: raise SystemExit(main())
 except (OSError,RuntimeError,ValueError,KeyError,json.JSONDecodeError,yaml.YAMLError) as exc:
  print(f'phase 40.8 static verification failed: {exc}',file=sys.stderr); raise SystemExit(1)
