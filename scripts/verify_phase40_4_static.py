#!/usr/bin/env python3
"""Static/schema/runtime-wiring gate for Phase 40.4 Direct certificates."""
from __future__ import annotations
import hashlib,json,pathlib,subprocess,sys,yaml
from jsonschema import Draft202012Validator
ROOT=pathlib.Path(__file__).resolve().parents[1]

def load_json(rel): return json.loads((ROOT/rel).read_text())
def sha256(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def require(rel,*tokens):
    p=ROOT/rel
    if not p.is_file(): raise RuntimeError(f'missing {rel}')
    text=p.read_text()
    for token in tokens:
        if token not in text: raise RuntimeError(f'{rel} missing {token}')
    return text

def run_validator(rel,expect,result=None):
    cmd=[sys.executable,str(ROOT/'scripts/validate_direct_certificate.py'),str(ROOT/rel)]
    if result: cmd += ['--result',str(ROOT/result)]
    cp=subprocess.run(cmd,cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    if (cp.returncode==0)!=expect: raise RuntimeError(f'certificate fixture expectation failed {rel}: {cp.stdout.strip()}')

def main():
    schema=load_json('contracts/direct-certificate.schema.json'); Draft202012Validator.check_schema(schema)
    run_validator('test-corpus/phase40_4/direct-certificate-valid.json',True,'test-corpus/phase40_3/direct-bgp-result-valid-complete.json')
    run_validator('test-corpus/phase40_4/direct-certificate-invalid-result-digest.json',False,'test-corpus/phase40_3/direct-bgp-result-valid-complete.json')
    for rel in ['direct-certificate-invalid-incomplete-partitions.json','direct-certificate-invalid-proof-coverage.json','direct-certificate-invalid-support-order.json','direct-certificate-invalid-extra-field.json']:
        run_validator('test-corpus/phase40_4/'+rel,False)
    require('crates/ngkg-types/src/direct_certificate.rs','DirectCertificate','DirectCompletenessEvidence','direct_bgp_result_sha256','RESULT_DIGEST_DOMAIN','solution_hashes.sort_unstable()','completed_partition_count != evidence.partition_count','proof_coverage == DirectProofCoverage::Complete','phase40_4_tests','35e90b74cd86849ed8ed5877088ef32ffdac9642c11fab422c470ff31171475f')
    require('crates/ngkg-reasoner-client/src/lib.rs','validate_direct_certificate_binding','DirectCertificateBindingMismatch','validate_direct_certificate_result(certificate, result).is_err()')
    require('docs/DIRECT_CERTIFICATE_CONTRACT.md','scheduling-independent','Phase 40.9','exhaustive-candidate-entailment')
    phase=load_json('verification/phase-40.4.json')
    for key in ['directCertificateRuntimeContractImplemented','schedulingIndependentResultDigestImplemented','exhaustiveCompletenessEvidenceContractImplemented','reasonerBoundaryCertificateVerificationImplemented','proofSupportVocabularyImplemented']:
        if phase.get(key) is not True: raise RuntimeError(f'Phase 40.4 missing capability {key}')
    for key in ['proofSupportRuntimeWiringImplemented','directBgpLegalityImplemented','exactDirectReasonerFallbackImplemented','owlDirectArbitraryBgpCompleteness','standardsClaimsEnabled']:
        if phase.get(key) is not False: raise RuntimeError(f'Phase 40.4 overclaims {key}')
    req=load_json('verification/phase-40.4-requirements.json'); trace=load_json('verification/phase-40.4-traceability.json')
    ids={r['id'] for r in req['requirements']}
    if ids!={f'P40-4-{n:03d}' for n in range(1,8)}: raise RuntimeError('Phase 40.4 requirement set incomplete')
    if {r['requirementId'] for r in trace['entries']}!=ids: raise RuntimeError('Phase 40.4 traceability mismatch')
    for entry in trace['entries']:
        for rel in entry.get('implementation',[])+entry.get('evidence',[]):
            if not (ROOT/rel).is_file(): raise RuntimeError(f'traceability missing {rel}')
    registry=yaml.safe_load(require('acceptance/phase-gates.yaml'))['phases']; by={str(x['phase']):x for x in registry}
    if by.get('40.4',{}).get('command')!='scripts/qualify_phase40_4.sh': raise RuntimeError('Phase 40.4 acceptance entry missing')
    capability=load_json('verification/phase-40-capability-status.json')
    direct=capability['capabilities'].get('directCertificateContract',{})
    if direct.get('status')!='implemented-static-qualified' or capability.get('standardsClaimsEnabled') is not False: raise RuntimeError('global capability declaration invalid')
    evidence=load_json('verification/stabilization/phase-40.4.json')
    if evidence.get('parentLabel')!='phase-40.3' or evidence.get('currentLabel')!='phase-40.4' or evidence.get('deletedFiles')!=[]: raise RuntimeError('Phase 40.4 inheritance evidence invalid')
    embedded=ROOT/evidence.get('embeddedParentManifest','')
    if not embedded.is_file() or sha256(embedded)!=evidence.get('parentFileManifestSha256'): raise RuntimeError('Phase 40.4 parent manifest evidence invalid')
    # Phase 40.5+ descendants are allowed to modify files that Phase 40.4 also changed.
    # Historical Phase 40.4 now validates its immutable parent evidence itself; the
    # descendant phase gate owns current-tree ancestry against the actual 40.4 ZIP.
    parent={}
    for line in embedded.read_text().splitlines():
        if line.strip():
            digest,path=line.split('  ',1)
            if len(digest)!=64 or path in parent: raise RuntimeError('Phase 40.4 embedded parent manifest is malformed')
            parent[path]=digest
    if len(parent)!=evidence.get('parentPayloadFileCount'): raise RuntimeError('Phase 40.4 parent manifest count mismatch')
    for changed in evidence.get('changedParentFiles',[]):
        path=changed.get('path','')
        if path not in parent or changed.get('parentSha256')!=parent[path] or len(changed.get('currentSha256',''))!=64:
            raise RuntimeError('Phase 40.4 preserved parent delta evidence is malformed')
    print('Phase 40.4 static verification passed; Direct certificate/result-digest/completeness/reasoner/support contract is coherent')
    return 0
if __name__=='__main__':
    try: raise SystemExit(main())
    except (OSError,RuntimeError,ValueError,KeyError,json.JSONDecodeError,yaml.YAMLError) as exc:
        print(f'phase 40.4 static verification failed: {exc}',file=sys.stderr); raise SystemExit(1)
