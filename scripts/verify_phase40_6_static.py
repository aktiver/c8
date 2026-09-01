#!/usr/bin/env python3
"""Static/schema/runtime-wiring gate for Phase 40.6 global OWL 2 DL consistency qualification."""
from __future__ import annotations
import hashlib,json,pathlib,subprocess,sys,yaml
from jsonschema import Draft202012Validator
ROOT=pathlib.Path(__file__).resolve().parents[1]

def load_json(rel):
    p=ROOT/rel
    if not p.is_file(): raise RuntimeError(f'missing {rel}')
    return json.loads(p.read_text())
def sha256(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def require(rel,*tokens):
    p=ROOT/rel
    if not p.is_file(): raise RuntimeError(f'missing {rel}')
    text=p.read_text()
    for token in tokens:
        if token not in text: raise RuntimeError(f'{rel} missing {token}')
    return text
def fixture(rel, should_pass):
    cp=subprocess.run([sys.executable,str(ROOT/'scripts/validate_owl_consistency_qualification.py'),str(ROOT/rel)],cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    if (cp.returncode==0)!=should_pass: raise RuntimeError(f'fixture expectation failed {rel}: {cp.stdout.strip()}')

def main():
    schema=load_json('contracts/owl-consistency-qualification.schema.json'); Draft202012Validator.check_schema(schema)
    if schema.get('additionalProperties') is not False or schema['properties']['formatVersion'].get('const')!=1:
        raise RuntimeError('consistency schema is not strict formatVersion 1')
    fixture('test-corpus/phase40_6/owl-consistency-qualification-valid-consistent.json',True)
    fixture('test-corpus/phase40_6/owl-consistency-qualification-valid-inconsistent.json',True)
    for rel in ['owl-consistency-qualification-invalid-publication.json','owl-consistency-qualification-invalid-loaded-count.json','owl-consistency-qualification-invalid-unchecked.json','owl-consistency-qualification-invalid-extra-field.json']:
        fixture('test-corpus/phase40_6/'+rel,False)

    report=load_json('contracts/reasoner-report.schema.json'); Draft202012Validator.check_schema(report)
    if report['properties']['formatVersion'].get('const')!=5 or 'owlConsistencyQualificationSha256' not in report['required']:
        raise RuntimeError('reasoner report is not Phase 40.6 formatVersion 5')
    if report['properties']['owlConsistencyQualificationSha256'].get('pattern')!='^[0-9a-f]{64}$':
        raise RuntimeError('reasoner report does not checksum-constrain consistency qualification')

    require('crates/ngkg-reference/src/model.rs','pub struct OwlConsistencyQualification','output_owl_consistency_qualification_path','owl_consistency_qualification_sha256')
    require('crates/ngkg-reference/src/compiler.rs','reasoner/owl-consistency-qualification.json','output_owl_consistency_qualification_path','owlConsistencyQualificationSha256','owl_consistency_qualification_sha256: Some')
    require('crates/ngkg-reference/src/lib.rs','verify_semantic_qualification_bindings','reasoner/owl-consistency-qualification.json','incomplete semantic qualification binding chain')
    require('crates/ngkg-reference/src/reasoner.rs','read_and_validate_owl_consistency_qualification','consistency check did not cover the complete checksum-bound document set','report.format_version != 5','publication must equal consistency','phase40_6_tests')
    require('adapters/hermit-reasoner/src/main/java/io/ngkg/reasoner/Main.java','request.formatVersion() != 4','writeOwlConsistencyQualification','reasoner.isConsistent()','OWLReasoner.isConsistent','reject_snapshot','new Report(\n                    5,')
    require('adapters/hermit-reasoner/src/test/java/io/ngkg/reasoner/MainTest.java','emitsFailClosedEvidenceForGloballyInconsistentMergedOntology','owlConsistencyQualificationSha256','publicationPermitted')
    require('docs/OWL_CONSISTENCY_QUALIFICATION.md','Consistency is global','not split','Phase 40.7')

    phase=load_json('verification/phase-40.6.json')
    for key in ['globalMergedOntologyConsistencyEvidenceImplemented','consistencyQualificationShaBindingImplemented','independentRustConsistencyVerificationImplemented','inconsistentSnapshotPublicationRejected','perGraphConsistencySubstitutionProhibited']:
        if phase.get(key) is not True: raise RuntimeError(f'Phase 40.6 missing capability {key}')
    for key in ['directBgpLegalityImplemented','exactDirectReasonerFallbackImplemented','owlDirectArbitraryBgpCompleteness','standardsClaimsEnabled']:
        if phase.get(key) is not False: raise RuntimeError(f'Phase 40.6 overclaims {key}')

    req=load_json('verification/phase-40.6-requirements.json'); trace=load_json('verification/phase-40.6-traceability.json')
    ids={r['id'] for r in req['requirements']}
    if ids!={f'P40-6-{n:03d}' for n in range(1,8)}: raise RuntimeError('Phase 40.6 requirements incomplete')
    if {e['requirementId'] for e in trace['entries']}!=ids: raise RuntimeError('Phase 40.6 traceability mismatch')
    for entry in trace['entries']:
        for rel in entry.get('implementation',[])+entry.get('evidence',[]):
            if not (ROOT/rel).is_file(): raise RuntimeError(f'traceability missing {rel}')

    registry=yaml.safe_load(require('acceptance/phase-gates.yaml'))['phases']; by={str(x['phase']):x for x in registry}
    if by.get('40.6',{}).get('command')!='scripts/qualify_phase40_6.sh': raise RuntimeError('Phase 40.6 acceptance entry missing')
    cap=load_json('verification/phase-40-capability-status.json')
    cons=cap['capabilities'].get('owlConsistencyQualificationEvidence',{})
    if cons.get('status')!='implemented-static-qualified' or cap.get('standardsClaimsEnabled') is not False:
        raise RuntimeError('global capability declaration invalid')

    evidence=load_json('verification/stabilization/phase-40.6.json')
    if evidence.get('parentLabel')!='phase-40.5' or evidence.get('currentLabel')!='phase-40.6' or evidence.get('deletedFiles')!=[]:
        raise RuntimeError('Phase 40.6 inheritance evidence invalid')
    embedded=ROOT/evidence.get('embeddedParentManifest','')
    if not embedded.is_file() or sha256(embedded)!=evidence.get('parentFileManifestSha256'):
        raise RuntimeError('Phase 40.6 parent manifest evidence invalid')
    parent={}
    for line in embedded.read_text().splitlines():
        if line.strip(): digest,path=line.split('  ',1); parent[path]=digest
    changed=evidence.get('changedParentFiles')
    if not isinstance(changed,list) or changed!=sorted(changed,key=lambda row:row.get('path','')):
        raise RuntimeError('Phase 40.6 changed-parent evidence must be a sorted list')
    changed_by_path={}
    for row in changed:
        path=row.get('path'); parent_sha=row.get('parentSha256'); current_sha=row.get('currentSha256')
        if path not in parent or parent.get(path)!=parent_sha or not isinstance(current_sha,str) or len(current_sha)!=64:
            raise RuntimeError('Phase 40.6 changed-parent evidence row is invalid')
        changed_by_path[path]=current_sha

    # Descendant phases may legitimately edit Phase 40.6 files.  When an immutable final Phase
    # 40.6 manifest is present, validate the historical delta against those preserved release
    # bytes instead of re-hashing the descendant working tree.  Phase 40.7 binds this preserved
    # manifest as its parent manifest, so historical ancestry remains cryptographically closed.
    final_manifest=ROOT/'verification/stabilization/phase-40.6-parent-FILE_MANIFEST_SHA256.txt'
    if final_manifest.is_file():
        final={}
        for line in final_manifest.read_text().splitlines():
            if line.strip(): digest,path=line.split('  ',1); final[path]=digest
        for path,parent_sha in parent.items():
            expected=changed_by_path.get(path,parent_sha)
            if final.get(path)!=expected:
                raise RuntimeError(f'Phase 40.6 immutable release manifest disagrees for {path}')
    else:
        observed=[]; deleted=[]
        for path,digest in parent.items():
            current=ROOT/path
            if not current.is_file(): deleted.append(path); continue
            cur=sha256(current)
            if cur!=digest: observed.append({'path':path,'parentSha256':digest,'currentSha256':cur})
        observed.sort(key=lambda row:row['path'])
        if deleted!=evidence.get('deletedFiles') or observed!=changed:
            raise RuntimeError('Phase 40.6 parent delta evidence does not match current tree')
    print('Phase 40.6 static verification passed; global OWL 2 DL consistency is checksum-bound and fail-closed')
    return 0
if __name__=='__main__':
    try: raise SystemExit(main())
    except (OSError,RuntimeError,ValueError,KeyError,json.JSONDecodeError,yaml.YAMLError) as exc:
        print(f'phase 40.6 static verification failed: {exc}',file=sys.stderr); raise SystemExit(1)
