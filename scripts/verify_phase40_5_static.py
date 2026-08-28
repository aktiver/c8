#!/usr/bin/env python3
"""Static/schema/runtime-wiring gate for Phase 40.5 OWL 2 DL profile/import qualification."""
from __future__ import annotations
import hashlib,json,pathlib,subprocess,sys,yaml
from jsonschema import Draft202012Validator,FormatChecker
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

def run_qualification_fixture(rel, should_pass):
    cp=subprocess.run([sys.executable,str(ROOT/'scripts/validate_owl_profile_qualification.py'),str(ROOT/rel)],cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    if (cp.returncode==0)!=should_pass:
        raise RuntimeError(f'qualification fixture expectation failed {rel}: {cp.stdout.strip()}')

def main():
    schema=load_json('contracts/owl-profile-qualification.schema.json')
    Draft202012Validator.check_schema(schema)
    if schema.get('additionalProperties') is not False or schema.get('properties',{}).get('formatVersion',{}).get('const')!=1:
        raise RuntimeError('OWL profile qualification schema is not strict formatVersion 1')
    run_qualification_fixture('test-corpus/phase40_5/owl-profile-qualification-valid.json',True)
    for rel in [
        'owl-profile-qualification-invalid-unresolved-count.json',
        'owl-profile-qualification-invalid-profile-evidence.json',
        'owl-profile-qualification-invalid-document-order.json',
        'owl-profile-qualification-invalid-import-target.json',
        'owl-profile-qualification-invalid-extra-field.json',
    ]:
        run_qualification_fixture('test-corpus/phase40_5/'+rel,False)

    report=load_json('contracts/reasoner-report.schema.json'); Draft202012Validator.check_schema(report)
    if report['properties']['formatVersion'].get('const')!=5 or 'owlProfileQualificationSha256' not in report['required'] or 'owlConsistencyQualificationSha256' not in report['required']:
        raise RuntimeError('reasoner report is not current Phase 40.6 formatVersion 5 retaining Phase 40.5 binding')
    if report['properties']['owlProfileQualificationSha256'].get('pattern')!='^[0-9a-f]{64}$':
        raise RuntimeError('reasoner report does not checksum-constrain profile qualification')

    require('crates/ngkg-reference/src/compiler.rs',
            'MultipleOntologyIris','MultipleVersionIris','MisplacedOntologyHeader',
            'output_owl_profile_qualification_path','reasoner/owl-profile-qualification.json',
            'owl_profile_qualification_sha256: Some','ontology_preflight_accepts_version_iri_import_alias')
    require('crates/ngkg-reference/src/model.rs',
            'pub struct OwlProfileQualification','pub struct OwlProfileOntologyDocument',
            'pub struct OwlProfileImportResolution','owl_profile_qualification_sha256')
    require('crates/ngkg-reference/src/reasoner.rs',
            'read_and_validate_owl_profile_qualification','complete_local_import_closure',
            'resolved import is not bound to the declared local ontology document',
            'report.format_version != 5','phase40_5_tests')
    require('adapters/hermit-reasoner/src/main/java/io/ngkg/reasoner/Main.java',
            'request.formatVersion() != 4','writeOwlProfileQualification',
            'OWLAPI ontology/version identity differs from checksum-bound preflight aliases',
            'owl:imports target was not loaded from its checksum-bound local document',
            'completeLocalImportClosure','new Report(\n                    5,')
    require('adapters/hermit-reasoner/src/test/java/io/ngkg/reasoner/MainTest.java',
            'resolvesVersionIriImportIntoChecksumBoundLocalClosure','owlProfileQualificationSha256')
    require('docs/OWL_PROFILE_IMPORT_QUALIFICATION.md','complete local import','Phase 40.6','not split across graph partitions')

    phase=load_json('verification/phase-40.5.json')
    for key in ['ontologyHeaderPreflightHardened','versionIriImportAliasResolutionImplemented','owlapiImportClosureEvidenceImplemented','combinedOwl2DlProfileEvidenceImplemented','profileQualificationShaBindingImplemented','independentRustQualificationVerificationImplemented']:
        if phase.get(key) is not True: raise RuntimeError(f'Phase 40.5 missing capability {key}')
    for key in ['consistencyQualificationHardened','directBgpLegalityImplemented','exactDirectReasonerFallbackImplemented','owlDirectArbitraryBgpCompleteness','standardsClaimsEnabled']:
        if phase.get(key) is not False: raise RuntimeError(f'Phase 40.5 overclaims {key}')

    req=load_json('verification/phase-40.5-requirements.json'); trace=load_json('verification/phase-40.5-traceability.json')
    ids={r['id'] for r in req['requirements']}
    if ids!={f'P40-5-{n:03d}' for n in range(1,8)}: raise RuntimeError('Phase 40.5 requirement set incomplete')
    if {r['requirementId'] for r in trace['entries']}!=ids: raise RuntimeError('Phase 40.5 traceability mismatch')
    for entry in trace['entries']:
        for rel in entry.get('implementation',[])+entry.get('evidence',[]):
            if not (ROOT/rel).is_file(): raise RuntimeError(f'traceability missing {rel}')

    registry=yaml.safe_load(require('acceptance/phase-gates.yaml'))['phases']; by={str(x['phase']):x for x in registry}
    if by.get('40.5',{}).get('command')!='scripts/qualify_phase40_5.sh': raise RuntimeError('Phase 40.5 acceptance entry missing')
    capability=load_json('verification/phase-40-capability-status.json')
    prof=capability['capabilities'].get('owlProfileImportQualificationEvidence',{})
    if prof.get('status')!='implemented-static-qualified' or capability.get('standardsClaimsEnabled') is not False:
        raise RuntimeError('global capability declaration invalid')

    evidence=load_json('verification/stabilization/phase-40.5.json')
    if evidence.get('parentLabel')!='phase-40.4' or evidence.get('currentLabel')!='phase-40.5' or evidence.get('deletedFiles')!=[]:
        raise RuntimeError('Phase 40.5 inheritance evidence invalid')
    embedded=ROOT/evidence.get('embeddedParentManifest','')
    if not embedded.is_file() or sha256(embedded)!=evidence.get('parentFileManifestSha256'):
        raise RuntimeError('Phase 40.5 parent manifest evidence invalid')
    # Phase 40.6+ descendants may legitimately modify files also changed in 40.5.
    # Historical 40.5 validates its preserved parent evidence; the descendant gate owns
    # current-tree ancestry against the actual Phase 40.5 archive.
    parent={}
    for line in embedded.read_text().splitlines():
        if line.strip():
            digest,path=line.split('  ',1)
            if len(digest)!=64 or path in parent: raise RuntimeError('Phase 40.5 embedded parent manifest is malformed')
            parent[path]=digest
    if len(parent)!=evidence.get('parentPayloadFileCount'): raise RuntimeError('Phase 40.5 parent manifest count mismatch')
    for changed in evidence.get('changedParentFiles',[]):
        path=changed.get('path','')
        if path not in parent or changed.get('parentSha256')!=parent[path] or len(changed.get('currentSha256',''))!=64:
            raise RuntimeError('Phase 40.5 preserved parent delta evidence is malformed')

    print('Phase 40.5 static verification passed; checksum-bound OWLAPI import closure and merged OWL 2 DL profile evidence are coherent')
    return 0
if __name__=='__main__':
    try: raise SystemExit(main())
    except (OSError,RuntimeError,ValueError,KeyError,json.JSONDecodeError,yaml.YAMLError) as exc:
        print(f'phase 40.5 static verification failed: {exc}',file=sys.stderr); raise SystemExit(1)
