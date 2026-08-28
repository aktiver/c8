#!/usr/bin/env python3
"""Static/schema/runtime-wiring gate for Phase 40.2 datatype policy."""
from __future__ import annotations
import hashlib,json,pathlib,subprocess,sys,yaml
from jsonschema import Draft202012Validator,FormatChecker
ROOT=pathlib.Path(__file__).resolve().parents[1]

def load_json(rel:str):
    p=ROOT/rel
    if not p.is_file(): raise RuntimeError(f"missing {rel}")
    return json.loads(p.read_text(encoding='utf-8'))

def require(rel:str,*tokens:str)->str:
    p=ROOT/rel
    if not p.is_file(): raise RuntimeError(f"missing {rel}")
    text=p.read_text(encoding='utf-8')
    for token in tokens:
        if token not in text: raise RuntimeError(f"{rel} missing {token}")
    return text

def sha256(path:pathlib.Path)->str:
    h=hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda:handle.read(1024*1024),b''): h.update(block)
    return h.hexdigest()

def fixture(schema:dict,rel:str,should_pass:bool)->None:
    value=load_json(rel)
    errors=list(Draft202012Validator(schema,format_checker=FormatChecker()).iter_errors(value))
    if should_pass and errors: raise RuntimeError(f"{rel} unexpectedly failed schema: {errors[0].message}")
    if not should_pass and not errors: raise RuntimeError(f"{rel} unexpectedly passed schema")

def run_policy(rel:str,should_pass:bool)->None:
    completed=subprocess.run([sys.executable,str(ROOT/'scripts/validate_datatype_policy.py'),str(ROOT/rel)],cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    if (completed.returncode==0) != should_pass:
        raise RuntimeError(f"datatype policy validator mismatch for {rel}: {completed.stdout.strip()}")

def main()->int:
    requirements=load_json('verification/phase-40.2-requirements.json')
    ids={row['id'] for row in requirements['requirements']}
    if ids != {f'P40-2-{n:03d}' for n in range(1,8)}: raise RuntimeError('Phase 40.2 requirement set is incomplete')
    trace=load_json('verification/phase-40.2-traceability.json')
    if {row['requirementId'] for row in trace['entries']} != ids: raise RuntimeError('Phase 40.2 traceability is incomplete')
    for row in trace['entries']:
        for rel in row['implementation']+row['evidence']:
            if not (ROOT/rel).is_file(): raise RuntimeError(f'traceability references missing {rel}')

    schema=load_json('contracts/datatype-policy.schema.json')
    Draft202012Validator.check_schema(schema)
    if schema.get('additionalProperties') is not False or schema.get('properties',{}).get('formatVersion',{}).get('const') != 1:
        raise RuntimeError('datatype policy schema is not a strict version 1 contract')
    fixture(schema,'test-corpus/phase40_2/datatype-policy-valid.json',True)
    fixture(schema,'test-corpus/phase40_2/datatype-policy-invalid-duplicate.json',False)
    fixture(schema,'test-corpus/phase40_2/datatype-policy-invalid-lexical-space.json',False)
    fixture(schema,'test-corpus/phase40_2/datatype-policy-invalid-extra-field.json',False)
    if (ROOT/'policies/owl-direct-datatype-policy.json').read_bytes() != (ROOT/'test-corpus/phase40_2/datatype-policy-valid.json').read_bytes():
        raise RuntimeError('valid fixture must be the exact deployed datatype policy bytes')
    run_policy('policies/owl-direct-datatype-policy.json',True)
    run_policy('test-corpus/phase40_2/datatype-policy-invalid-duplicate.json',False)
    run_policy('test-corpus/phase40_2/datatype-policy-invalid-lexical-space.json',False)

    policy=load_json('policies/owl-direct-datatype-policy.json')
    iris=[row['iri'] for row in policy['supportedDatatypes']]
    if len(iris)!=31 or iris!=sorted(iris) or len(iris)!=len(set(iris)):
        raise RuntimeError('deployed datatype map must contain 31 sorted unique IRI rules')

    require('crates/ngkg-reference/src/datatype_policy.rs','write_embedded_policy','validate_reasoning_literals','available_parallelism','min_by_key','datatype is not present in the operator-supported datatype map','date_time_stamp','phase40_2_tests')
    require('crates/ngkg-reference/src/compiler.rs','reasoner/datatype-policy.json','reasoner/datatype-validation.json','validate_reasoning_literals','datatype_policy_sha256: datatype_policy_sha256.clone()','datatype_policy_sha256: Some')
    require('crates/ngkg-reference/src/model.rs','datatype_policy_path','datatype_policy_sha256','datatype_policy_sha256: Option<String>')
    require('crates/ngkg-reference/src/reasoner.rs','read_policy(&request.datatype_policy_path)','report.datatype_policy_sha256 != request.datatype_policy_sha256','report.format_version != 5')
    require('adapters/hermit-reasoner/src/main/java/io/ngkg/reasoner/Main.java','readAndValidateDatatypePolicy','validateDatatypeCoverage','datatypePolicySha256','merged ontology contains datatypes outside the operator policy')
    require('adapters/hermit-reasoner/src/test/java/io/ngkg/reasoner/MainTest.java','rejectsMergedOntologyDatatypeOutsidePolicy','datatypePolicySha256')

    report=load_json('contracts/reasoner-report.schema.json')
    Draft202012Validator.check_schema(report)
    if report['properties']['formatVersion'].get('const')!=5 or 'datatypePolicySha256' not in report['required'] or 'owlProfileQualificationSha256' not in report['required'] or 'owlConsistencyQualificationSha256' not in report['required']:
        raise RuntimeError('reasoner report is not current Phase 40.6 formatVersion 5 with inherited datatype binding')
    if report['properties']['datatypePolicySha256'].get('pattern')!='^[0-9a-f]{64}$':
        raise RuntimeError('reasoner report does not constrain datatypePolicySha256')

    phase=load_json('verification/phase-40.2.json')
    if phase.get('datatypePolicyRuntimeImplemented') is not True or phase.get('deterministicParallelValidationImplemented') is not True:
        raise RuntimeError('Phase 40.2 capability declaration is incomplete')
    if phase.get('owlDirectArbitraryBgpCompleteness') is not False or phase.get('standardsClaimsEnabled') is not False:
        raise RuntimeError('Phase 40.2 must not enable unqualified OWL Direct claims')

    registry=yaml.safe_load(require('acceptance/phase-gates.yaml'))['phases']
    by_phase={str(row['phase']):row for row in registry}
    if by_phase.get('40.2',{}).get('command')!='scripts/qualify_phase40_2.sh':
        raise RuntimeError('acceptance registry does not point Phase 40.2 to its qualification gate')

    evidence=load_json('verification/stabilization/phase-40.2.json')
    if evidence.get('parentLabel')!='phase-40.1' or evidence.get('currentLabel')!='phase-40.2' or evidence.get('deletedFiles')!=[]:
        raise RuntimeError('Phase 40.2 parent inheritance evidence is invalid')
    embedded=ROOT/evidence.get('embeddedParentManifest','')
    if not embedded.is_file() or sha256(embedded)!=evidence.get('parentFileManifestSha256'):
        raise RuntimeError('Phase 40.2 embedded parent manifest evidence is invalid')
    for changed in evidence.get('changedParentFiles',[]):
        if len(changed.get('parentSha256',''))!=64 or len(changed.get('currentSha256',''))!=64:
            raise RuntimeError(f"invalid Phase 40.2 transition digest for {changed.get('path')}")

    print('Phase 40.2 static verification passed; datatype policy/schema/runtime/hash/HPC validation wiring is coherent')
    return 0
if __name__=='__main__':
    try: raise SystemExit(main())
    except (KeyError,OSError,RuntimeError,TypeError,ValueError,json.JSONDecodeError,yaml.YAMLError) as exc:
        print(f'phase 40.2 static verification failed: {exc}',file=sys.stderr); raise SystemExit(1)
