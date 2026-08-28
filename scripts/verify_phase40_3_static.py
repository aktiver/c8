#!/usr/bin/env python3
from __future__ import annotations
import hashlib,json,pathlib,subprocess,sys,yaml
from jsonschema import Draft202012Validator
ROOT=pathlib.Path(__file__).resolve().parents[1]
def require(path:str, token:str|None=None):
    text=(ROOT/path).read_text()
    if token is not None and token not in text: raise RuntimeError(f'{path} missing {token!r}')
    return text
def load_json(path:str): return json.loads((ROOT/path).read_text())
def sha256(path:pathlib.Path): return hashlib.sha256(path.read_bytes()).hexdigest()
def run_fixture(path:str, valid:bool):
    completed=subprocess.run([sys.executable,str(ROOT/'scripts/validate_direct_bgp_result.py'),str(ROOT/path)],cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
    if (completed.returncode==0)!=valid: raise RuntimeError(f'{path} expected valid={valid}: {completed.stdout}')
def main():
    schema=load_json('contracts/direct-bgp-result.schema.json'); Draft202012Validator.check_schema(schema)
    if schema['properties']['formatVersion'].get('const')!=1 or schema['properties']['entailmentRegime'].get('const')!='owl2-direct': raise RuntimeError('Direct-BGP schema version/regime mismatch')
    run_fixture('test-corpus/phase40_3/direct-bgp-result-valid-complete.json',True)
    run_fixture('test-corpus/phase40_3/direct-bgp-result-valid-failed.json',True)
    run_fixture('test-corpus/phase40_3/direct-bgp-result-invalid-variable-order.json',False)
    run_fixture('test-corpus/phase40_3/direct-bgp-result-invalid-multiplicity-total.json',False)
    run_fixture('test-corpus/phase40_3/direct-bgp-result-invalid-failed-partial.json',False)
    run_fixture('test-corpus/phase40_3/direct-bgp-result-invalid-extra-field.json',False)
    require('crates/ngkg-types/src/lib.rs','pub mod direct_bgp;')
    require('crates/ngkg-reasoner-client/src/lib.rs','validate_direct_bgp_result_binding')
    require('crates/ngkg-reasoner-client/src/lib.rs','DirectBgpBindingMismatch')
    for token in ['DirectBgpResult','DirectBgpRdfTerm','solution_multiplicity_total','candidate_binding_count','validate_direct_bgp_result','available_parallelism','MAX_VALIDATION_LANES','min(solutions.len())','lowest solution']:
        require('crates/ngkg-types/src/direct_bgp.rs',token)
    for token in ['complete requires exact + complete and forbids error','failed requires no successful solutions','rdf:langString']:
        require('crates/ngkg-types/src/direct_bgp.rs',token)
    phase=load_json('verification/phase-40.3.json')
    if not all(phase.get(k) is True for k in ['directBgpResultRuntimeContractImplemented','losslessRdfTermIdentityImplemented','bagMultiplicityContractImplemented','graphSensitiveResultContextImplemented','deterministicParallelResultValidationImplemented']): raise RuntimeError('Phase 40.3 capability declaration incomplete')
    if phase.get('directCertificateImplemented') is not False or phase.get('owlDirectArbitraryBgpCompleteness') is not False or phase.get('standardsClaimsEnabled') is not False: raise RuntimeError('Phase 40.3 overclaims Direct semantics')
    registry=yaml.safe_load(require('acceptance/phase-gates.yaml'))['phases']; by={str(x['phase']):x for x in registry}
    if by.get('40.3',{}).get('command')!='scripts/qualify_phase40_3.sh': raise RuntimeError('Phase 40.3 acceptance entry missing')
    req=load_json('verification/phase-40.3-requirements.json'); trace=load_json('verification/phase-40.3-traceability.json')
    ids={r['id'] for r in req['requirements']}; traced={r['requirementId'] for r in trace['entries']}
    if ids!=traced: raise RuntimeError('Phase 40.3 requirements/traceability mismatch')
    evidence=load_json('verification/stabilization/phase-40.3.json')
    if evidence.get('parentLabel')!='phase-40.2' or evidence.get('currentLabel')!='phase-40.3' or evidence.get('deletedFiles')!=[]: raise RuntimeError('Phase 40.3 inheritance evidence invalid')
    embedded=ROOT/evidence.get('embeddedParentManifest','')
    if not embedded.is_file() or sha256(embedded)!=evidence.get('parentFileManifestSha256'): raise RuntimeError('Phase 40.3 parent manifest evidence invalid')
    # Phase 40.3 evidence describes the historical 40.2 -> 40.3 transition. Descendant
    # phases may legitimately modify those files; their own checksum-bound ancestry proves
    # the next transition. Validate the embedded parent manifest and recorded historical
    # digests here rather than requiring a descendant tree to byte-match Phase 40.3.
    seen=set()
    for changed in evidence.get('changedParentFiles', []):
        path=changed.get('path','')
        if not path or path in seen or len(changed.get('parentSha256',''))!=64 or len(changed.get('currentSha256',''))!=64:
            raise RuntimeError('Phase 40.3 parent delta evidence contains an invalid historical digest')
        seen.add(path)
    print('Phase 40.3 static verification passed; Direct-BGP result/schema/RDF-term/bag/graph/HPC contract is coherent')
    return 0
if __name__=='__main__':
    try: raise SystemExit(main())
    except (OSError,RuntimeError,ValueError,KeyError,json.JSONDecodeError,yaml.YAMLError) as exc:
        print(f'phase 40.3 static verification failed: {exc}',file=sys.stderr); raise SystemExit(1)
