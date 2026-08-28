#!/usr/bin/env python3
"""Static/schema/runtime-wiring gate for Phase 40.7 OWL Direct-BGP legality admission."""
from __future__ import annotations
import hashlib, json, pathlib, subprocess, sys, yaml
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
    cp=subprocess.run([sys.executable,str(ROOT/'scripts/validate_direct_bgp_legality.py'),str(ROOT/rel)],cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    if (cp.returncode==0)!=should_pass: raise RuntimeError(f'fixture expectation failed {rel}: {cp.stdout.strip()}')

def main():
    schema=load_json('contracts/direct-bgp-legality.schema.json'); Draft202012Validator.check_schema(schema)
    if schema.get('additionalProperties') is not False or schema['properties']['formatVersion'].get('const')!=1:
        raise RuntimeError('Direct-BGP legality schema is not strict formatVersion 1')
    if schema['properties']['classifier'].get('const')!='w3c-owl2-direct-bgp-cp1-cp4-v1':
        raise RuntimeError('classifier identifier is not checksum-stable/versioned')
    fixture('test-corpus/phase40_7/direct-bgp-legality-valid-legal.json',True)
    fixture('test-corpus/phase40_7/direct-bgp-legality-valid-illegal.json',True)
    for rel in ['direct-bgp-legality-invalid-aggregate.json','direct-bgp-legality-invalid-missing-failure.json','direct-bgp-legality-invalid-variable-order.json','direct-bgp-legality-invalid-extra-field.json']:
        fixture('test-corpus/phase40_7/'+rel,False)

    require('crates/ngkg-types/src/direct_legality.rs','DirectBgpLegalityReport','DirectVariableRole','AnnotationProperty','Datatype','grounded_owl2dl_check_required','MAX_VALIDATION_LANES','validate_direct_bgp_legality_report')
    require('crates/ngkg-owl-direct/src/lib.rs','classify_direct_bgps','available_parallelism','MAX_CLASSIFICATION_LANES','observe_explicit_declaration','index_structural_nodes','ConflictingVariableType','UndeclaredEntityVariable','is_datatype_facet','property_paths_outside_direct_bgps','grounded_owl2dl_check_required: true')
    require('crates/ngkg-sparql-compiler/src/lib.rs','pub const fn query(&self) -> &Query','SparqlParser::new()')
    require('crates/ngkg-reasoner-client/src/lib.rs','DirectBgpLegalityExpectedBinding','require_legal_direct_bgp','DirectBgpLegalityBindingMismatch','IllegalDirectBgp')
    require('services/online-serving/src/main.rs','/v1/datasets/{dataset_id}/sparql/direct/validate','validate_direct_bgps','require_reasoning_graph_authorization','resolve_request_dataset','owl_signature: Arc<OwlSignatureIndex>','validate_direct_bgp_legality_report')
    require('api/online-openapi.yaml','/v1/datasets/{datasetId}/sparql/direct/validate:','DirectBgpLegalityReport','groundedOwl2dlCheckRequired')
    require('docs/OWL_DIRECT_BGP_LEGALITY.md','BGP-local','Property-path','groundedOwl2dlCheckRequired=true','Phase 40.8')
    if ('TO' + 'DO') in require('crates/ngkg-owl-direct/src/lib.rs') or 'unsafe {' in require('crates/ngkg-owl-direct/src/lib.rs'):
        raise RuntimeError('new OWL Direct classifier contains placeholder/unsafe implementation')

    parity=subprocess.run([sys.executable,str(ROOT/'scripts/verify_api_openapi_parity.py'),'--report',str(ROOT/'qualification/phase40_7-api-openapi-parity.json')],cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    if parity.returncode: raise RuntimeError(f'API/OpenAPI parity failed: {parity.stdout.strip()}')
    if 'online-data-plane: 15 OpenAPI-covered REST operations' not in parity.stdout:
        raise RuntimeError('Phase 40.7 Swagger route is not included in online API parity')

    phase=load_json('verification/phase-40.7.json')
    for key in ['typedAlgebraBgpTraversalImplemented','bgpLocalVariableTypingImplemented','owlSignatureEntityDisambiguationImplemented','failClosedStructuralClassificationImplemented','deterministicParallelBgpClassificationImplemented','directBgpValidationRestApiImplemented','reasonerLegalityHandoffImplemented','propertyPathsSeparatedFromDirectBgps','groundedOwl2DlCandidateCheckDeferredToPhase40_8']:
        if phase.get(key) is not True: raise RuntimeError(f'Phase 40.7 missing capability {key}')
    for key in ['exactDirectReasonerFallbackImplemented','owlDirectArbitraryBgpCompleteness','standardsClaimsEnabled']:
        if phase.get(key) is not False: raise RuntimeError(f'Phase 40.7 overclaims {key}')

    req=load_json('verification/phase-40.7-requirements.json'); trace=load_json('verification/phase-40.7-traceability.json')
    ids={r['id'] for r in req['requirements']}
    if ids!={f'P40-7-{n:03d}' for n in range(1,9)}: raise RuntimeError('Phase 40.7 requirements incomplete')
    if {e['requirementId'] for e in trace['entries']}!=ids: raise RuntimeError('Phase 40.7 traceability mismatch')
    for entry in trace['entries']:
        for rel in entry.get('implementation',[])+entry.get('evidence',[]):
            if not (ROOT/rel).is_file(): raise RuntimeError(f'traceability missing {rel}')

    registry=yaml.safe_load(require('acceptance/phase-gates.yaml'))['phases']; by={str(x['phase']):x for x in registry}
    if by.get('40.7',{}).get('command')!='scripts/qualify_phase40_7.sh': raise RuntimeError('Phase 40.7 acceptance entry missing')
    cap=load_json('verification/phase-40-capability-status.json')
    legality=cap['capabilities'].get('directBgpLegalityClassification',{})
    if legality.get('status')!='implemented-static-qualified' or cap.get('standardsClaimsEnabled') is not False:
        raise RuntimeError('Phase 40.7 capability declaration invalid')
    if cap['capabilities']['owlDirectArbitraryBgpCompleteness'].get('status')!='not-implemented':
        raise RuntimeError('Phase 40.7 falsely claims arbitrary OWL Direct completeness')

    evidence=load_json('verification/stabilization/phase-40.7.json')
    if evidence.get('parentLabel')!='phase-40.6' or evidence.get('currentLabel')!='phase-40.7' or evidence.get('deletedFiles')!=[]:
        raise RuntimeError('Phase 40.7 inheritance evidence invalid')
    embedded=ROOT/evidence.get('embeddedParentManifest','')
    if not embedded.is_file() or sha256(embedded)!=evidence.get('parentFileManifestSha256'):
        raise RuntimeError('Phase 40.7 parent manifest evidence invalid')
    # Historical Phase 40.7 ancestry is immutable evidence about the Phase 40.7 release itself.
    # Descendant phases are allowed to change files that existed in 40.7; their own parent-manifest
    # records prove those changes. Recomputing the 40.6->40.7 delta against a 40.8+ tree would
    # incorrectly treat legitimate descendant edits as corruption.
    if evidence.get('parentPayloadFileCount',0) <= 0 or not isinstance(evidence.get('changedParentFiles'),list):
        raise RuntimeError('Phase 40.7 parent delta evidence is incomplete')
    print('Phase 40.7 static verification passed; OWL Direct BGP admission is typed, snapshot-bound, deterministic and fail-closed')
    return 0
if __name__=='__main__':
    try: raise SystemExit(main())
    except (OSError,RuntimeError,ValueError,KeyError,json.JSONDecodeError,yaml.YAMLError) as exc:
        print(f'phase 40.7 static verification failed: {exc}',file=sys.stderr); raise SystemExit(1)
