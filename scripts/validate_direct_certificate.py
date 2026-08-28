#!/usr/bin/env python3
"""Independent Phase 40.4/40.9 Direct certificate/schema/result-binding validator."""
from __future__ import annotations
import argparse, hashlib, json, pathlib, re, struct, sys, uuid
from jsonschema import Draft202012Validator

ROOT=pathlib.Path(__file__).resolve().parents[1]
SCHEMA=ROOT/'contracts/direct-certificate.schema.json'
RESULT_SCHEMA=ROOT/'contracts/direct-bgp-result.schema.json'
SHA=re.compile(r'^[0-9a-f]{64}$')
RESULT_DOMAIN=b'ngkg-direct-bgp-result-v1\0'
SOLUTION_DOMAIN=b'ngkg-direct-bgp-solution-v1\0'
FAILURE_TAGS={
    'illegal-bgp':1,'inconsistent-ontology':2,'unsupported-datatype':3,'resource-exhausted':4,
    'timeout':5,'reasoner-failure':6,'integrity-failure':7,'not-covered':8,
}

def absolute_iri(value:str)->bool:
    if not value or any(c.isspace() for c in value) or ':' not in value: return False
    return bool(re.fullmatch(r'[A-Za-z][A-Za-z0-9+.-]*',value.split(':',1)[0]))

def put_string(h, value:str)->None:
    payload=value.encode('utf-8'); h.update(struct.pack('>Q',len(payload))); h.update(payload)

def put_graph(h, graph:dict)->None:
    if graph['scope']=='default': h.update(b'\x01'); put_string(h,graph['activeDefaultGraphSha256'])
    else: h.update(b'\x02'); put_string(h,graph['graphIri'])

def put_term(h,term:dict)->None:
    kind=term['termType']
    if kind=='iri': h.update(b'\x01'); put_string(h,term['value'])
    elif kind=='blankNode': h.update(b'\x02'); put_string(h,term['value'])
    else:
        h.update(b'\x03'); put_string(h,term['lexicalForm']); put_string(h,term['datatypeIri'])
        if 'language' in term: h.update(b'\x01'); put_string(h,term['language'])
        else: h.update(b'\x00')

def solution_digest(solution:dict)->bytes:
    h=hashlib.sha256(); h.update(SOLUTION_DOMAIN); bindings=solution['bindings']; h.update(struct.pack('>Q',len(bindings)))
    for variable in sorted(bindings): put_string(h,variable); put_term(h,bindings[variable])
    h.update(struct.pack('>Q',solution['multiplicity'])); return h.digest()

def result_sha256(value:dict)->str:
    schema=json.loads(RESULT_SCHEMA.read_text()); errors=list(Draft202012Validator(schema).iter_errors(value))
    if errors: raise ValueError(f'Direct-BGP result schema invalid: {errors[0].message}')
    h=hashlib.sha256(); h.update(RESULT_DOMAIN); h.update(struct.pack('>I',value['formatVersion']))
    h.update(uuid.UUID(value['datasetId']).bytes); h.update(uuid.UUID(value['snapshotId']).bytes)
    for key in ['querySha256','bgpSha256','activeDatasetSha256','authorizedGraphSetSha256','owlSignatureSha256','datatypePolicySha256']: put_string(h,value[key])
    h.update(b'\x01'); put_graph(h,value['graphContext'])
    h.update(struct.pack('>Q',len(value['variables'])))
    for variable in value['variables']: put_string(h,variable)
    h.update(struct.pack('>Q',value['candidateBindingCount'])); h.update(struct.pack('>Q',value['solutionMultiplicityTotal']))
    digests=sorted(solution_digest(row) for row in value['solutions']); h.update(struct.pack('>Q',len(digests)))
    for digest in digests: h.update(digest)
    h.update(bytes([{'complete':1,'failed':2}[value['outcome']['status']]]))
    h.update(bytes([{'exact':1,'not-established':2}[value['outcome']['exactness']]]))
    h.update(bytes([{'complete':1,'incomplete':2,'not-established':3}[value['outcome']['completeness']]]))
    if 'error' in value:
        err=value['error']; h.update(b'\x01'); h.update(bytes([FAILURE_TAGS[err['code']]])); h.update(bytes([1 if err['retryable'] else 0])); put_string(h,err['detail'])
    else: h.update(b'\x00')
    return h.hexdigest()

def semantic_validate(cert:dict, result:dict|None)->None:
    graph=cert['graphContext']
    if graph['scope']=='named' and not absolute_iri(graph['graphIri']): raise ValueError('named graph IRI must be absolute')
    reasoner=cert['reasoner']
    for key in ['engine','engineVersion','adapterName','adapterVersion']:
        value=reasoner[key]
        if not value or len(value)>256 or any(ord(c)<32 or ord(c)==127 for c in value): raise ValueError(f'invalid reasoner field {key}')
    comp=cert['completeness']
    if comp['checkedCandidateBindingCount']!=comp['candidateBindingCount']: raise ValueError('candidate enumeration is incomplete')
    if comp['completedPartitionCount']!=comp['partitionCount']: raise ValueError('partition execution is incomplete')
    if comp['successfulReasonerRequestCount']!=comp['reasonerRequestCount']: raise ValueError('reasoner request execution is incomplete')
    refs=cert['supportReferences']; ids=[row['supportId'] for row in refs]
    if ids!=sorted(set(ids)): raise ValueError('supportReferences must be strictly sorted and unique by supportId')
    for row in refs:
        if 'sourceGraphIri' in row and not absolute_iri(row['sourceGraphIri']): raise ValueError('support sourceGraphIri must be absolute')
    if cert['proofCoverage']=='complete' and not refs: raise ValueError('complete proof coverage requires support references')
    if cert.get('formatVersion')==2:
        proof_sha=cert.get('proofManifestSha256')
        if not proof_sha or not SHA.fullmatch(proof_sha): raise ValueError('formatVersion 2 requires proofManifestSha256')
        if cert.get('proofCoverage')!='complete': raise ValueError('formatVersion 2 requires complete proof coverage')
        if any(row.get('kind')!='reasoner-check' or row.get('artifactSha256')!=proof_sha for row in refs): raise ValueError('formatVersion 2 supports must bind proof manifest')
    elif 'proofManifestSha256' in cert:
        raise ValueError('legacy certificate may not carry proofManifestSha256')
    if result is None: return
    for cert_key,result_key in [
        ('datasetId','datasetId'),('snapshotId','snapshotId'),('querySha256','querySha256'),('bgpSha256','bgpSha256'),
        ('activeDatasetSha256','activeDatasetSha256'),('authorizedGraphSetSha256','authorizedGraphSetSha256'),
        ('owlSignatureSha256','owlSignatureSha256'),('datatypePolicySha256','datatypePolicySha256'),('graphContext','graphContext')]:
        if cert[cert_key]!=result[result_key]: raise ValueError(f'certificate/result mismatch: {cert_key}')
    outcome=result['outcome']
    if outcome != {'status':'complete','exactness':'exact','completeness':'complete'} or 'error' in result: raise ValueError('certificate may only bind exact complete result')
    if cert['completeness']['candidateBindingCount']!=result['candidateBindingCount']: raise ValueError('candidate count mismatch')
    observed=result_sha256(result)
    if cert['directBgpResultSha256']!=observed: raise ValueError(f'Direct-BGP result digest mismatch: expected {observed}')

def validate(cert_path:pathlib.Path,result_path:pathlib.Path|None=None)->None:
    schema=json.loads(SCHEMA.read_text()); Draft202012Validator.check_schema(schema)
    cert=json.loads(cert_path.read_text()); errors=sorted(Draft202012Validator(schema).iter_errors(cert),key=lambda e:list(e.absolute_path))
    if errors: raise ValueError('; '.join(e.message for e in errors[:8]))
    result=json.loads(result_path.read_text()) if result_path else None
    semantic_validate(cert,result)

def main()->int:
    ap=argparse.ArgumentParser(); ap.add_argument('certificate',type=pathlib.Path); ap.add_argument('--result',type=pathlib.Path); args=ap.parse_args()
    validate(args.certificate,args.result); print(f'valid Phase 40.4 Direct certificate: {args.certificate}'); return 0
if __name__=='__main__':
    try: raise SystemExit(main())
    except (OSError,ValueError,KeyError,json.JSONDecodeError) as exc:
        print(f'invalid Phase 40.4 Direct certificate: {exc}',file=sys.stderr); raise SystemExit(1)
