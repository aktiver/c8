#!/usr/bin/env python3
"""Independent Phase 40.9 proof/support manifest and certificate coverage validator."""
from __future__ import annotations
import argparse, hashlib, json, pathlib, struct, sys, uuid
from collections import Counter
from jsonschema import Draft202012Validator
from validate_direct_certificate import result_sha256
ROOT=pathlib.Path(__file__).resolve().parents[1]
MANIFEST_SCHEMA=ROOT/'contracts/direct-proof-manifest.schema.json'
CERT_SCHEMA=ROOT/'contracts/direct-certificate.schema.json'
RESULT_SCHEMA=ROOT/'contracts/direct-bgp-result.schema.json'
BINDING_DOMAIN=b'ngkg-direct-proof-binding-v1\0'
SUPPORT_DOMAIN=b'ngkg-direct-reasoner-check-support-v1\0'
COMPLETION_DOMAIN=b'ngkg-direct-completion-support-v1\0'

def put_string(h, value:str):
    b=value.encode(); h.update(struct.pack('>Q',len(b))); h.update(b)
def put_graph(h,g):
    if g['scope']=='default': h.update(b'\x01'); put_string(h,g['activeDefaultGraphSha256'])
    else: h.update(b'\x02'); put_string(h,g['graphIri'])
def put_term(h,t):
    if t['termType']=='iri': h.update(b'\x01'); put_string(h,t['value'])
    elif t['termType']=='blankNode': h.update(b'\x02'); put_string(h,t['value'])
    else:
        h.update(b'\x03'); put_string(h,t['lexicalForm']); put_string(h,t['datatypeIri'])
        if 'language' in t: h.update(b'\x01'); put_string(h,t['language'])
        else: h.update(b'\x00')
def binding_sha(bindings):
    h=hashlib.sha256(); h.update(BINDING_DOMAIN); h.update(struct.pack('>Q',len(bindings)))
    for name in sorted(bindings): put_string(h,name); put_term(h,bindings[name])
    return h.hexdigest()
def support_id(m,p):
    h=hashlib.sha256(); h.update(SUPPORT_DOMAIN); h.update(uuid.UUID(m['datasetId']).bytes); h.update(uuid.UUID(m['snapshotId']).bytes)
    for k in ['querySha256','bgpSha256','activeDatasetSha256','authorizedGraphSetSha256','owlSignatureSha256','datatypePolicySha256']: put_string(h,m[k])
    put_graph(h,m['graphContext']); h.update(struct.pack('>Q',p['candidateOrdinal'])); h.update(struct.pack('>I',p['partitionIndex']))
    for k in ['requestSha256','bindingSha256','groundedRdfSha256','logicalAxiomsSha256']: put_string(h,p[k])
    h.update(struct.pack('>Q',p['logicalAxiomCount'])); return h.hexdigest()
def completion_id(m):
    h=hashlib.sha256(); h.update(COMPLETION_DOMAIN); h.update(uuid.UUID(m['datasetId']).bytes); h.update(uuid.UUID(m['snapshotId']).bytes)
    for k in ['querySha256','bgpSha256','directBgpResultSha256','candidateSpaceSha256','executionRootSha256','reasonerEngine','reasonerVersion','adapterVersion']: put_string(h,m[k])
    return h.hexdigest()
def validate(manifest_path,result_path,certificate_path):
    m=json.loads(manifest_path.read_text()); r=json.loads(result_path.read_text()); c=json.loads(certificate_path.read_text())
    Draft202012Validator(json.loads(MANIFEST_SCHEMA.read_text())).validate(m)
    Draft202012Validator(json.loads(RESULT_SCHEMA.read_text())).validate(r)
    Draft202012Validator(json.loads(CERT_SCHEMA.read_text())).validate(c)
    if m['completionSupportId']!=completion_id(m): raise ValueError('completion supportId mismatch')
    ords=[p['candidateOrdinal'] for p in m['answerProofs']]
    if ords!=sorted(set(ords)): raise ValueError('answer proofs must be candidate-ordinal sorted and unique')
    for p in m['answerProofs']:
        if p['supportId']!=support_id(m,p): raise ValueError('answer supportId mismatch')
    observed_result=result_sha256(r)
    if m['directBgpResultSha256']!=observed_result or c['directBgpResultSha256']!=observed_result: raise ValueError('result digest mismatch')
    manifest_sha=hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    if c.get('formatVersion')!=2 or c.get('proofManifestSha256')!=manifest_sha or c.get('proofCoverage')!='complete': raise ValueError('certificate does not bind complete proof manifest')
    for k in ['datasetId','snapshotId','querySha256','bgpSha256','activeDatasetSha256','authorizedGraphSetSha256','owlSignatureSha256','datatypePolicySha256','graphContext']:
        if m[k]!=r[k] or c[k]!=r[k]: raise ValueError(f'identity mismatch: {k}')
    if m['candidateSpaceSha256']!=c['completeness']['candidateSpaceSha256'] or m['executionRootSha256']!=c['completeness']['executionRootSha256']: raise ValueError('completeness root mismatch')
    expected=Counter()
    for solution in r['solutions']:
        digest=binding_sha(solution['bindings'])
        if digest in expected:
            raise ValueError('result contains duplicate compressed binding rows')
        expected[digest]=solution['multiplicity']
    observed=Counter(p['bindingSha256'] for p in m['answerProofs'])
    if expected!=observed or len(m['answerProofs'])!=r['solutionMultiplicityTotal']: raise ValueError('proof records do not cover exact solution multiset')
    expected_ids={m['completionSupportId']}|{p['supportId'] for p in m['answerProofs']}
    refs=c['supportReferences']; actual_ids={x['supportId'] for x in refs}
    if expected_ids!=actual_ids or len(refs)!=len(actual_ids): raise ValueError('certificate support ID set mismatch')
    for ref in refs:
        if ref['kind']!='reasoner-check' or ref.get('artifactSha256')!=manifest_sha: raise ValueError('support reference is not bound to proof manifest')
    if m['reasonerEngine']!=c['reasoner']['engine'] or m['reasonerVersion']!=c['reasoner']['engineVersion'] or m['adapterVersion']!=c['reasoner']['adapterVersion']: raise ValueError('reasoner identity mismatch')
    return manifest_sha

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('manifest',type=pathlib.Path); ap.add_argument('--result',required=True,type=pathlib.Path); ap.add_argument('--certificate',required=True,type=pathlib.Path); a=ap.parse_args()
    sha=validate(a.manifest,a.result,a.certificate); print(f'valid Phase 40.9 Direct proof bundle: {a.manifest} sha256={sha}'); return 0
if __name__=='__main__':
    try: raise SystemExit(main())
    except Exception as exc: print(f'invalid Phase 40.9 Direct proof bundle: {exc}',file=sys.stderr); raise SystemExit(1)
