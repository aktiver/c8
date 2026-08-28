#!/usr/bin/env python3
"""Independent Phase 40.3 Direct-BGP result schema + semantic validator."""
from __future__ import annotations
import argparse, json, pathlib, re, sys
from jsonschema import Draft202012Validator

ROOT=pathlib.Path(__file__).resolve().parents[1]
SCHEMA=ROOT/'contracts/direct-bgp-result.schema.json'
SHA=re.compile(r'^[0-9a-f]{64}$')
RDF_LANG='http://www.w3.org/1999/02/22-rdf-syntax-ns#langString'

def absolute_iri(value:str)->bool:
    if not value or any(c.isspace() for c in value) or ':' not in value: return False
    scheme=value.split(':',1)[0]
    return bool(re.fullmatch(r'[A-Za-z][A-Za-z0-9+.-]*',scheme))

def semantic_validate(value:dict)->None:
    variables=value['variables']
    if variables!=sorted(set(variables)) or any((not v or len(v)>1024 or v[0] in '?$' or any(c.isspace() for c in v)) for v in variables):
        raise ValueError('variables must be canonical sorted unique names')
    if value['graphContext']['scope']=='named' and not absolute_iri(value['graphContext']['graphIri']):
        raise ValueError('named graph IRI must be absolute')
    if value['graphContext']['scope']=='default' and not SHA.fullmatch(value['graphContext']['activeDefaultGraphSha256']):
        raise ValueError('default graph hash must be lowercase SHA-256')
    allowed=set(variables); total=0
    for index,solution in enumerate(value['solutions']):
        if any(k not in allowed for k in solution['bindings']): raise ValueError(f'solution {index} binds undeclared variable')
        total+=solution['multiplicity']
        if total>2**64-1: raise ValueError('solution multiplicity overflows u64')
        for term in solution['bindings'].values():
            if term['termType']=='iri' and not absolute_iri(term['value']): raise ValueError(f'solution {index} has invalid IRI')
            if term['termType']=='blankNode' and (not term['value'] or any(c.isspace() for c in term['value'])): raise ValueError(f'solution {index} has invalid blank node')
            if term['termType']=='literal':
                if not absolute_iri(term['datatypeIri']): raise ValueError(f'solution {index} has invalid literal datatype')
                if 'language' in term and term['datatypeIri']!=RDF_LANG: raise ValueError(f'solution {index} language literal must use rdf:langString')
    if total!=value['solutionMultiplicityTotal']: raise ValueError('solutionMultiplicityTotal mismatch')
    if value['outcome']['status']=='complete' and value['candidateBindingCount']<len(value['solutions']):
        raise ValueError('candidateBindingCount smaller than distinct complete solutions')

def validate(path:pathlib.Path)->None:
    schema=json.loads(SCHEMA.read_text()); Draft202012Validator.check_schema(schema)
    value=json.loads(path.read_text())
    errors=sorted(Draft202012Validator(schema).iter_errors(value), key=lambda e:list(e.absolute_path))
    if errors: raise ValueError('; '.join(e.message for e in errors[:8]))
    semantic_validate(value)

def main()->int:
    ap=argparse.ArgumentParser(); ap.add_argument('path',type=pathlib.Path); args=ap.parse_args()
    validate(args.path); print(f'valid Phase 40.3 Direct-BGP result: {args.path}'); return 0
if __name__=='__main__':
    try: raise SystemExit(main())
    except (OSError,ValueError,json.JSONDecodeError) as exc:
        print(f'invalid Phase 40.3 Direct-BGP result: {exc}',file=sys.stderr); raise SystemExit(1)
