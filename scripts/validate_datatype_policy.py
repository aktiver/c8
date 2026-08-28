#!/usr/bin/env python3
"""Validate the strict Phase 40.2 datatype policy plus deterministic ordering invariants."""
from __future__ import annotations
import argparse,json,pathlib,sys
from jsonschema import Draft202012Validator,FormatChecker
ROOT=pathlib.Path(__file__).resolve().parents[1]
def main()->int:
    ap=argparse.ArgumentParser(); ap.add_argument('policy',type=pathlib.Path); args=ap.parse_args()
    schema=json.loads((ROOT/'contracts/datatype-policy.schema.json').read_text(encoding='utf-8'))
    value=json.loads(args.policy.read_text(encoding='utf-8'))
    errors=sorted(Draft202012Validator(schema,format_checker=FormatChecker()).iter_errors(value),key=lambda e:list(e.path))
    if errors: raise RuntimeError(errors[0].message)
    iris=[row['iri'] for row in value['supportedDatatypes']]
    if iris != sorted(iris) or len(iris)!=len(set(iris)): raise RuntimeError('supportedDatatypes must be strictly sorted by IRI and duplicate-free')
    mapping={row['iri']:row['lexicalSpace'] for row in value['supportedDatatypes']}
    required={
      'http://www.w3.org/1999/02/22-rdf-syntax-ns#langString':'language_tagged_string',
      'http://www.w3.org/2001/XMLSchema#string':'string',
      'http://www.w3.org/2001/XMLSchema#boolean':'boolean',
      'http://www.w3.org/2001/XMLSchema#integer':'integer',
      'http://www.w3.org/2001/XMLSchema#decimal':'decimal',
      'http://www.w3.org/2001/XMLSchema#dateTime':'date_time',
      'http://www.w3.org/2001/XMLSchema#dateTimeStamp':'date_time_stamp'
    }
    for iri,space in required.items():
        if mapping.get(iri)!=space: raise RuntimeError(f'missing required datatype mapping {iri}')
    print(f"datatype policy valid: {value['policyId']} ({len(iris)} datatypes)")
    return 0
if __name__=='__main__':
    try: raise SystemExit(main())
    except (OSError,ValueError,KeyError,RuntimeError,json.JSONDecodeError) as exc:
        print(f'datatype policy validation failed: {exc}',file=sys.stderr); raise SystemExit(1)
