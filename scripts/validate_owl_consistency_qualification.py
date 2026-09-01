#!/usr/bin/env python3
"""Independent Phase 40.6 OWL consistency qualification validator."""
from __future__ import annotations
import json, pathlib, sys
from jsonschema import Draft202012Validator, FormatChecker
ROOT=pathlib.Path(__file__).resolve().parents[1]

def main()->int:
    if len(sys.argv)!=2:
        print('usage: validate_owl_consistency_qualification.py PATH',file=sys.stderr); return 2
    schema=json.loads((ROOT/'contracts/owl-consistency-qualification.schema.json').read_text())
    value=json.loads(pathlib.Path(sys.argv[1]).read_text())
    Draft202012Validator.check_schema(schema)
    errors=sorted(Draft202012Validator(schema,format_checker=FormatChecker()).iter_errors(value), key=lambda e:list(e.path))
    if errors:
        raise ValueError('; '.join(f'{list(e.path)}: {e.message}' for e in errors[:8]))
    if value['loadedOntologyCount'] != value['inputDocumentCount']:
        raise ValueError('loadedOntologyCount must equal the complete checksum-bound inputDocumentCount')
    if value['consistencyChecked']:
        if value['publicationPermitted'] != value['consistent']:
            raise ValueError('publicationPermitted must equal consistent after a completed consistency check')
    elif value['consistent'] or value['publicationPermitted']:
        raise ValueError('unchecked consistency cannot assert consistency or permit publication')
    print(f"OWL consistency qualification valid: checked={value['consistencyChecked']} consistent={value['consistent']} publicationPermitted={value['publicationPermitted']}")
    return 0
if __name__=='__main__':
    try: raise SystemExit(main())
    except (OSError,ValueError,KeyError,json.JSONDecodeError) as exc:
        print(f'OWL consistency qualification invalid: {exc}',file=sys.stderr); raise SystemExit(1)
