#!/usr/bin/env python3
"""Independent semantic validator for Phase 40.5 OWL profile/import qualification evidence."""
from __future__ import annotations
import argparse, json, pathlib, sys
from jsonschema import Draft202012Validator, FormatChecker

ROOT = pathlib.Path(__file__).resolve().parents[1]


def fail(message: str) -> None:
    raise ValueError(message)


def validate(value: dict) -> None:
    schema = json.loads((ROOT / 'contracts/owl-profile-qualification.schema.json').read_text())
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(value)
    docs = value['ontologyDocuments']
    imports = value['importResolutions']
    if value['inputDocumentCount'] != value['ontologyDocumentCount'] + value['aboxDocumentCount']:
        fail('inputDocumentCount must equal ontologyDocumentCount + aboxDocumentCount')
    if value['ontologyDocumentCount'] != len(docs):
        fail('ontologyDocumentCount does not match ontologyDocuments')
    if value['loadedOntologyCount'] != value['inputDocumentCount']:
        fail('loadedOntologyCount must equal the complete checksum-bound input document count')
    if value['importDeclarationCount'] != len(imports) or value['resolvedImportCount'] != len(imports):
        fail('import counts must equal the complete resolved import edge list')
    doc_keys = [(d['ontologyIri'], d.get('versionIri') or '', d['sha256']) for d in docs]
    if doc_keys != sorted(doc_keys) or len(set(doc_keys)) != len(doc_keys):
        fail('ontologyDocuments must be strictly sorted and unique')
    import_keys = [(e['sourceOntologyIri'], e['importedIri'], e['resolvedDocumentSha256']) for e in imports]
    if import_keys != sorted(import_keys) or len(set(import_keys)) != len(import_keys):
        fail('importResolutions must be strictly sorted and unique')
    source_iris = {d['ontologyIri'] for d in docs}
    aliases = {}
    for d in docs:
        aliases[d['ontologyIri']] = d['sha256']
        if d.get('versionIri'):
            if d['versionIri'] in aliases and aliases[d['versionIri']] != d['sha256']:
                fail('ontology/version alias resolves to more than one document')
            aliases[d['versionIri']] = d['sha256']
    for edge in imports:
        if edge['sourceOntologyIri'] not in source_iris:
            fail('import source is not an ontology document')
        if aliases.get(edge['importedIri']) != edge['resolvedDocumentSha256']:
            fail('import target is not bound to its declared local document')
    if not value['completeLocalImportClosure']:
        fail('completeLocalImportClosure must be true')
    if value['profileValid']:
        if value['profileViolationCount'] != 0 or value['profileViolationSamples']:
            fail('valid profile cannot carry violations')
    elif value['profileViolationCount'] == 0:
        fail('invalid profile must carry a non-zero violation count')


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('path', type=pathlib.Path)
    args = parser.parse_args()
    value = json.loads(args.path.read_text())
    validate(value)
    print(f'valid Phase 40.5 OWL profile/import qualification: {args.path}')
    return 0

if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f'invalid Phase 40.5 OWL profile/import qualification: {exc}', file=sys.stderr)
        raise SystemExit(1)
