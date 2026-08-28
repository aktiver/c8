#!/usr/bin/env python3
"""Static and schema contract gate for Phase 40.1 OWL signature runtime wiring."""
from __future__ import annotations
import hashlib, json, pathlib, subprocess, sys, yaml
from jsonschema import Draft202012Validator, FormatChecker

ROOT=pathlib.Path(__file__).resolve().parents[1]

def load_json(rel:str):
    p=ROOT/rel
    if not p.is_file(): raise RuntimeError(f"missing {rel}")
    return json.loads(p.read_text(encoding="utf-8"))

def require(rel:str,*tokens:str)->str:
    p=ROOT/rel
    if not p.is_file(): raise RuntimeError(f"missing {rel}")
    text=p.read_text(encoding="utf-8")
    for token in tokens:
        if token not in text: raise RuntimeError(f"{rel} missing {token}")
    return text

def sha256(path:pathlib.Path)->str:
    h=hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda:handle.read(1024*1024),b""): h.update(block)
    return h.hexdigest()

def validate_schema_fixture(schema:dict,rel:str,should_pass:bool)->None:
    value=load_json(rel)
    errors=list(Draft202012Validator(schema,format_checker=FormatChecker()).iter_errors(value))
    if should_pass and errors:
        raise RuntimeError(f"{rel} unexpectedly failed schema: {errors[0].message}")
    if not should_pass and not errors:
        raise RuntimeError(f"{rel} unexpectedly passed schema")

def run_validator(rel:str,should_pass:bool)->None:
    result=subprocess.run(
        [sys.executable,str(ROOT/"scripts/validate_owl_signature.py"),str(ROOT/rel)],
        cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,
    )
    if (result.returncode==0) != should_pass:
        raise RuntimeError(f"OWL signature validator result mismatch for {rel}: {result.stdout.strip()}")

def main()->int:
    requirements=load_json("verification/phase-40.1-requirements.json")
    ids={row["id"] for row in requirements["requirements"]}
    if ids != {f"P40-1-{n:03d}" for n in range(1,8)}:
        raise RuntimeError("Phase 40.1 requirement set is incomplete")
    trace=load_json("verification/phase-40.1-traceability.json")
    if {row["requirementId"] for row in trace["entries"]} != ids:
        raise RuntimeError("Phase 40.1 traceability is incomplete")
    for row in trace["entries"]:
        for rel in row["implementation"]+row["evidence"]:
            if not (ROOT/rel).is_file(): raise RuntimeError(f"traceability references missing {rel}")

    schema=load_json("contracts/owl-signature.schema.json")
    Draft202012Validator.check_schema(schema)
    if schema.get("additionalProperties") is not False or schema.get("properties",{}).get("formatVersion",{}).get("const") != 1:
        raise RuntimeError("OWL signature schema is not a strict formatVersion 1 contract")
    required=set(schema["required"])
    expected={"formatVersion","datasetId","snapshotId","aggregateInputSha256","ontologyDocuments","imports","classes","objectProperties","dataProperties","annotationProperties","namedIndividuals","datatypes"}
    if required != expected: raise RuntimeError("OWL signature schema required fields drifted")
    validate_schema_fixture(schema,"test-corpus/phase40_1/owl-signature-valid.json",True)
    validate_schema_fixture(schema,"test-corpus/phase40_1/owl-signature-invalid-duplicate.json",False)
    validate_schema_fixture(schema,"test-corpus/phase40_1/owl-signature-invalid-extra-field.json",False)
    # Ordering is a runtime semantic invariant rather than a JSON-Schema keyword.
    validate_schema_fixture(schema,"test-corpus/phase40_1/owl-signature-invalid-unsorted.json",True)
    run_validator("test-corpus/phase40_1/owl-signature-valid.json",True)
    run_validator("test-corpus/phase40_1/owl-signature-invalid-duplicate.json",False)
    run_validator("test-corpus/phase40_1/owl-signature-invalid-unsorted.json",False)

    model=require("crates/ngkg-reference/src/model.rs","pub struct OwlSignature","pub struct OwlSignatureOntologyDocument","output_owl_signature_path","owl_signature_sha256")
    reasoner=require("crates/ngkg-reference/src/reasoner.rs","read_and_validate_owl_signature","ontologyDocuments do not exactly match","require_sorted_unique_iris","report.owl_signature_sha256 != owl_signature_sha256","OwlSignatureMissing")
    java=require("adapters/hermit-reasoner/src/main/java/io/ngkg/reasoner/Main.java","writeOwlSignature(request, loader, merged)","getClassesInSignature","getObjectPropertiesInSignature","getDataPropertiesInSignature","getAnnotationPropertiesInSignature","getIndividualsInSignature","getDatatypesInSignature","getImportsDeclarations","owlSignatureSha256")
    compiler=require("crates/ngkg-reference/src/compiler.rs","reasoner/owl-signature.json","output_owl_signature_path","owlSignatureSha256","owl_signature_sha256: Some")
    require("adapters/hermit-reasoner/src/test/java/io/ngkg/reasoner/MainTest.java","outputOwlSignaturePath","owlSignatureSha256","namedIndividuals")

    report_schema=load_json("contracts/reasoner-report.schema.json")
    if "owlSignatureSha256" not in report_schema.get("required",[]):
        raise RuntimeError("reasoner report schema does not require owlSignatureSha256")
    if report_schema["properties"]["owlSignatureSha256"].get("pattern") != "^[0-9a-f]{64}$":
        raise RuntimeError("reasoner report does not constrain owlSignatureSha256")

    phase=load_json("verification/phase-40.1.json")
    if phase.get("owlSignatureRuntimeImplemented") is not True or phase.get("standardsClaimsEnabled") is not False:
        raise RuntimeError("Phase 40.1 capability declaration is invalid")
    if phase.get("hpcExecutionSemanticsChanged") is not False:
        raise RuntimeError("Phase 40.1 must preserve inherited HPC execution semantics")

    registry=yaml.safe_load(require("acceptance/phase-gates.yaml"))["phases"]
    by_phase={str(row["phase"]):row for row in registry}
    if by_phase.get("40.1",{}).get("command") != "scripts/qualify_phase40_1.sh":
        raise RuntimeError("acceptance registry does not point Phase 40.1 to its qualification gate")

    evidence=load_json("verification/stabilization/phase-40.1.json")
    if evidence.get("parentLabel")!="phase-40" or evidence.get("currentLabel")!="phase-40.1" or evidence.get("deletedFiles")!=[]:
        raise RuntimeError("Phase 40.1 parent inheritance evidence is invalid")
    embedded=ROOT/evidence.get("embeddedParentManifest","")
    if not embedded.is_file() or sha256(embedded)!=evidence.get("parentFileManifestSha256"):
        raise RuntimeError("Phase 40.1 embedded parent manifest evidence is invalid")
    for changed in evidence.get("changedParentFiles",[]):
        if len(changed.get("parentSha256",""))!=64 or len(changed.get("currentSha256",""))!=64:
            raise RuntimeError(f"Phase 40.1 parent evidence has invalid digest for {changed.get('path')}")

    print("Phase 40.1 static verification passed; OWL signature runtime/schema/hash binding is coherent")
    return 0

if __name__=="__main__":
    try: raise SystemExit(main())
    except (KeyError,OSError,RuntimeError,TypeError,ValueError,json.JSONDecodeError,yaml.YAMLError) as exc:
        print(f"phase 40.1 static verification failed: {exc}",file=sys.stderr); raise SystemExit(1)
