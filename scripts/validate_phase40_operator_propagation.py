#!/usr/bin/env python3
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
import yaml

ENV_ORDER = [
    ("maxCandidateBindings", "NGKG_PHASE40_DIRECT_MAX_CANDIDATE_BINDINGS"),
    ("maxPartitionCandidates", "NGKG_PHASE40_DIRECT_MAX_PARTITION_CANDIDATES"),
    ("maxExactPartitions", "NGKG_PHASE40_DIRECT_MAX_EXACT_PARTITIONS"),
    ("maxGroundedAxiomsPerCandidate", "NGKG_PHASE40_DIRECT_MAX_GROUNDED_AXIOMS_PER_CANDIDATE"),
    ("maxGroundedRdfBytesPerCandidate", "NGKG_PHASE40_DIRECT_MAX_GROUNDED_RDF_BYTES_PER_CANDIDATE"),
    ("reasonerConcurrency", "NGKG_PHASE40_DIRECT_REASONER_CONCURRENCY"),
    ("reasonerHeapMiBPerLane", "NGKG_PHASE40_DIRECT_REASONER_HEAP_MIB_PER_LANE"),
    ("reasonerTimeoutSeconds", "NGKG_PHASE40_DIRECT_REASONER_TIMEOUT_SECONDS"),
    ("maxCertificateBytes", "NGKG_PHASE40_DIRECT_MAX_CERTIFICATE_BYTES"),
    ("maxProofSupportIds", "NGKG_PHASE40_DIRECT_MAX_PROOF_SUPPORT_IDS"),
]
DOMAIN=b"ngkg-phase40-reference-worker-ceilings-v1\0"

def bundle_sha(values: dict[str, int]) -> str:
    h=hashlib.sha256(); h.update(DOMAIN)
    for helm, env in ENV_ORDER:
        h.update(env.encode()); h.update(b"="); h.update(str(values[helm]).encode()); h.update(b"\n")
    return h.hexdigest()

def main() -> int:
    ap=argparse.ArgumentParser(); ap.add_argument('--root', type=Path, required=True); ap.add_argument('--report', type=Path)
    args=ap.parse_args(); root=args.root.resolve()
    values=yaml.safe_load((root/'charts/ngkg-platform/values.yaml').read_text())['phase40']['direct']
    digest=bundle_sha(values)
    expected='5a6c84f87b725f3598a4a5fb3ba496aba0557b0f97495614a5b3f897d470de50'
    if digest != expected: raise SystemExit(f'Phase 40 default ceiling digest drifted: {digest} != {expected}')
    operator=(root/'services/operator/src/main.rs').read_text()
    distributed=(root/'services/distributed-operator/src/main.rs').read_text()
    core=(root/'crates/ngkg-operator-core/src/lib.rs').read_text()
    worker=(root/'services/reference-worker/src/phase40_limits.rs').read_text()
    for _, env in ENV_ORDER:
        if env not in core or env not in worker:
            raise SystemExit(f'missing shared Phase 40 environment mapping: {env}')
    for rel in ['charts/ngkg-platform/templates/operator.yaml','charts/ngkg-platform/templates/distributed-operator.yaml']:
        text=(root/rel).read_text()
        if "configMapRef:" not in text or "phase40-reference-ceilings" not in text:
            raise SystemExit(f'{rel} does not import the immutable Phase 40 ConfigMap')
    required=[
        'Phase40DirectCeilings::from_env', 'phase40_direct.bundle_sha256()',
        'ngkg.io/phase40-direct-ceilings-sha256', 'NGKG_PHASE40_DIRECT_CEILINGS_SHA256',
    ]
    for token in required:
        if token not in operator: raise SystemExit(f'standard operator missing {token}')
        if token not in distributed: raise SystemExit(f'distributed operator missing {token}')
    if '(stage == Stage::Reasoner).then(|| config.phase40_direct.bundle_sha256())' not in distributed:
        raise SystemExit('distributed operator does not scope exact ceilings to Reasoner stage')
    if 'expected_phase40 != observed_phase40' not in distributed:
        raise SystemExit('distributed operator does not reject existing-Job policy drift')
    if 'observed_phase40 != Some(&expected_phase40)' not in operator:
        raise SystemExit('standard operator does not reject existing-Job policy drift')
    if 'Phase 40 direct ceiling bundle SHA mismatch' not in worker:
        raise SystemExit('reference worker does not independently verify operator bundle SHA')
    report={
        'formatVersion':1, 'phase':'40.13', 'ceilingCount':len(ENV_ORDER),
        'bundleSha256':digest, 'operatorPropagation':True, 'distributedReasonerPropagation':True,
        'distributedNonReasonerPropagation':False, 'workerIndependentHashVerification':True,
    }
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True); args.report.write_text(json.dumps(report,indent=2)+"\n")
    print(json.dumps(report, sort_keys=True)); return 0
if __name__=='__main__': raise SystemExit(main())
