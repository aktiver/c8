#!/usr/bin/env python3
"""Generate checksum-bound parent evidence for incremental NGKG stabilization archives."""
from __future__ import annotations
import argparse, hashlib, json, pathlib, re, zipfile
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def sha256_file(path: pathlib.Path) -> str:
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024), b''):
            h.update(chunk)
    return h.hexdigest()

def parse_manifest(data: bytes) -> dict[str,str]:
    out={}
    for n,raw in enumerate(data.decode('utf-8').splitlines(),1):
        if not raw: continue
        try: digest,rel=raw.split('  ',1)
        except ValueError as e: raise RuntimeError(f'invalid parent manifest line {n}') from e
        parts=pathlib.PurePosixPath(rel).parts
        if not SHA256_RE.fullmatch(digest) or not rel or rel.startswith('/') or '..' in parts:
            raise RuntimeError(f'invalid parent manifest entry at line {n}')
        if rel in out: raise RuntimeError(f'duplicate parent manifest path: {rel}')
        out[rel]=digest
    if not out: raise RuntimeError('parent archive manifest is empty')
    return out

def locate_manifest(z: zipfile.ZipFile) -> tuple[str,bytes]:
    matches=[n for n in z.namelist() if n=='FILE_MANIFEST_SHA256.txt' or n.endswith('/FILE_MANIFEST_SHA256.txt')]
    if len(matches)!=1: raise RuntimeError('parent archive must contain exactly one FILE_MANIFEST_SHA256.txt')
    return matches[0], z.read(matches[0])

def main()->int:
    ap=argparse.ArgumentParser()
    ap.add_argument('--parent-archive',required=True,type=pathlib.Path)
    ap.add_argument('--parent-label',required=True)
    ap.add_argument('--current-label',required=True)
    ap.add_argument('--root',type=pathlib.Path,default=pathlib.Path(__file__).resolve().parents[1])
    args=ap.parse_args(); root=args.root.resolve(); archive=args.parent_archive.resolve()
    if not archive.is_file(): raise RuntimeError(f'parent archive does not exist: {archive}')
    with zipfile.ZipFile(archive) as z:
        bad=z.testzip()
        if bad is not None: raise RuntimeError(f'parent archive ZIP integrity failed at {bad}')
        member,manifest_bytes=locate_manifest(z)
    parent=parse_manifest(manifest_bytes)
    deleted=[]; changed=[]
    for rel,parent_sha in sorted(parent.items()):
        cur=root/rel
        if not cur.is_file(): deleted.append(rel); continue
        cur_sha=sha256_file(cur)
        if cur_sha!=parent_sha: changed.append({'path':rel,'parentSha256':parent_sha,'currentSha256':cur_sha})
    if deleted: raise RuntimeError(f'current tree deleted parent files: {deleted[:20]}')
    safe_parent=re.sub(r'[^A-Za-z0-9_.-]+','-',args.parent_label)
    safe_current=re.sub(r'[^A-Za-z0-9_.-]+','-',args.current_label)
    outdir=root/'verification'/'stabilization'; outdir.mkdir(parents=True,exist_ok=True)
    embedded=outdir/f'{safe_parent}-files.sha256'; embedded.write_bytes(manifest_bytes)
    evidence={
      'formatVersion':1,'currentLabel':args.current_label,'parentLabel':args.parent_label,
      'parentArchiveSha256':sha256_file(archive),'parentFileManifestSha256':sha256_bytes(manifest_bytes),
      'parentPayloadFileCount':len(parent),'embeddedParentManifest':embedded.relative_to(root).as_posix(),
      'deletedFiles':[],'changedParentFiles':changed,
    }
    output=outdir/f'{safe_current}.json'; output.write_text(json.dumps(evidence,indent=2,sort_keys=True)+'\n')
    print(json.dumps({'output':str(output),'parentFiles':len(parent),'changedParentFiles':len(changed)},indent=2))
    return 0
if __name__=='__main__': raise SystemExit(main())
