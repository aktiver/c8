#!/usr/bin/env python3
"""Execute acceptance-registry commands for a bounded numeric phase range."""
from __future__ import annotations
import argparse,pathlib,subprocess,sys,yaml
ROOT=pathlib.Path(__file__).resolve().parents[1]
def parse(v):
 raw=str(v)
 if raw.endswith(('A','B','C')): return None
 if '.' in raw:
  a,b=raw.split('.',1); return int(a),int(b)
 return int(raw),0
def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--from-phase',required=True); ap.add_argument('--through-phase',required=True); ap.add_argument('--dry-run',action='store_true')
 args=ap.parse_args(); low=parse(args.from_phase); high=parse(args.through_phase)
 registry=yaml.safe_load((ROOT/'acceptance/phase-gates.yaml').read_text())['phases']
 selected=[]
 for item in registry:
  v=parse(item['phase'])
  if v is not None and low<=v<=high: selected.append((v,item))
 selected.sort(key=lambda x:x[0])
 if not selected: raise RuntimeError('no acceptance gates selected')
 for _,item in selected:
  command=item['command']; print(f"phase {item['phase']}: {command}",flush=True)
  if args.dry_run: continue
  completed=subprocess.run(['bash','-lc',command],cwd=ROOT)
  if completed.returncode!=0: return completed.returncode
 return 0
if __name__=='__main__':
 try: raise SystemExit(main())
 except (KeyError,OSError,RuntimeError,ValueError) as e:
  print(f'acceptance execution failed: {e}',file=sys.stderr); raise SystemExit(1)
