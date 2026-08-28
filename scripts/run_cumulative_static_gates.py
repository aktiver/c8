#!/usr/bin/env python3
"""Run every repository static phase gate from Phase 15 through the requested endpoint."""
from __future__ import annotations
import argparse,pathlib,re,subprocess,sys
ROOT=pathlib.Path(__file__).resolve().parents[1]
PATTERN=re.compile(r'verify_phase(?P<major>\d+)(?:_(?P<minor>\d+))?_static\.py$')
def version(path:pathlib.Path):
 m=PATTERN.match(path.name)
 if not m: return None
 return int(m.group('major')),int(m.group('minor') or 0)
def parse(value:str):
 if '.' in value:
  a,b=value.split('.',1); return int(a),int(b)
 return int(value),0
def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--from-phase',default='15'); ap.add_argument('--through-phase',default='40'); ap.add_argument('--report',type=pathlib.Path)
 args=ap.parse_args(); low=parse(args.from_phase); high=parse(args.through_phase)
 gates=[]
 for path in (ROOT/'scripts').glob('verify_phase*_static.py'):
  v=version(path)
  if v is not None and low<=v<=high: gates.append((v,path))
 gates.sort()
 if not gates: raise RuntimeError('no static gates selected')
 records=[]
 for v,path in gates:
  completed=subprocess.run([sys.executable,str(path)],cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
  records.append({'phase':f'{v[0]}.{v[1]}' if v[1] else str(v[0]),'path':str(path.relative_to(ROOT)),'exitCode':completed.returncode,'output':completed.stdout.strip()})
  print(f"[{records[-1]['phase']}] {path.name}: {'PASS' if completed.returncode==0 else 'FAIL'}")
  if completed.returncode!=0:
   if args.report:
    import json; args.report.parent.mkdir(parents=True,exist_ok=True); args.report.write_text(json.dumps({'formatVersion':1,'records':records},indent=2)+'\n')
   return completed.returncode
 if args.report:
  import json; args.report.parent.mkdir(parents=True,exist_ok=True); args.report.write_text(json.dumps({'formatVersion':1,'records':records},indent=2)+'\n')
 return 0
if __name__=='__main__':
 try: raise SystemExit(main())
 except (OSError,RuntimeError,ValueError) as e:
  print(f'cumulative static gate failed: {e}',file=sys.stderr); raise SystemExit(1)
