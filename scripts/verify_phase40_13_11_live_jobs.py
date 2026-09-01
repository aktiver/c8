#!/usr/bin/env python3
"""Validate live Indexed decode jobs supplied on stdin by kubectl."""

import json
import sys

payload = json.load(sys.stdin)
jobs = payload.get("items", [])
if not jobs:
    raise SystemExit("no source-decode Jobs were observed")
for job in jobs:
    spec = job.get("spec", {})
    if spec.get("completionMode") != "Indexed":
        raise SystemExit("source-decode Job is not Indexed")
    if spec.get("maxFailedIndexes") != 0:
        raise SystemExit("source-decode Job permits a partial barrier")
    if spec.get("completions", 0) < 1 or spec.get("parallelism", 0) < 1:
        raise SystemExit("source-decode Job has no schedulable work")
    complete = job.get("status", {}).get("succeeded", 0)
    if complete != spec["completions"]:
        raise SystemExit("source-decode Job did not complete every index")
print(json.dumps({"status": "passed", "indexedJobs": len(jobs)}))
