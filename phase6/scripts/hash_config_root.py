#!/usr/bin/env python3
import argparse
from pathlib import Path
from phase6_common import config_root_sha256

parser = argparse.ArgumentParser()
parser.add_argument("config_root", type=Path)
args = parser.parse_args()
print(config_root_sha256(args.config_root.resolve()))
