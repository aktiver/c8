#!/usr/bin/env bash
set -euo pipefail
scripts/qualify_phase39_2.sh
python3 scripts/verify_phase39_3_static.py
cargo test --locked -p ngkg-reference -p ngkg-dataset -p ngkg-sparql-compiler --all-features
