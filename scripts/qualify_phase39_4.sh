#!/usr/bin/env bash
set -euo pipefail
scripts/qualify_phase39_3.sh
python3 scripts/verify_phase39_4_static.py
cargo test --locked -p ngkg-reference -p ngkg-online-serving --all-features
