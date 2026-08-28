#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

python3 scripts/verify_phase40_13_13_static.py
cargo fmt --all --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
mvn -f adapters/hermit-reasoner/pom.xml test package
helm lint charts/ngkg-platform --values charts/ngkg-platform/values.yaml
helm template ngkg charts/ngkg-platform --values charts/ngkg-platform/values.yaml >/dev/null
