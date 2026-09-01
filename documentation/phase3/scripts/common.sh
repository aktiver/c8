#!/usr/bin/env bash
set -euo pipefail

phase3_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
candidate_root="$(cd "${phase3_root}/.." && pwd)"

die() {
  echo "phase3: $*" >&2
  exit 2
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command is unavailable: $1"
}

require_file() {
  [[ -f "$1" && -s "$1" ]] || die "required non-empty file is unavailable: $1"
}

require_digest_ref() {
  [[ "$2" =~ @sha256:[0-9a-f]{64}$ ]] || die "$1 must end in @sha256:<64 lowercase hex characters>"
}

sha256_file() {
  sha256sum "$1" | awk '{print $1}'
}

epoch_ms() {
  printf '%s000\n' "$(date +%s)"
}

canonical_json() {
  jq -cS . "$1"
}

write_sha_file() {
  local input="$1"
  local output="$2"
  sha256_file "$input" >"$output"
}
