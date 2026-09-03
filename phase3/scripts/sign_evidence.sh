#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_command cosign
[[ "$#" -eq 2 ]] || die "usage: sign_evidence.sh FILE SIGNATURE_DIRECTORY"
input="$1"
directory="$2"
require_file "${input}"
mkdir -p "${directory}"
name="$(basename "${input}")"
if [[ "${NGKG_COSIGN_MODE:-keyless}" == key ]]; then
  : "${NGKG_COSIGN_KEY:?NGKG_COSIGN_KEY is required for key signing}"
  cosign sign-blob --yes --key "${NGKG_COSIGN_KEY}" \
    --output-signature "${directory}/${name}.sig" "${input}"
else
  cosign sign-blob --yes \
    --output-signature "${directory}/${name}.sig" \
    --output-certificate "${directory}/${name}.pem" "${input}"
fi
echo "signed evidence: ${name}"
