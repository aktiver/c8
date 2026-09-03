#!/usr/bin/env python3
"""Bounded mTLS qualification driver for site-owned benchmark/chaos APIs."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import ssl
import stat
import sys
import urllib.error
import urllib.request

MAX_REQUEST = 4 * 1024 * 1024
MAX_RESPONSE = 64 * 1024 * 1024


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, new_url):  # type: ignore[no-untyped-def]
        return None


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def private_file(name: str) -> Path:
    path = Path(os.environ[name]).resolve()
    require(path.is_file() and stat.S_IMODE(path.stat().st_mode) & 0o077 == 0, f"{name} must be a private regular file")
    return path


def main() -> int:
    raw = sys.stdin.buffer.read(MAX_REQUEST + 1)
    require(len(raw) <= MAX_REQUEST, "driver request exceeds byte limit")
    request_document = json.loads(raw)
    require(request_document.get("formatVersion") == 1, "unsupported driver request")
    endpoint = os.environ.get("NGKG_PHASE6_DRIVER_API", "")
    require(endpoint.startswith("https://") and "@" not in endpoint.split("//", 1)[1].split("/", 1)[0], "driver API must be credential-free HTTPS")
    ca = private_file("NGKG_PHASE6_DRIVER_CA_FILE")
    token = private_file("NGKG_PHASE6_DRIVER_TOKEN_FILE").read_text(encoding="utf-8").strip()
    require(bool(token) and "\n" not in token and "\r" not in token, "driver bearer token is invalid")
    context = ssl.create_default_context(cafile=str(ca))
    client_cert = os.environ.get("NGKG_PHASE6_DRIVER_CLIENT_CERT")
    if client_cert:
        context.load_cert_chain(str(private_file("NGKG_PHASE6_DRIVER_CLIENT_CERT")), str(private_file("NGKG_PHASE6_DRIVER_CLIENT_KEY")))
    outbound = urllib.request.Request(
        endpoint.rstrip("/") + "/v1/qualification/actions",
        data=json.dumps(request_document, sort_keys=True, separators=(",", ":")).encode(),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-NGKG-Subject-Sha256": request_document["subjectSha256"],
        },
    )
    opener = urllib.request.build_opener(
        urllib.request.HTTPSHandler(context=context),
        NoRedirect(),
        urllib.request.HTTPErrorProcessor(),
    )
    try:
        with opener.open(outbound, timeout=int(os.environ.get("NGKG_PHASE6_DRIVER_TIMEOUT_SECONDS", "172800"))) as response:
            require(response.status == 200, f"driver API returned HTTP {response.status}")
            require(response.headers.get_content_type() == "application/json", "driver API returned the wrong content type")
            payload = response.read(MAX_RESPONSE + 1)
    except urllib.error.HTTPError as error:
        # No response body is emitted: site diagnostics can contain credentials.
        raise RuntimeError(f"driver API returned HTTP {error.code}") from error
    require(len(payload) <= MAX_RESPONSE, "driver response exceeds byte limit")
    document = json.loads(payload)
    require(document.get("subjectSha256") == request_document["subjectSha256"], "driver response subject mismatch")
    require(document.get("provider") == request_document["provider"], "driver response provider mismatch")
    require(document.get("action") == request_document["action"], "driver response action mismatch")
    document["transportEvidenceSha256"] = hashlib.sha256(payload).hexdigest()
    sys.stdout.write(json.dumps(document, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, KeyError, ValueError, RuntimeError, json.JSONDecodeError) as error:
        print(f"qualification API driver failed: {error}", file=sys.stderr)
        raise SystemExit(1)
