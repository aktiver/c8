#!/usr/bin/env python3
"""Create a sorted, normalized, secret-screened NGKG 1.0.0 source ZIP."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import stat
import time
import zipfile

FORBIDDEN_PARTS = {".git", "target", "__pycache__", ".pytest_cache"}
FORBIDDEN_NAMES = re.compile(r"(^|[._-])(id_rsa|id_ed25519|credentials|secrets?|token|private[-_]?key)([._-]|$)", re.IGNORECASE)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--root-name", default="NGKG_1_0_0_GA")
    parser.add_argument("--source-date-epoch", type=int, required=True)
    args = parser.parse_args()
    source = args.source.resolve()
    if not source.is_dir() or not args.root_name or "/" in args.root_name or args.source_date_epoch < 315532800:
        raise ValueError("source, archive root, or SOURCE_DATE_EPOCH is invalid")
    date = time.gmtime(args.source_date_epoch)[:6]
    paths = []
    for path in sorted(source.rglob("*")):
        relative = path.relative_to(source)
        if any(part in FORBIDDEN_PARTS for part in relative.parts):
            continue
        if path.is_symlink():
            raise ValueError(f"symlinks are forbidden in GA archives: {relative}")
        if path.is_file():
            if FORBIDDEN_NAMES.search(path.name) and "test-corpus" not in relative.parts:
                raise ValueError(f"credential-like filename is forbidden: {relative}")
            paths.append(path)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(args.output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9, strict_timestamps=True) as archive:
        for path in paths:
            info = zipfile.ZipInfo(f"{args.root_name}/{path.relative_to(source).as_posix()}", date_time=date)
            mode = 0o755 if path.stat().st_mode & stat.S_IXUSR else 0o644
            info.external_attr = (stat.S_IFREG | mode) << 16
            info.create_system = 3; info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, path.read_bytes(), compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)
    print(json.dumps({"archive": str(args.output), "files": len(paths), "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest()}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
