#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: db8808b2-4686-4f5b-95d7-a7fe3c097f54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Merge per-platform PG-extension build outputs into provisa_pg_ext/_ext for the universal wheel.

CI extracts each ``provisa-pg-ext-<os>-<arch>.tar.gz`` (built by scripts/ci/build_pg_extensions.sh)
into ``<artifacts_root>/<os>-<arch>/`` — each holding ``manifest.json`` + ``lib/<name>.<suf>``. This
gathers all of them into one tree so a single ``provisa-pg-ext`` wheel carries every platform.

Usage: python collect.py <artifacts_root>   # dir containing per-platform <os>-<arch>/ subtrees
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sys
from pathlib import Path

_DEST = Path(__file__).resolve().parent / "provisa_pg_ext" / "_ext"


def stale_rows(platform_dir: Path) -> list[str]:
    """Each manifest row whose file is missing or whose sha256 is not the file's. A wheel carrying
    such a row is refused by every installer that checks it (provisa.pg_extensions.external_install,
    the smoke test), and once on PyPI a version cannot be replaced -- 0.1.1 was published with five
    such rows -- so the tree is refused here, before the wheel is built."""
    manifest = json.loads((platform_dir / "manifest.json").read_text())
    stale = []
    for artifact in manifest["artifacts"]:
        path = platform_dir / artifact["file"]
        if not path.is_file():
            stale.append(f"{artifact['file']}: missing")
        elif hashlib.sha256(path.read_bytes()).hexdigest() != artifact["sha256"]:
            stale.append(f"{artifact['file']}: sha256 does not match its manifest row")
    return stale


def main(src_root: Path) -> int:
    # Each platform tree is identified by its manifest.json at the <os>-<arch>/ root.
    manifests = sorted(src_root.rglob("manifest.json"))
    if not manifests:
        print("[collect] ERROR: no manifest.json found — nothing to package", file=sys.stderr)
        return 1
    refused = {m.parent.name: stale_rows(m.parent) for m in manifests}
    refused = {plat: rows for plat, rows in refused.items() if rows}
    if refused:
        for plat, rows in refused.items():
            print(f"[collect] ERROR: {plat}: " + "; ".join(rows), file=sys.stderr)
        return 1
    n = 0
    for manifest in manifests:
        platform_dir = manifest.parent  # <artifacts_root>/.../<os>-<arch>
        dst = _DEST / platform_dir.name
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(platform_dir, dst)
        n += 1
        print(f"[collect] staged {platform_dir.name} -> {dst}")
    print(f"[collect] gathered {n} platform tree(s) into {_DEST}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(Path(sys.argv[1]).resolve()))
