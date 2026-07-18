#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 0b768f11-9d83-44aa-b195-49056c3b0458
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Merge per-platform DuckDB mirror outputs into provisa_duckdb_ext/_ext for the universal wheel.

CI runs scripts/ci/mirror_duckdb_extensions.py on each platform (macos/linux/windows × arch); each
emits ``<version>/<platform>/*.duckdb_extension``. This gathers all of them (from downloaded build
artifacts) into one tree so a single ``provisa-duckdb-ext`` wheel carries every platform.

Usage: python collect.py <artifacts_root>   # a dir containing the per-platform mirror outputs
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

_DEST = Path(__file__).resolve().parent / "provisa_duckdb_ext" / "_ext"


def main(src_root: Path) -> int:
    n = 0
    for ext_file in sorted(src_root.rglob("*.duckdb_extension")):
        # Preserve the trailing <raw_version>/<platform>/<name>.duckdb_extension the loader expects.
        rel = Path(*ext_file.parts[-3:])
        dst = _DEST / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ext_file, dst)
        n += 1
    print(f"[collect] gathered {n} .duckdb_extension binaries into {_DEST}")
    if not n:
        print("[collect] ERROR: no extension binaries found — nothing to package", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(Path(sys.argv[1]).resolve()))
