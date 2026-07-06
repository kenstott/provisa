#!/usr/bin/env python
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Startup step: stage the DuckDB federation extensions and report each probe (REQ-899, REQ-904).

Run once at startup / packaging. It INSTALLs each community extension that backs a DuckDB federation
connector into DuckDB's extension directory (so it is reachable offline by the federation runtime),
then LOAD-probes each and prints whether the probe succeeded. It does NOT open any live source
connection — a green row means "the extension is installed and its scanner symbol is registered",
not "a live database is reachable".

Set PROVISA_DUCKDB_EXT_DIR to stage into a bundled directory; the federation runtime reads the same
variable. Exit code is non-zero if any probe failed, so a start script can gate on it.
"""

from __future__ import annotations

import asyncio
import sys

from provisa.federation.duckdb_extensions import (
    extension_directory,
    stage_and_probe,
)


async def _main() -> int:
    ext_dir = extension_directory()
    print(f"Staging DuckDB federation extensions into: {ext_dir or 'DuckDB default cache (~/.duckdb)'}")
    probes = await stage_and_probe()

    width = max(len(p.key) for p in probes)
    failed = 0
    for p in probes:
        mark = "OK  " if p.available else "FAIL"
        if not p.available:
            failed += 1
        line = f"  [{mark}] {p.key.ljust(width)}  {p.extension:<10} -> {p.source_type:<12} {p.reason}"
        print(line)
        if not p.available and p.remediation:
            print(f"         remediation: {p.remediation}")

    total = len(probes)
    print(f"\n{total - failed}/{total} extensions probed OK.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
