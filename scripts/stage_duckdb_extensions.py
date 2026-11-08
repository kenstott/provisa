#!/usr/bin/env python
# Copyright (c) 2026 Kenneth Stott
# Canary: c8d11db1-1d9c-45d3-8507-93f58757e6a8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Build step: INSTALL every DuckDB extension a connection may load, then report each probe (REQ-294).

Run once on a connected machine: on a source checkout (the extensions land in DuckDB's per-user directory),
in an image build, or wherever a bundle is put together. The runtime never does this: it only LOADs, and
refuses by name what is missing. It does NOT open any live source connection -- a green row means "the
extension is installed and its scanner symbol is registered", not "a live database is reachable".

Set PROVISA_DUCKDB_EXT_DIR to stage into a bundled directory; the federation runtime reads the same
variable. Exit code is non-zero if any probe failed, so a build can gate on it.
"""

from __future__ import annotations

import asyncio
import sys

from provisa.federation.duckdb_extensions import (
    connect,
    extension_directory,
    probe_extensions,
    required_extensions,
)


def _install() -> None:
    con = connect()
    try:
        for name, community in required_extensions().items():
            con.execute(f"INSTALL {name}" + (" FROM community" if community else ""))
    finally:
        con.close()


async def _main() -> int:
    ext_dir = extension_directory()
    print(
        f"Staging DuckDB federation extensions into: {ext_dir or 'DuckDB default directory (~/.duckdb)'}"
    )
    _install()
    probes = await probe_extensions()

    width = max(len(p.key) for p in probes)
    failed = 0
    for p in probes:
        mark = "OK  " if p.available else "FAIL"
        if not p.available:
            failed += 1
        line = (
            f"  [{mark}] {p.key.ljust(width)}  {p.extension:<10} -> {p.source_type:<12} {p.reason}"
        )
        print(line)
        if not p.available and p.remediation:
            print(f"         remediation: {p.remediation}")

    total = len(probes)
    print(f"\n{total - failed}/{total} extensions probed OK.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
