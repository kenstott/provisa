# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Build step: INSTALL every DuckDB extension a connection may load, then report each probe (REQ-294).

Run once on a connected machine: on a source checkout (``scripts/stage_duckdb_extensions.py``), in an
image or installer build, or after ``pip install provisa[embedded]``, whose wheel holds only what fits
under PyPI's per-file limit. The runtime never does this: it only LOADs, and refuses by name what is
missing. It does NOT open any live source connection -- a green row means "the extension is installed
and its scanner symbol is registered", not "a live database is reachable".

``PROVISA_DUCKDB_EXT_DIR`` names the directory to stage into; the federation runtime reads the same
variable. Without it, ``python -m provisa.federation.stage_duckdb_extensions`` stages where ``provisa
run`` looks by default, and ``scripts/stage_duckdb_extensions.py`` into DuckDB's per-user directory.
Exit code is non-zero if any probe failed, so a build can gate on it.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from provisa.core import engine_memory
from provisa.federation.duckdb_extensions import (
    connect,
    extension_directory,
    probe_extensions,
    required_extensions,
)

#: Where ``provisa run`` stages the wheel's blobs by default (``cli._DEFAULT_DATA_DIR``).
RUN_DIRECTORY = Path.home() / ".provisa" / "native" / "duckdb-ext"


def _install() -> None:
    con = connect()
    try:
        for name, community in required_extensions().items():
            con.execute(f"INSTALL {name}" + (" FROM community" if community else ""))
    finally:
        con.close()


async def _stage() -> int:
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


def main(default_directory: Path | None = RUN_DIRECTORY) -> int:
    """Stage into ``PROVISA_DUCKDB_EXT_DIR``, else ``default_directory`` (None: DuckDB's own)."""
    if default_directory is not None and not os.environ.get(engine_memory.EXT_DIR_ENV):
        default_directory.mkdir(parents=True, exist_ok=True)
        os.environ[engine_memory.EXT_DIR_ENV] = str(default_directory)
    return asyncio.run(_stage())


if __name__ == "__main__":
    sys.exit(main())
