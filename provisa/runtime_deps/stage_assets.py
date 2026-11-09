# Copyright (c) 2026 Kenneth Stott
# Canary: b581ff70-46c6-4b8c-aa96-b3a23987e92f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""The one stage command of every Provisa install (REQ-294, EVERY INSTALLER WORKS THE SAME).

Places the DuckDB extensions and the pgwire connector bundles in local directories outside the
installed application, once, at install or first launch, using the machine's configured package
index: pip's for the extensions, the Maven repository for the bundles. A directory already filled,
as a devops step does for an air-gapped site, is used without fetching anything.

  python -I -m provisa.runtime_deps.stage_assets [home]

``home`` gives the two directories their install's place (``<home>/duckdb-ext``, ``<home>/runtime-deps``);
without it ``PROVISA_DUCKDB_EXT_DIR`` and ``PROVISA_RUNTIME_DEPS_CACHE`` do. Exit code is non-zero,
naming what is missing and the fix, unless everything is staged afterwards.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable, Mapping
from pathlib import Path

#: The directories under an install's home. The launchers spell the same two names.
DUCKDB_DIR = "duckdb-ext"
BUNDLES_DIR = "runtime-deps"

#: Each step as this process runs: an isolated Python (-I, an install's) or not (the image, whose
#: provisa is /app and not site-packages).
_PY = ("-I", "-m") if sys.flags.isolated else ("-m",)
_STEPS = (
    (*_PY, "provisa.federation.stage_duckdb_extensions"),
    (*_PY, "provisa.federation.duckdb_extensions"),
    (*_PY, "provisa.runtime_deps.stage_pgwire_bundles"),
)

Runner = Callable[[tuple[str, ...], Mapping[str, str]], None]


def _run(args: tuple[str, ...], env: Mapping[str, str]) -> None:
    subprocess.run([sys.executable, *args], env={**os.environ, **env}, check=True)


def environment(home: Path | None) -> dict[str, str]:
    """The two directories' variables for ``home``. Without one, the environment's own, the
    extension directory defaulting to where ``provisa run`` loads from."""
    if home is None:
        if os.environ.get("PROVISA_DUCKDB_EXT_DIR"):
            return {}
        return {"PROVISA_DUCKDB_EXT_DIR": str(Path.home() / ".provisa" / "native" / DUCKDB_DIR)}
    return {
        "PROVISA_DUCKDB_EXT_DIR": str(home / DUCKDB_DIR),
        "PROVISA_RUNTIME_DEPS_CACHE": str(home / BUNDLES_DIR),
    }


def stage(home: Path | None, *, runner: Runner = _run) -> None:
    """Fill both directories and check them; each step is a process of the install's own Python."""
    for args in _STEPS:
        runner(args, environment(home))


def main(argv: list[str]) -> int:
    match argv:
        case []:
            home = None
        case [named]:
            home = Path(named).resolve()
        case _:
            print(__doc__)
            return 2
    try:
        stage(home)
    except subprocess.CalledProcessError as failed:
        print(f"stage assets: {' '.join(failed.cmd[1:])} failed (see above)", file=sys.stderr)
        return 1
    print("stage assets: every DuckDB extension and connector bundle is in place")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
