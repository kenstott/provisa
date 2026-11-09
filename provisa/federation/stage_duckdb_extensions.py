# Copyright (c) 2026 Kenneth Stott
# Canary: 9bf7b7ea-aef4-49dd-b598-7586672e7b85
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Stage the DuckDB extensions from the machine's configured package index (REQ-294).

The core extensions come from the ``duckdb-extension-*`` projects on PyPI and the rest from the
version parts of ``provisa-duckdb-ext``, each fetched with ``pip download`` so pip's own configuration
(``pip.conf``, ``PIP_INDEX_URL``: an enterprise Artifactory) names where. Run once at install or first
launch, through ``python -m provisa.runtime_deps.stage_assets``. A directory already filled is used
without fetching anything; the runtime only LOADs, and refuses by name what is missing.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path

from provisa.core import engine_memory
from provisa.federation.duckdb_extensions import (
    BundledExtensionsMissing,
    is_staged,
    stage_wheels,
    wheel_requirements,
)

#: Fetch these requirements' wheels into a directory. Injected so tests never reach an index.
Fetcher = Callable[[Sequence[str], Path], None]

#: Where ``provisa run`` looks by default (``cli._DEFAULT_DATA_DIR``).
RUN_DIRECTORY = Path.home() / ".provisa" / "native" / "duckdb-ext"


def pip_download(requirements: Sequence[str], dest: Path) -> None:
    """``pip download`` the wheels for this platform from the index pip is configured for."""
    command = [sys.executable, "-m", "pip", "download", "--no-deps", "--only-binary=:all:"]
    try:
        subprocess.run([*command, "--dest", str(dest), *requirements], check=True)
    except subprocess.CalledProcessError as failed:
        raise BundledExtensionsMissing(
            f"pip could not fetch the DuckDB extension wheels ({', '.join(requirements)}): exit "
            f"{failed.returncode}. Check the package index pip is configured for, or pre-fill the "
            f"directory {engine_memory.EXT_DIR_ENV} names."
        ) from failed


def stage(target: Path, *, fetch: Fetcher = pip_download) -> bool:
    """Fill ``target`` from the package index; False when it already is filled, nothing fetched."""
    if is_staged(target):
        return False
    target.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=target, prefix=".wheels.") as work:
        fetch(wheel_requirements(), Path(work))
        stage_wheels(sorted(Path(work).glob("*.whl")), target)
    return True


def main(default_directory: Path = RUN_DIRECTORY) -> int:
    """Stage into ``PROVISA_DUCKDB_EXT_DIR``, else ``default_directory``."""
    import os

    target = Path(os.environ.get(engine_memory.EXT_DIR_ENV) or default_directory)
    try:
        fetched = stage(target)
    except BundledExtensionsMissing as refused:
        print(f"FAIL: {refused}", file=sys.stderr)
        return 1
    print(f"DuckDB extensions {'staged into' if fetched else 'already in'} {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
