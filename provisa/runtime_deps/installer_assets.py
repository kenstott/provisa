# Copyright (c) 2026 Kenneth Stott
# Canary: b581ff70-46c6-4b8c-aa96-b3a23987e92f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""The one archive of runtime assets a native Provisa installer carries (REQ-294).

``build`` runs inside the runtime the installer ships: it stages every DuckDB extension and every
connector pgwire bundle but ``INSTALLER_EXCEPTED``, passes both install checks, and packs the two
directories. The seven bundles are 3.9 GB of which 2.9 GB are files they share (the JRE, the Calcite
jars), so each file is stored once and its copies as hard links; laid side by side they would pass
GitHub's 2 GiB limit on a release asset. ``unpack`` runs at first launch, puts them under the
install's home, repeats the checks there, and records the archive so that launching again does nothing.

  python -I -m provisa.runtime_deps.installer_assets build <out.tar.gz>
  python -I -m provisa.runtime_deps.installer_assets unpack <archive> <home>
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import tarfile
import tempfile
from collections.abc import Callable, Mapping
from pathlib import Path

from provisa.runtime_deps.pgwire_bundles import INSTALLER_EXCEPTED

#: The directories under an install's home: what first launch exports as PROVISA_DUCKDB_EXT_DIR and
#: PROVISA_RUNTIME_DEPS_CACHE. The launchers spell the same two names.
DUCKDB_DIR = "duckdb-ext"
BUNDLES_DIR = "runtime-deps"
_MARKER = ".provisa-assets"

_EXCEPT_ARGS = [arg for name in INSTALLER_EXCEPTED for arg in ("--except", name)]
_PY = ("-I", "-m")
_STAGE_MODULES = (
    (*_PY, "provisa.federation.stage_duckdb_extensions"),
    (*_PY, "provisa.runtime_deps.stage_pgwire_bundles", *_EXCEPT_ARGS),
)
_CHECK_MODULES = (
    (*_PY, "provisa.federation.duckdb_extensions"),
    (*_PY, "provisa.runtime_deps.pgwire_bundles", *_EXCEPT_ARGS),
)

Runner = Callable[[tuple[str, ...], Mapping[str, str]], None]


def _run(args: tuple[str, ...], env: Mapping[str, str]) -> None:
    subprocess.run([sys.executable, *args], env={**os.environ, **env}, check=True)


def _environment(root: Path) -> dict[str, str]:
    return {
        "PROVISA_DUCKDB_EXT_DIR": str(root / DUCKDB_DIR),
        "PROVISA_RUNTIME_DEPS_CACHE": str(root / BUNDLES_DIR),
    }


def _check(root: Path, runner: Runner) -> None:
    for args in _CHECK_MODULES:
        runner(args, _environment(root))


def pack(staged: Path, out: Path) -> None:
    """Pack ``staged`` into ``out``: each file once, its identical copies as hard links to it."""
    first_of: dict[tuple[str, int], str] = {}
    with tarfile.open(out, "w:gz", compresslevel=6) as tar:
        for path in sorted(staged.rglob("*")):
            if path.name.endswith(".lock"):
                continue  # the staging step's own (BundleResolver._fetch)
            arcname = path.relative_to(staged).as_posix()
            if path.is_symlink() or not path.is_file():
                tar.add(path, arcname=arcname, recursive=False)
                continue
            with path.open("rb") as content:
                digest = hashlib.file_digest(content, "sha256").hexdigest()
            info = tar.gettarinfo(path, arcname=arcname)
            # The mode is part of what is shared: extracting a link sets it on the one inode.
            key = (digest, info.mode)
            if key in first_of:
                info.type = tarfile.LNKTYPE
                info.linkname = first_of[key]
                info.size = 0
                tar.addfile(info)
            else:
                first_of[key] = arcname
                with path.open("rb") as content:
                    tar.addfile(info, content)


def build(out: Path, *, runner: Runner = _run) -> None:
    """Stage everything an installer holds, check it, and pack it into ``out``."""
    out.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=out.parent, prefix=".provisa-assets.") as work:
        staged = Path(work)
        for name in (DUCKDB_DIR, BUNDLES_DIR):
            (staged / name).mkdir()
        for args in _STAGE_MODULES:
            runner(args, _environment(staged))
        _check(staged, runner)
        pack(staged, out)


def _archive_id(archive: Path) -> str:
    with archive.open("rb") as content:
        return hashlib.file_digest(content, "sha256").hexdigest()


def unpack(archive: Path, home: Path, *, runner: Runner = _run) -> bool:
    """Lay ``archive`` out under ``home`` and check it; False when this archive already is."""
    marker = home / _MARKER
    archive_id = _archive_id(archive)
    if marker.is_file() and marker.read_text().strip() == archive_id:
        return False
    home.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive, "r:gz") as tar:
        # filter="data" rejects paths and links outside home (Python 3.12 safe extraction).
        tar.extractall(home, filter="data")
    _check(home, runner)
    marker.write_text(archive_id)
    return True


def main(argv: list[str]) -> int:
    try:
        match argv:
            case ["build", out]:
                build(Path(out).resolve())
                print(f"installer assets: {out}")
            case ["unpack", archive, home]:
                done = unpack(Path(archive).resolve(), Path(home).resolve())
                print(f"installer assets: {'unpacked into' if done else 'already in'} {home}")
            case _:
                print(__doc__)
                return 2
    except subprocess.CalledProcessError as failed:
        print(f"installer assets: {' '.join(failed.cmd[1:])} failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
