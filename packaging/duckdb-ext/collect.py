#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 0b768f11-9d83-44aa-b195-49056c3b0458
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Fill provisa-duckdb-ext's _ext with what fits of ONE platform's DuckDB mirror, for that wheel.

One platform's full set of extensions is 136-208 MB compressed, and PyPI's per-file limit is 100 MB,
so a wheel holds a subset: WHEEL_PRIORITY's order, skipping what would pass WHEEL_BUDGET_BYTES. The
Provisa installers and the container image stage the whole set at build time (scripts/
stage_duckdb_extensions.py); a pip user runs `python -m provisa.federation.stage_duckdb_extensions`
for the rest. An extension is listed here and nowhere else; one not listed stops the build, by name,
so a new extension can never be left out silently.

CI runs scripts/ci/mirror_duckdb_extensions.py on each platform of the matrix; each mirror is extracted
as ``<label>/<version>/<duckdb_platform>/``.

Usage:
  python collect.py collect <artifacts_root> <label>   # _ext := what fits of that label's mirror
  python collect.py labels                             # the platform labels, one per line
  python collect.py wheel-tag <label>                  # the wheel platform tag of a label
  python collect.py check-wheels <dist_dir>            # one wheel per platform, each < 100 MB
"""

from __future__ import annotations

import shutil
import sys
import tomllib
import zlib
from pathlib import Path

_HERE = Path(__file__).resolve().parent

#: The mirror matrix's labels (build-duckdb-extensions.yml) -> (the platform DuckDB names itself
#: there, PRAGMA platform, which is the dir the runtime stages from; the wheel's platform tag).
PLATFORMS: dict[str, tuple[str, str]] = {
    "osx-arm64": ("osx_arm64", "macosx_11_0_arm64"),
    "linux-x64": ("linux_amd64", "manylinux_2_28_x86_64"),
    "linux-arm64": ("linux_arm64", "manylinux_2_28_aarch64"),
    "windows-x64": ("windows_amd64", "win_amd64"),
}

#: The extensions, most wanted first; a tuple travels together or not at all (LOAD motherduck pulls in
#: motherduck_impl). What a connection of any tier needs comes first, then the warehouse and SaaS
#: connectors by how many deployments reach for them.
WHEEL_PRIORITY: tuple[tuple[str, ...], ...] = (
    ("postgres_scanner",),
    ("sqlite_scanner",),
    ("httpfs",),
    ("azure",),
    ("iceberg", "avro"),
    ("vss",),
    ("inet",),
    ("crypto",),
    ("delta",),
    ("mssql",),
    ("snowflake",),
    ("bigquery",),
    ("mongo",),
    ("gsheets",),
    ("firebird",),
    ("airport",),
    ("motherduck", "motherduck_impl"),
)

#: PyPI's per-file limit, in decimal megabytes.
MAX_WHEEL_BYTES = 100 * 1000 * 1000

#: What the blobs may compress to, below the limit by the wheel's own files and zip entries.
WHEEL_BUDGET_BYTES = 97 * 1000 * 1000


def _ext_dir() -> Path:
    return _HERE / "provisa_duckdb_ext" / "_ext"


def _compressed_size(path: Path) -> int:
    """What ``path`` adds to a wheel: a wheel is a zip, deflated at zlib's default level."""
    return len(zlib.compress(path.read_bytes()))


def _pinned_duckdb_version() -> str:
    deps = tomllib.loads((_HERE / "pyproject.toml").read_text())["project"]["dependencies"]
    (pin,) = [d for d in deps if d.startswith("duckdb==")]
    return pin.removeprefix("duckdb==")


def _err(message: str) -> int:
    print(f"[collect] ERROR: {message}", file=sys.stderr)
    return 1


def _select(found: list[Path]) -> tuple[list[Path], list[str]] | str:
    """The files the wheel carries and the names left out, walking WHEEL_PRIORITY against the budget.
    A file WHEEL_PRIORITY does not list is returned as an error message."""
    by_name = {f.name.split(".")[0]: f for f in found}
    listed = {name for group in WHEEL_PRIORITY for name in group}
    unlisted = sorted(set(by_name) - listed)
    if unlisted:
        return f"{', '.join(unlisted)}: not listed; add to WHEEL_PRIORITY in collect.py"
    carried: list[Path] = []
    left_out: list[str] = []
    total = 0
    for group in WHEEL_PRIORITY:
        files = [by_name[name] for name in group if name in by_name]
        size = sum(_compressed_size(f) for f in files)
        if total + size <= WHEEL_BUDGET_BYTES:
            carried += files
            total += size
        else:
            left_out += [f.name.split(".")[0] for f in files]
    return carried, left_out


def collect(src_root: Path, label: str) -> int:
    """Replace _ext with what fits of ``<src_root>/<label>``'s extensions, in the layout DuckDB
    loads from: ``<raw_version>/<duckdb_platform>/<name>.duckdb_extension``."""
    if label not in PLATFORMS:
        return _err(f"unknown platform label {label!r}; known: {', '.join(PLATFORMS)}")
    duckdb_platform = PLATFORMS[label][0]
    src = src_root / label
    if not src.is_dir():
        return _err(f"no mirror for {label} under {src_root}")
    pinned = _pinned_duckdb_version()
    # The previous platform's blobs must never ride along in this platform's wheel.
    shutil.rmtree(_ext_dir(), ignore_errors=True)
    found = sorted(src.rglob("*.duckdb_extension"))
    if not found:
        return _err(f"no extension binaries found for {label}: nothing to package")
    for ext_file in found:
        version, platform = ext_file.parts[-3:-1]
        if version.lstrip("v") != pinned:
            return _err(
                f"{ext_file} was mirrored from DuckDB {version}, but the wheels pin duckdb=={pinned}"
            )
        if platform != duckdb_platform:
            return _err(f"{ext_file} is not under DuckDB platform {duckdb_platform} ({label})")
    selection = _select(found)
    if isinstance(selection, str):
        return _err(selection)
    carried, left_out = selection
    for ext_file in carried:
        version, platform = ext_file.parts[-3:-1]
        dst = _ext_dir() / version / platform / ext_file.name
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ext_file, dst)
    print(f"[collect] {label}: {len(carried)} extensions in the wheel")
    print(f"[collect] {label}: left to the stage command: {', '.join(left_out) or 'none'}")
    return 0


def check_wheels(dist_dir: Path) -> int:
    """One wheel for every platform, and each fits PyPI's limit."""
    wheels = {w.name: w.stat().st_size for w in dist_dir.glob("*.whl")}
    code = 0
    for _duckdb_platform, tag in PLATFORMS.values():
        named = [
            n for n in wheels if n.startswith("provisa_duckdb_ext-") and n.endswith(f"-{tag}.whl")
        ]
        if len(named) != 1:
            code = _err(f"expected one provisa-duckdb-ext wheel for {tag}; found {named}")
            continue
        print(f"[collect] {named[0]}: {wheels[named[0]] / 1e6:.1f} MB")
        if wheels[named[0]] >= MAX_WHEEL_BYTES:
            code = _err(f"{named[0]} is past PyPI's 100 MB limit; lower WHEEL_BUDGET_BYTES")
    return code


def main(argv: list[str]) -> int:
    match argv:
        case ["collect", src_root, label]:
            return collect(Path(src_root).resolve(), label)
        case ["labels"]:
            print("\n".join(PLATFORMS))
            return 0
        case ["wheel-tag", label]:
            print(PLATFORMS[label][1])
            return 0
        case ["check-wheels", dist_dir]:
            return check_wheels(Path(dist_dir).resolve())
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
