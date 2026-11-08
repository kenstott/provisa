#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 0b768f11-9d83-44aa-b195-49056c3b0458
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Fill each provisa-duckdb-ext distribution's _ext with ONE platform's DuckDB mirror, for that wheel.

One platform's full set of extensions is 136-208 MB compressed, and PyPI's per-file limit for these
projects is 100 MB, so the set is split across three distributions (PARTS) and each is published as one
wheel per platform. provisa-duckdb-ext depends on the other two at its own version, so installing it
installs all three. An extension is assigned to a part here and nowhere else; one assigned to no part
stops the build, by name, so a new extension can never be left out silently.

CI runs scripts/ci/mirror_duckdb_extensions.py on each platform of the matrix; each mirror is extracted
as ``<label>/<version>/<duckdb_platform>/``.

Usage:
  python collect.py collect <artifacts_root> <label>   # each part's _ext := that label's mirror
  python collect.py labels                             # the platform labels, one per line
  python collect.py dirs                               # the part package directories, one per line
  python collect.py wheel-tag <label>                  # the wheel platform tag of a label
  python collect.py check-wheels <dist_dir>            # every part per platform, each < 100 MB
"""

from __future__ import annotations

import shutil
import sys
import tomllib
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_PACKAGING = _HERE.parent

#: The mirror matrix's labels (build-duckdb-extensions.yml) -> (the platform DuckDB names itself
#: there, PRAGMA platform, which is the dir the runtime stages from; the wheel's platform tag).
PLATFORMS: dict[str, tuple[str, str]] = {
    "osx-arm64": ("osx_arm64", "macosx_11_0_arm64"),
    "linux-x64": ("linux_amd64", "manylinux_2_28_x86_64"),
    "linux-arm64": ("linux_arm64", "manylinux_2_28_aarch64"),
    "windows-x64": ("windows_amd64", "win_amd64"),
}

#: Distribution -> the extensions it carries (a file's name up to its first dot). Sized from DuckDB
#: 1.5.5's linux_amd64, the largest platform: 82, 87 and 60 MB compressed.
PARTS: dict[str, tuple[str, ...]] = {
    "provisa-duckdb-ext": (
        "postgres_scanner",
        "sqlite_scanner",
        "httpfs",
        "azure",
        "iceberg",
        "avro",
        "vss",
        "inet",
        "crypto",
    ),
    "provisa-duckdb-ext-warehouse": (
        "delta",
        "bigquery",
        "snowflake",
        "motherduck",
        "motherduck_impl",
        "mssql",
    ),
    "provisa-duckdb-ext-saas": ("airport", "mongo", "firebird", "gsheets"),
}

#: PyPI's per-file limit for these projects, in decimal megabytes.
MAX_WHEEL_BYTES = 100 * 1000 * 1000


def _package_dir(dist: str) -> Path:
    """Where a distribution's project lives: packaging/<dist minus the provisa- prefix>."""
    return _PACKAGING / dist.removeprefix("provisa-")


def _ext_dir(dist: str) -> Path:
    return _package_dir(dist) / dist.replace("-", "_") / "_ext"


def _pinned_duckdb_version() -> str:
    deps = tomllib.loads((_HERE / "pyproject.toml").read_text())["project"]["dependencies"]
    (pin,) = [d for d in deps if d.startswith("duckdb==")]
    return pin.removeprefix("duckdb==")


def _err(message: str) -> int:
    print(f"[collect] ERROR: {message}", file=sys.stderr)
    return 1


def _part_of(name: str) -> str | None:
    return next((dist for dist, names in PARTS.items() if name in names), None)


def collect(src_root: Path, label: str) -> int:
    """Replace every part's _ext with ``<src_root>/<label>``'s extensions, in the layout DuckDB
    loads from: ``<raw_version>/<duckdb_platform>/<name>.duckdb_extension``."""
    if label not in PLATFORMS:
        return _err(f"unknown platform label {label!r}; known: {', '.join(PLATFORMS)}")
    duckdb_platform = PLATFORMS[label][0]
    src = src_root / label
    if not src.is_dir():
        return _err(f"no mirror for {label} under {src_root}")
    pinned = _pinned_duckdb_version()
    # The previous platform's blobs must never ride along in this platform's wheel.
    for dist in PARTS:
        shutil.rmtree(_ext_dir(dist), ignore_errors=True)
    found = sorted(src.rglob("*.duckdb_extension"))
    if not found:
        return _err(f"no extension binaries found for {label}: nothing to package")
    counts = dict.fromkeys(PARTS, 0)
    for ext_file in found:
        version, platform = ext_file.parts[-3:-1]
        if version.lstrip("v") != pinned:
            return _err(
                f"{ext_file} was mirrored from DuckDB {version}, but the wheels pin duckdb=={pinned}"
            )
        if platform != duckdb_platform:
            return _err(f"{ext_file} is not under DuckDB platform {duckdb_platform} ({label})")
        name = ext_file.name.split(".")[0]
        dist = _part_of(name)
        if dist is None:
            return _err(f"{name} is assigned to no distribution; add it to PARTS in collect.py")
        dst = _ext_dir(dist) / version / platform / ext_file.name
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ext_file, dst)
        counts[dist] += 1
    for dist, count in counts.items():
        print(f"[collect] {label}: {dist}: {count} extensions")
    return 0


def check_wheels(dist_dir: Path) -> int:
    """Every part has a wheel for every platform, and each fits PyPI's limit."""
    wheels = {w.name: w.stat().st_size for w in dist_dir.glob("*.whl")}
    code = 0
    for dist in PARTS:
        stem = dist.replace("-", "_") + "-"
        for _duckdb_platform, tag in PLATFORMS.values():
            named = [n for n in wheels if n.startswith(stem) and n.endswith(f"-{tag}.whl")]
            if len(named) != 1:
                code = _err(f"expected one {dist} wheel for {tag}; found {named}")
                continue
            print(f"[collect] {named[0]}: {wheels[named[0]] / 1e6:.1f} MB")
            if wheels[named[0]] >= MAX_WHEEL_BYTES:
                code = _err(f"{named[0]} is past PyPI's 100 MB limit; move an extension in PARTS")
    return code


def main(argv: list[str]) -> int:
    match argv:
        case ["collect", src_root, label]:
            return collect(Path(src_root).resolve(), label)
        case ["labels"]:
            print("\n".join(PLATFORMS))
            return 0
        case ["dirs"]:
            print("\n".join(str(_package_dir(dist)) for dist in PARTS))
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
