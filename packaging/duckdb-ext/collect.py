#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 0b768f11-9d83-44aa-b195-49056c3b0458
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Fill provisa_duckdb_ext/_ext with ONE platform's DuckDB mirror, for that platform's wheel.

provisa-duckdb-ext is published as one wheel per platform (maintainer decision 2026-11-06: a single
wheel of every platform's DuckDB 2.0 extensions passes PyPI's 100 MB limit), each tagged for its
platform and holding only that platform's extensions. CI runs scripts/ci/mirror_duckdb_extensions.py
on each platform of the matrix; each mirror is extracted as ``<label>/<version>/<duckdb_platform>/``
(a named repository's extension, crypto's, under ``repositories/<repo>/`` with its ``.info``).

Usage:
  python collect.py collect <artifacts_root> <label>   # _ext := that label's mirror, nothing else
  python collect.py labels                             # the platform labels, one per line
  python collect.py wheel-tag <label>                  # the wheel platform tag of a label
  python collect.py check-wheels <dist_dir>            # one wheel per platform, each < 100 MB
"""

from __future__ import annotations

import re
import shutil
import sys
import tomllib
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_DEST = _HERE / "provisa_duckdb_ext" / "_ext"
#: DuckDB's on-disk version dir ("v2.0.0"), where the loader's layout begins.
_VERSION_DIR = re.compile(r"v\d+\.\d+\.\d+.*")

#: The mirror matrix's labels (build-duckdb-extensions.yml) -> (the platform DuckDB names itself
#: there, PRAGMA platform, which is the dir the runtime stages from; the wheel's platform tag).
PLATFORMS: dict[str, tuple[str, str]] = {
    "osx-arm64": ("osx_arm64", "macosx_11_0_arm64"),
    "linux-x64": ("linux_amd64", "manylinux_2_28_x86_64"),
    "linux-arm64": ("linux_arm64", "manylinux_2_28_aarch64"),
    "windows-x64": ("windows_amd64", "win_amd64"),
}

#: PyPI's per-file limit for this project ("Limit for project 'provisa-duckdb-ext' is 100 MB").
#: Decimal megabytes: the stricter reading, so a wheel this passes PyPI accepts either way.
MAX_WHEEL_BYTES = 100 * 1000 * 1000


def _pinned_duckdb_version() -> str:
    deps = tomllib.loads((_HERE / "pyproject.toml").read_text())["project"]["dependencies"]
    (pin,) = [d for d in deps if d.startswith("duckdb==")]
    return pin.removeprefix("duckdb==")


def _err(message: str) -> int:
    print(f"[collect] ERROR: {message}", file=sys.stderr)
    return 1


def main(src_root: Path, label: str) -> int:
    """Replace _ext with ``<src_root>/<label>``'s extensions, in the layout DuckDB loads from:
    ``<raw_version>/<duckdb_platform>/[repositories/<repo>/]<name>.duckdb_extension[.info]``."""
    if label not in PLATFORMS:
        return _err(f"unknown platform label {label!r}; known: {', '.join(PLATFORMS)}")
    duckdb_platform = PLATFORMS[label][0]
    src = src_root / label
    if not src.is_dir():
        return _err(f"no mirror for {label} under {src_root}")
    pinned = _pinned_duckdb_version()
    # The previous platform's blobs must never ride along in this platform's wheel.
    shutil.rmtree(_DEST, ignore_errors=True)
    n = 0
    for ext_file in sorted(src.rglob("*.duckdb_extension*")):
        if ext_file.suffix not in (".duckdb_extension", ".info"):
            continue
        parts = ext_file.relative_to(src).parts
        start = next((i for i, part in enumerate(parts) if _VERSION_DIR.fullmatch(part)), None)
        if start is None:
            return _err(f"{ext_file} is under no DuckDB version dir")
        rel = Path(*parts[start:])
        if rel.parts[0].lstrip("v") != pinned:
            return _err(
                f"{ext_file} was mirrored from DuckDB {rel.parts[0]}, "
                f"but the wheel pins duckdb=={pinned}"
            )
        if len(rel.parts) < 3 or rel.parts[1] != duckdb_platform:
            return _err(
                f"{ext_file} is not under DuckDB platform {duckdb_platform}, "
                f"the platform of the {label} wheel"
            )
        dst = _DEST / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ext_file, dst)
        n += ext_file.suffix == ".duckdb_extension"
    print(f"[collect] gathered {n} {label} .duckdb_extension binaries into {_DEST}")
    if not n:
        return _err(f"no extension binaries found for {label} — nothing to package")
    return 0


def check_wheels(dist: Path) -> int:
    """Exactly one wheel per platform, each tagged ``py3-none-<its tag>`` and under PyPI's limit."""
    wheels = sorted(dist.glob("*.whl"))
    failures: list[str] = []
    for label, (_plat, tag) in PLATFORMS.items():
        mine = [w for w in wheels if w.name.endswith(f"-py3-none-{tag}.whl")]
        if len(mine) != 1:
            failures.append(f"{label}: expected one *-py3-none-{tag}.whl, found {len(mine)}")
    known = {f"-py3-none-{tag}.whl" for _plat, tag in PLATFORMS.values()}
    failures += [
        f"{w.name}: not a platform wheel"
        for w in wheels
        if not any(w.name.endswith(k) for k in known)
    ]
    for wheel in wheels:
        size = wheel.stat().st_size
        print(f"[collect] {wheel.name}: {size / 1e6:.1f} MB")
        if size >= MAX_WHEEL_BYTES:
            failures.append(f"{wheel.name} is {size / 1e6:.1f} MB, at or over PyPI's 100 MB limit")
    for failure in failures:
        _err(failure)
    return 1 if failures else 0


def _cli(argv: list[str]) -> int:
    match argv:
        case ["collect", root, label]:
            return main(Path(root).resolve(), label)
        case ["labels"]:
            print("\n".join(PLATFORMS))
            return 0
        case ["wheel-tag", label] if label in PLATFORMS:
            print(PLATFORMS[label][1])
            return 0
        case ["check-wheels", dist]:
            return check_wheels(Path(dist))
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(_cli(sys.argv[1:]))
