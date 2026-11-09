#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 0b768f11-9d83-44aa-b195-49056c3b0458
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Fill provisa_duckdb_ext/_ext with ONE version part of ONE platform's DuckDB mirror (REQ-294).

provisa-duckdb-ext stays one PyPI project, split across version parts ``<release>.<part>``, each
published as one wheel per platform and every wheel under PyPI's 100 MB per-file limit. PARTS names
which extensions each part holds, the same on every platform, so every part exists for every platform
and pip finds it. It carries what the duckdb-extension-* wheels lack: crypto and the community
extensions. CI runs scripts/ci/mirror_duckdb_extensions.py on each platform of the matrix; each mirror
is extracted as ``<label>/<version>/<duckdb_platform>/`` (a named repository's extension, crypto's or
airport's, under ``repositories/<repo>/`` with its ``.info``).

Usage:
  python collect.py collect <artifacts_root> <label> <part>  # _ext := that part of that label's mirror
  python collect.py labels                                   # the platform labels, one per line
  python collect.py parts                                    # the part numbers, one per line
  python collect.py wheel-tag <label>                        # the wheel platform tag of a label
  python collect.py set-version <part>                       # pyproject's version := <release>.<part>
  python collect.py check-wheels <dist_dir>                  # one wheel per platform and part, < 100 MB
"""

from __future__ import annotations

import re
import shutil
import sys
import tomllib
import zlib
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
#: Room kept under the limit for what the fill does not count: the wheel's metadata, zip headers, and
#: the gap between this estimate and the wheel builder's own compression. check-wheels is the gate.
HEADROOM_BYTES = 2 * 1000 * 1000

#: The extensions of each version part, part 1 first (provisa.federation.duckdb_extensions.
#: EXT_PACKAGE_PARTS is their count; tests/unit/test_duckdb_ext_collect.py holds the two equal, and
#: tests/unit/test_mirror_duckdb_extensions.py that every community extension the connectors declare
#: is here). Measured for DuckDB 2.0 (compressed, largest platform linux_amd64): part 1 is 77.1 MB and
#: part 2 68.6 MB. A part that passes its budget stops the build, by name, as does a mirrored extension
#: no part names.
PARTS: tuple[tuple[str, ...], ...] = (
    ("crypto", "mssql", "snowflake", "bigquery"),
    ("mongo", "gsheets", "airport", "firebird"),
)
PRIORITY = tuple(name for part in PARTS for name in part)


def _pinned_duckdb_version() -> str:
    deps = tomllib.loads((_HERE / "pyproject.toml").read_text())["project"]["dependencies"]
    (pin,) = [d for d in deps if d.startswith("duckdb==")]
    return pin.removeprefix("duckdb==")


def _err(message: str) -> int:
    print(f"[collect] ERROR: {message}", file=sys.stderr)
    return 1


def _deflated_size(path: Path) -> int:
    """The bytes ``path`` takes in a wheel: deflate at zlib's default level, as the wheel builder
    writes it."""
    deflate = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, -15)
    size = 0
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):
            size += len(deflate.compress(chunk))
    return size + len(deflate.flush())


def main(
    src_root: Path, label: str, part: int, budget: int = MAX_WHEEL_BYTES - HEADROOM_BYTES
) -> int:
    """Replace _ext with part ``part`` of ``<src_root>/<label>``'s extensions, in the layout DuckDB
    loads from: ``<raw_version>/<duckdb_platform>/[repositories/<repo>/]<name>.duckdb_extension[.info]``.
    Over ``budget``, or with a mirrored extension no part names, the build stops by name."""
    if not 1 <= part <= len(PARTS):
        return _err(f"no part {part}: {len(PARTS)} parts")
    if label not in PLATFORMS:
        return _err(f"unknown platform label {label!r}; known: {', '.join(PLATFORMS)}")
    duckdb_platform = PLATFORMS[label][0]
    src = src_root / label
    if not src.is_dir():
        return _err(f"no mirror for {label} under {src_root}")
    pinned = _pinned_duckdb_version()
    # The previous platform's blobs must never ride along in this platform's wheel.
    shutil.rmtree(_DEST, ignore_errors=True)
    # extension name -> its files (the binary and, for a named repository's, its .info), each with
    # its path in the layout.
    extensions: dict[str, list[tuple[Path, Path]]] = {}
    # A named repository's trust anchor (<repo>.duckdb_extension.repo.json), by the directory it
    # shares with that repository's extensions: carried with each of them, never on its own.
    anchors: dict[Path, tuple[Path, Path]] = {}
    for ext_file in sorted(src.rglob("*.duckdb_extension*")):
        is_anchor = ext_file.name.endswith(".duckdb_extension.repo.json")
        if ext_file.suffix not in (".duckdb_extension", ".info") and not is_anchor:
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
        if is_anchor:
            anchors[rel.parent] = (ext_file, rel)
            continue
        name = ext_file.name.split(".duckdb_extension")[0]
        extensions.setdefault(name, []).append((ext_file, rel))
    for files in extensions.values():
        anchor = anchors.get(files[0][1].parent)
        if anchor is not None:
            files.append(anchor)
    if not extensions:
        return _err(f"no extension binaries found for {label} — nothing to package")
    unassigned = sorted(set(extensions) - set(PRIORITY))
    if unassigned:
        return _err(f"{', '.join(unassigned)}: in no part; add to PARTS in collect.py")
    packed: list[str] = []
    used = 0
    for name in PARTS[part - 1]:
        if name not in extensions:
            continue
        used += sum(_deflated_size(f) for f, _rel in extensions[name])
        packed.append(name)
        for ext_file, rel in extensions[name]:
            dst = _DEST / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ext_file, dst)
    unmirrored = [n for n in PARTS[part - 1] if n not in extensions]
    print(
        f"[collect] {label} part {part}: packed {', '.join(packed) or 'nothing'} "
        f"({used / 1e6:.1f} MB compressed) into {_DEST}"
    )
    print(f"[collect] {label} part {part}: not mirrored: {', '.join(unmirrored) or 'none'}")
    if used > budget:
        return _err(
            f"{label} part {part} is {used / 1e6:.1f} MB compressed, past the "
            f"{budget / 1e6:.1f} MB budget; move an extension to another part in PARTS"
        )
    return 0


def set_version(part: int) -> int:
    """Write ``<release>.<part>`` as this project's version, for the wheel build that follows."""
    path = _HERE / "pyproject.toml"
    text = path.read_text()
    release = re.search(r'^version = "(\d+\.\d+\.\d+)(?:\.\d+)?"', text, re.M)
    if release is None:
        return _err("no version line in pyproject.toml")
    path.write_text(
        text[: release.start()] + f'version = "{release.group(1)}.{part}"' + text[release.end() :]
    )
    return 0


def check_wheels(dist: Path) -> int:
    """Exactly one wheel per platform and part, each tagged ``py3-none-<its tag>`` and under PyPI's
    limit."""
    wheels = sorted(dist.glob("*.whl"))
    failures: list[str] = []
    for label, (_plat, tag) in PLATFORMS.items():
        for part in range(1, len(PARTS) + 1):
            suffix = f".{part}-py3-none-{tag}.whl"
            mine = [w for w in wheels if w.name.endswith(suffix)]
            if len(mine) != 1:
                failures.append(f"{label} part {part}: expected one *{suffix}, found {len(mine)}")
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
        case ["collect", root, label, part]:
            return main(Path(root).resolve(), label, int(part))
        case ["parts"]:
            print("\n".join(str(p) for p in range(1, len(PARTS) + 1)))
            return 0
        case ["set-version", part]:
            return set_version(int(part))
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
