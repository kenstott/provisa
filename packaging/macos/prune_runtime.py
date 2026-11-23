# Copyright (c) 2026 Kenneth Stott
# Canary: 3f0c9a52-7d1e-4b8a-9e64-2c5d8f1a7b03
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prune non-runtime payload from the macOS native runtime's site-packages before signing.

The DMG is notarized, and Apple unpacks every archive in it to inspect the executables inside.
A file named like an archive that is not one fails that: dry run 45212409648 was rejected for
joblib/test/data/joblib_*_compressed_pickle_*.gz, joblib's own zlib pickles, which are not gzip.
Third-party test suites and their fixtures are not part of the runtime (REQ-294: the installer
keeps runtime only), so every package's ``test``/``tests`` directory is removed. What remains is
then scanned, and any archive that still cannot be read as its suffix says fails the build.

Usage: python -I prune_runtime.py <site-packages>
"""

from __future__ import annotations

import bz2
import gzip
import lzma
import shutil
import sys
import tarfile
import zipfile
from pathlib import Path

# A package's test suite: no runtime import reaches these (checked for the runtime's joblib,
# numpy, scipy, pandas, statsmodels, scikit-learn, torch, mpmath, jinja2 and openpyxl; jinja2's
# runtime ``tests`` is a module file, never a directory, so it is kept).
TEST_DIR_NAMES = frozenset({"test", "tests"})

_ZIP_SUFFIXES = frozenset({".zip", ".whl", ".jar", ".egg"})
_STREAM_OPENERS = {".gz": gzip.open, ".tgz": gzip.open, ".bz2": bz2.open, ".xz": lzma.open}
ARCHIVE_SUFFIXES = _ZIP_SUFFIXES | frozenset(_STREAM_OPENERS) | frozenset({".tar"})
_CHUNK = 1 << 20


def test_dirs(site: Path) -> list[Path]:
    """The outermost ``test``/``tests`` directories under ``site``, sorted."""
    found: list[Path] = []
    for path in sorted(site.rglob("*")):
        if path.is_dir() and not path.is_symlink() and path.name in TEST_DIR_NAMES:
            if not any(parent in found for parent in path.parents):
                found.append(path)
    return found


def prune_test_dirs(site: Path) -> list[Path]:
    """Remove every package test directory under ``site``; return what was removed."""
    removed = test_dirs(site)
    for path in removed:
        shutil.rmtree(path)
    return removed


def _readable(path: Path) -> bool:
    suffix = path.suffix.lower()
    try:
        if suffix in _ZIP_SUFFIXES:
            if not zipfile.is_zipfile(path):
                return False
            with zipfile.ZipFile(path) as archive:
                return archive.testzip() is None
        if suffix == ".tar":
            return tarfile.is_tarfile(path)
        with _STREAM_OPENERS[suffix](path, "rb") as stream:
            while stream.read(_CHUNK):
                pass
        return True
    except (OSError, EOFError, zipfile.BadZipFile, lzma.LZMAError, tarfile.TarError):
        return False


def unreadable_archives(root: Path) -> list[Path]:
    """Files under ``root`` named like an archive that do not open as one, sorted."""
    return [
        path
        for path in sorted(root.rglob("*"))
        if path.is_file()
        and not path.is_symlink()
        and path.suffix.lower() in ARCHIVE_SUFFIXES
        and not _readable(path)
    ]


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: prune_runtime.py <site-packages>", file=sys.stderr)
        return 2
    site = Path(argv[1])
    if not site.is_dir():
        print(f"prune_runtime: {site} is not a directory", file=sys.stderr)
        return 2
    removed = prune_test_dirs(site)
    for path in removed:
        print(f"pruned {path.relative_to(site)}")
    print(f"prune_runtime: removed {len(removed)} test directories from {site}")
    bad = unreadable_archives(site)
    for path in bad:
        print(f"unreadable archive: {path.relative_to(site)}", file=sys.stderr)
    if bad:
        print(
            f"prune_runtime: {len(bad)} archive(s) notarization cannot unpack remain",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
