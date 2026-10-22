# Copyright (c) 2026 Kenneth Stott
# Canary: 85ff9cb5-db88-488e-9b01-9f710f00cf91
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's bundle of files: unpacked from an archive with every entry kept inside its
directory, and hashed so a source records exactly what it was saved with (REQ-1984, REQ-1993)."""

# Requirements: REQ-1984, REQ-1993
from __future__ import annotations

import hashlib
import io
import tarfile
import zipfile
from pathlib import Path

#: How far an archive may expand: a bomb is small on the wire (the same bound the project
#: importer keeps, import_shared/upload.py).
MAX_EXPANDED_BYTES = 256 * 1024 * 1024

#: Directories a bundle's hash and its install never take from a fetched tree.
_IGNORED_DIRS = frozenset({".git"})


class BundleRefused(ValueError):
    """A bundle that cannot be used as given: named, with what is wrong."""

    def __init__(self, code: str, message: str, **params: str) -> None:
        super().__init__(message)
        self.code = f"codesource.{code}"
        self.params = params


def content_hash(root: Path, skipped: frozenset[str] = _IGNORED_DIRS) -> str:
    """The SHA-256 of the bundle under ``root``: every file, in sorted relative-path order, each
    as its path, its size and its bytes; a symbolic link as its path and target. The same files
    give the same hash on every host; any changed byte, name or link changes it. ``skipped``: the
    directories left out wherever they are (a code directory leaves out more than .git, REQ-2121)."""
    digest = hashlib.sha256()
    entries = sorted(
        p for p in root.rglob("*") if not skipped.intersection(p.relative_to(root).parts)
    )
    for path in entries:
        rel = path.relative_to(root).as_posix().encode()
        if path.is_symlink():
            digest.update(b"L\0" + rel + b"\0" + str(path.readlink()).encode() + b"\0")
        elif path.is_file():
            data = path.read_bytes()
            digest.update(b"F\0" + rel + b"\0" + str(len(data)).encode() + b"\0" + data)
    return digest.hexdigest()


def unpack(filename: str, data: bytes, dest: Path) -> Path:
    """Unpack the archive ``data`` (``.zip``, ``.tgz`` or ``.tar.gz``) under ``dest`` and return the
    bundle's root: the archive's single top-level directory when it has exactly one (an npm
    tarball's ``package/``, a zipped project folder), else ``dest``. An entry that would land
    outside ``dest``, a link that points outside it, or an archive that expands past
    :data:`MAX_EXPANDED_BYTES` is refused by name."""
    dest.mkdir(parents=True, exist_ok=True)
    root = dest.resolve()
    name = filename.lower()
    if name.endswith(".zip"):
        try:
            archive = zipfile.ZipFile(io.BytesIO(data))
        except zipfile.BadZipFile as exc:
            raise BundleRefused("archive_unreadable", f"{filename} is not a readable zip") from exc
        with archive:
            total = 0
            for info in archive.infolist():
                total += info.file_size
                _within(total, filename)
                if not (dest / info.filename).resolve().is_relative_to(root):
                    raise BundleRefused(
                        "archive_escapes",
                        f"{filename}: entry {info.filename} lands outside the bundle",
                        entry=info.filename,
                    )
            # Every entry was checked above to land inside dest.
            archive.extractall(dest)  # nosec B202
    elif name.endswith((".tgz", ".tar.gz")):
        try:
            with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
                _within(sum(m.size for m in archive.getmembers()), filename)
                # filter="data" refuses absolute paths, "..", and links or specials that leave dest.
                archive.extractall(dest, filter="data")  # nosec B202 - the "data" filter refuses every unsafe member
        except tarfile.FilterError as exc:
            raise BundleRefused(
                "archive_escapes", f"{filename}: {exc}", entry=str(getattr(exc, "tarinfo", ""))
            ) from exc
        except (tarfile.TarError, EOFError, OSError) as exc:
            raise BundleRefused(
                "archive_unreadable", f"{filename} is not a readable gzip tar: {exc}"
            ) from exc
    else:
        raise BundleRefused(
            "archive_kind",
            f"{filename} is not a .zip, .tgz or .tar.gz archive",
            filename=filename,
        )
    entries = [p for p in dest.iterdir() if not p.name.startswith(("__MACOSX", "."))]
    if len(entries) == 1 and entries[0].is_dir():
        return entries[0]
    return dest


def _within(total: int, filename: str) -> None:
    if total > MAX_EXPANDED_BYTES:
        raise BundleRefused(
            "archive_too_large",
            f"{filename} expands to more than {MAX_EXPANDED_BYTES // (1024 * 1024)} MB",
            filename=filename,
        )
