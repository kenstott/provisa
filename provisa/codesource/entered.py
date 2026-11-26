# Copyright (c) 2026 Kenneth Stott
# Canary: b9dfa4c9-3bac-46c1-b5a1-3e21c4a41bb5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Code entered here (REQ-1993, amended 2026-11-25): a code source whose code is written in the
source form's editor and kept in the source's mapping, so it moves with the model.

    code    {"kind": "entered", "source": <the code's text>, "dependencies": [<one per line>]}

Its language lays the text and the dependency list out as the files of a bundle of its own
(:meth:`provisa.codesource.languages.CodeLanguage.entered_files`); the bundle is archived here,
always to the same bytes for the same files, and goes on as an uploaded archive does
(:class:`provisa.codesource.install.UploadCode`): installed, hashed, read and run on the upload's
path, its content hash recorded when the source is saved."""

# Requirements: REQ-1993, REQ-1984, REQ-2200, REQ-2227
from __future__ import annotations

import io
import zipfile
from typing import TYPE_CHECKING

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.install import UploadCode

if TYPE_CHECKING:
    from provisa.codesource.languages import CodeLanguage

ENTERED = "entered"
#: The name the entered bundle is archived under: a zip, as unpack reads it.
ARCHIVE = "entered.zip"
#: Every entry's time in the archive, so the same files always archive to the same bytes.
_EPOCH = (1980, 1, 1, 0, 0, 0)


def dependencies_of(code: dict) -> list[str]:
    """The entered code's dependency list: each a line of the language's own form, none blank.
    A mapping with no list has none; anything else is refused by name."""
    given = code.get("dependencies", [])
    if not isinstance(given, list) or not all(isinstance(d, str) and d.strip() for d in given):
        raise BundleRefused(
            "entered_dependencies",
            "the entered code's dependencies are a list of non-blank lines",
        )
    return [d.strip() for d in given]


def name_at_version(line: str) -> tuple[str, str]:
    """A ``name@version`` dependency line (npm's and cargo's form; a scoped npm name keeps its
    leading ``@``), split; one without both is refused by name."""
    at = line.rfind("@")
    name, version = (line[:at].strip(), line[at + 1 :].strip()) if at > 0 else ("", "")
    if not name or not version:
        raise BundleRefused(
            "entered_dependency",
            f"{line!r}: a dependency is written name@version",
            dependency=line,
        )
    return name, version


def archive(files: dict[str, str]) -> bytes:
    """The files as a zip, the same bytes for the same files."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as out:
        for name in sorted(files):
            info = zipfile.ZipInfo(name, date_time=_EPOCH)
            info.external_attr = 0o644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            out.writestr(info, files[name])
    return buf.getvalue()


def code_of(language: "CodeLanguage", code: dict) -> UploadCode:
    """The entered code as the archive its language lays it out in, for the upload's path."""
    files = language.entered_files(code["source"], dependencies_of(code))
    return UploadCode(ARCHIVE, archive(files))
