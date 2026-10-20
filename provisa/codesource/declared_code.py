# Copyright (c) 2026 Kenneth Stott
# Canary: 2dd07026-2d7c-4924-a467-0dae8368cbdb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A configuration's declared code archives: the code its code sources run, shipped beside the
config and kept as the organisation's uploads, so that nothing is fetched to run it (REQ-2101).

A demo whose code source runs its own package ships the package's archive beside its config.yaml::

    code/<name>.tar.gz        (or .tgz, or .zip)

and its config names the source's code by the archive's content hash
(``code: {kind: upload, upload: <hash>}``, the hash ``uploads.put`` answers). When the org's model
is built -- a demo template's from its demo's directory, the deployment org's from its launch
config's directory, the same directories whose declared replica rows land (REQ-2089) -- each
archive there is kept as the org's upload, once by its content hash. A sandbox cloned from the
template is given the template's uploads (``uploads.copy``)."""

# Requirements: REQ-2101
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.core.database import Database

CODE_DIRECTORY = "code"
_ARCHIVES = (".tar.gz", ".tgz", ".zip")


def declared_archives(directory: Path) -> list[Path]:
    """The code archives declared beside the config in ``directory``, in name order."""
    folder = directory / CODE_DIRECTORY
    if not folder.is_dir():
        return []
    return sorted(p for p in folder.iterdir() if p.is_file() and p.name.endswith(_ARCHIVES))


async def adopt_declared_code(admin_db: "Database", org_id: str, directory: Path) -> list[str]:
    """Keep each archive declared in ``directory`` as ``org_id``'s upload and return their content
    hashes. An archive already kept is kept once; one that cannot be used is refused by name, as
    its upload would be."""
    from provisa.codesource import uploads

    return [
        await uploads.put(admin_db, org_id, archive.name, archive.read_bytes(), actor=None)
        for archive in declared_archives(directory)
    ]
