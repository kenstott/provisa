# Copyright (c) 2026 Kenneth Stott
# Canary: 37248ebc-943f-438f-9f30-98d7eed4f869
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A configuration's declared org files (REQ-2101, amended 2026-10-21): files a demo (or a launch
config) ships for its organisations' own folder, so that a source reads them in place by an org
path (``{files}/<name>``) and nothing is fetched from the network -- a sandbox may not reach an
address its demo does not declare (provisa.security.sandbox_egress).

A demo ships them beside its config.yaml::

    org_files/<name>          (a file, or files under sub-folders)

When an org's model is built -- a demo template's from its demo's directory, a sandbox's from the
demo version it was opened from, the deployment org's from its launch config's directory -- each
is copied to ``<org folder>/files/<name>``, once: a file already there with the same bytes is left
as it is."""

# Requirements: REQ-2101
from __future__ import annotations

from pathlib import Path

ORG_FILES_DIRECTORY = "org_files"


def declared_org_files(directory: Path) -> list[Path]:
    """The org files declared beside the config in ``directory``, in path order."""
    folder = directory / ORG_FILES_DIRECTORY
    if not folder.is_dir():
        return []
    return sorted(p for p in folder.rglob("*") if p.is_file())


def adopt_declared_org_files(org_id: str, directory: Path) -> list[str]:
    """Copy each org file ``directory`` declares into ``org_id``'s folder under files/, unless the
    same bytes are there already; the paths written, relative to files/."""
    from provisa.core import org_files

    source_root = directory / ORG_FILES_DIRECTORY
    target_root = org_files.folder(org_id) / "files"
    written = []
    for path in declared_org_files(directory):
        rel = path.relative_to(source_root)
        target = target_root / rel
        data = path.read_bytes()
        if target.is_file() and target.read_bytes() == data:
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        staged = target.with_name(f".{target.name}.writing")
        staged.write_bytes(data)
        staged.replace(target)
        written.append(rel.as_posix())
    return written
