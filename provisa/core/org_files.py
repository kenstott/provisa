# Copyright (c) 2026 Kenneth Stott
# Canary: 3c9e5a71-8b2d-4f04-a6e3-1d7b9c5e2f86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each org's folder: the one place the files Provisa keeps for it go (REQ-1992).

A source's conversion caches and downloads, its connector server's working files, replicas,
spools, large results, the files a source was given by upload (``files/``, REQ-1997) and every
temporary file are written under ``<data dir>/orgs/<org id>`` and nowhere else. The folder is
durable: nothing removes its files on their own; each is kept until what it belongs to goes (an
uploaded file with its source).

A deployment of several nodes shares one mount of the data folder (``org_files.shared_mount``):
every node reads and writes the same org folders. What belongs to one process -- a connector
server's run directory, a replica spool, temporary files -- is kept in the node's own subtree,
``orgs/<org>/nodes/<node id>`` (``org_files.node_id``, the pod or host name), so nodes never write
each other's.

The folder is made ready when the org's runtime is built on a node (:func:`prepare`). An org that
has turned on encryption has its folder mounted encrypted instead
(``provisa.core.org_folder_crypt``). Asked for while it is not ready, the folder is refused by name
(:class:`OrgFolderUnavailable`): nothing is written anywhere else in its place.
"""

# Requirements: REQ-1992
from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path
from collections.abc import Callable
from typing import Any

from provisa.core.org_scope import current_org_segment


class OrgFolderUnavailable(RuntimeError):
    """An org's folder is not ready on this node."""

    def __init__(self, org_id: str) -> None:
        super().__init__(
            f"org {org_id!r}'s file folder is not available on this node; its runtime has not "
            "made it ready, so nothing is written for it"
        )
        self.org_id = org_id


def data_dir() -> Path:
    """This node's data directory (``bootstrap.data_dir``, ``$PROVISA_DATA_DIR``), else its
    documented default ``~/.provisa`` (REQ-1913)."""
    from provisa.core import settings_registry

    stated = settings_registry.value("bootstrap.data_dir")
    return Path(stated) if stated else Path.home() / ".provisa"


def shared_mount() -> bool:
    """Whether the data folder is one mount shared by every node of the deployment
    (``org_files.shared_mount``)."""
    from provisa.core import settings_registry

    return bool(settings_registry.value("org_files.shared_mount"))


def node_id() -> str:
    """This node's name among the nodes sharing the mount (``org_files.node_id``, the pod or host
    name): one path segment."""
    import re

    from provisa.core import settings_registry

    name = str(settings_registry.value("org_files.node_id"))
    # A pod or host name: one path segment, never a path.
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", name):
        raise ValueError(
            f"org_files.node_id must be one path segment, a pod or host name, got: {name!r}"
        )
    return name


def orgs_dir() -> Path:
    return data_dir() / "orgs"


def folder(org_id: str) -> Path:
    """Where ``org_id``'s folder is on this node, ready or not."""
    from provisa.core.db import _validate_org_id

    _validate_org_id(org_id)
    return orgs_dir() / org_id


async def prepare(
    org_id: str, admin_db: Any = None, before_change: Callable[[str], None] | None = None
) -> Path:
    """Make ``org_id``'s folder ready on this node: plain, or mounted encrypted under the org's
    key when the org has turned that on (``org_folder_crypt``), its files encrypted or decrypted
    when the setting changed. Asked at every build of the org's runtime. ``admin_db`` holds the
    org's setting; a node with no admin plane (a desktop, a test) has plain folders."""
    path = folder(org_id)
    if admin_db is None:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        return path
    from provisa.core import org_folder_crypt as crypt

    settings = await crypt.read_settings(admin_db, org_id)
    if before_change is not None and crypt.changes(path, settings):
        # What holds the folder's files open (the org's connector servers) is stopped before its
        # files are encrypted or decrypted.
        before_change(org_id)
    key_of = crypt.vault_key_reader(admin_db, org_id, asyncio.get_running_loop())
    await asyncio.to_thread(crypt.apply, org_id, path, settings, key_of)
    if settings.encrypted:
        _mounted[org_id] = path
    else:
        _mounted.pop(org_id, None)
    return path


#: The org folders this process uses through an encrypted mount, released when it shuts down.
_mounted: dict[str, Path] = {}


async def release_all() -> None:
    """This process stops using every encrypted org folder it mounted (its server shutting down:
    SIGTERM or a stop): the last process of the node to go releases the node's mount, so a node
    that shuts down gracefully leaves no record of one (``org_folder_crypt.release``)."""
    from provisa.core import org_folder_crypt as crypt

    for org_id, path in list(_mounted.items()):
        await asyncio.to_thread(crypt.release, org_id, path)
        _mounted.pop(org_id, None)


def org_root(org_id: str | None = None) -> Path:
    """The folder of ``org_id`` (the bound org when None), or :class:`OrgFolderUnavailable`."""
    org = org_id if org_id is not None else current_org_segment()
    path = folder(org)
    if not path.is_dir():
        raise OrgFolderUnavailable(org)
    return path


def org_path(*parts: str, org_id: str | None = None) -> Path:
    """A directory under the org's folder, made if it is not there."""
    path = org_root(org_id).joinpath(*parts)
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    return path


def node_path(*parts: str, org_id: str | None = None) -> Path:
    """A directory under this node's own subtree of the org's folder, made if it is not there:
    what one process keeps, never written by another node sharing the mount."""
    return org_path("nodes", node_id(), *parts, org_id=org_id)


def org_tmp(org_id: str | None = None) -> Path:
    """The org's temporary directory on this node: temporary files and directories are made in
    it, never in the node's own temporary directory."""
    return node_path("tmp", org_id=org_id)


def mkdtemp(prefix: str, org_id: str | None = None) -> str:
    """A new temporary directory in the org's temporary directory."""
    return tempfile.mkdtemp(prefix=prefix, dir=org_tmp(org_id))


# --- paths written with the org's tokens (REQ-1997) ------------------------------------------
#
# The maintainer's ruling: "all uploaded file paths must be relative to the org path (in saas in
# particular we don't want to reveal the physical location of the file)" -- "have some org-level
# {} statements that resolve to org locations". A file source's path may begin with one of these
# tokens; it is kept, shown and named in messages as written, and resolved where the file is read.

#: A token and the place under the org's folder it stands for.
PATH_TOKENS: dict[str, tuple[str, ...]] = {"{files}": ("files",), "{org}": ()}


class OrgPathRefused(PermissionError):
    """A path written with an org token that leaves the org's folder."""


def _token_of(path: str) -> tuple[str, str] | None:
    for token in PATH_TOKENS:
        if path == token or path.startswith(token + "/"):
            return token, path[len(token) :].lstrip("/")
    return None


def is_org_path(path: str | None) -> bool:
    """Whether ``path`` is written with one of the org's tokens."""
    return path is not None and _token_of(path) is not None


def resolve_path(path: str, org_id: str | None = None) -> str:
    """``path`` as the file it names: a path written with an org token is that place under the
    org's folder (``{files}/a.csv`` -> ``<org folder>/files/a.csv``), refused by name when it
    leaves the folder through ``..`` or a symlink; any other path is returned as it is, once a
    held org's is shown not to be a place on the server's own disk outside its folder
    (``source_uploads.require_own_disk``): this is where a file source's path is opened."""
    found = _token_of(path)
    if found is None:
        from provisa.core.source_uploads import require_own_disk

        require_own_disk(path, org_id)
        return path
    token, rest = found
    root = org_root(org_id)
    target = (
        root.joinpath(*PATH_TOKENS[token], rest) if rest else root.joinpath(*PATH_TOKENS[token])
    )
    real, real_root = os.path.realpath(target), os.path.realpath(root)
    if real != real_root and not real.startswith(real_root + os.sep):
        raise OrgPathRefused(f"{path} leaves the organization's folder")
    return str(target)


def token_path(path: str, org_id: str | None = None) -> str:
    """``path`` as the org's tokens write it, when it is under the org's folder (the files area
    as ``{files}/...``, anywhere else in the folder as ``{org}/...``); any other path as it is.
    What a form, an answer or a message shows, so no physical location is revealed."""
    root = os.path.realpath(org_root(org_id))
    real = os.path.realpath(path)
    for token, parts in sorted(PATH_TOKENS.items(), key=lambda kv: -len(kv[1])):
        base = os.path.join(root, *parts) if parts else root
        if real == base:
            return token
        if real.startswith(base + os.sep):
            return token + "/" + os.path.relpath(real, base).replace(os.sep, "/")
    return path
