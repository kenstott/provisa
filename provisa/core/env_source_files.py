# Copyright (c) 2026 Kenneth Stott
# Canary: 6b2e9c47-3a51-4d88-9f0c-5e7a1b24d3f6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""File-backed sources get their own COPY in an EPHEMERAL environment (REQ-1620).

WHAT THIS CLOSES. REQ-1602's sandbox visitor environment is copied with ``strip_identities=False``:
the connections land already bound, so the visitor gets a working demo instead of an environment it
would first have to bind itself. For every source reached over a network that is the whole of it --
the demo GraphQL server has no mutation type, the pet-store mock holds its rows in module-level
dicts. For a file-backed source it is not: ``sources.path`` names a file on the deployment's disk,
the DuckDB connector ATTACHes it read-write, and an UPDATE issued in a throwaway environment is a
permanent edit to the sample data every later visitor is shown.

THE AXIS IS EXPIRY, NOT ENVIRONMENT. An environment is a first-class place with its own features,
and pointing at whatever data source it likes -- prod's file included -- is one of them. This is not
a rule about environments. It is a rule about the ones that are going to be deleted: an edit whose
target outlives the environment that made it is the only thing being prevented, so the fork is
gated on ``expires_at`` and nothing else.

THE FIX IS A COPY, NOT A PROHIBITION. Writing to a source is a demonstrable feature, and taking it
away from the sandbox demonstrates less than the sandbox exists to demonstrate. So the environment
is given its own file: the row is repointed at a copy under the environment's own directory, the
visitor writes to that, and the directory goes when the environment does.

WHERE THE FILES LIVE. ``$PROVISA_DATA_DIR`` (else ``~/.provisa``) ``/env_files/<org>/<env>``, the
same root and the same derivation :mod:`provisa.core.env_repo` uses for an environment's git repo --
one data dir per deployment, with each environment's belongings under its own name so that removing
the environment is removing a directory.

WHAT IS NOT COPIED. A ``path`` that is not a local filesystem path -- an ``s3://`` or ``https://``
object, a URL -- names a store this deployment does not own and cannot fork. There is nothing to
copy and nothing to clean up, so those rows are left standing exactly as the copy left them.
"""

# Requirements: REQ-1620, REQ-1491, REQ-1602

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from sqlalchemy import select

from provisa.core.env_copy import _scoped
from provisa.core.environments import org_schema
from provisa.core.schema_org import sources as org_sources

if TYPE_CHECKING:
    from provisa.core.database import Database

log = logging.getLogger(__name__)

#: The source types whose ``path`` is a file (or a directory of files) on the deployment's own disk.
#: The same classification :mod:`provisa.events.probes` reads to decide a source is probed by
#: mtime+size rather than scanned, stated here because the reason is the same one: these are the
#: sources whose bytes this deployment holds.
FILE_TYPES: frozenset[str] = frozenset({"csv", "parquet", "sqlite", "files"})


class SourceFileForkError(Exception):
    """A file-backed source could not be forked into the environment's own directory."""


def env_files_root() -> Path:
    """The deployment's directory of per-environment source files."""
    return Path(os.environ.get("PROVISA_DATA_DIR") or (Path.home() / ".provisa")) / "env_files"


def env_files_dir(org_id: str, env: str) -> Path:
    """Where ``env``'s own copies of its file-backed sources live.

    Both segments are validated identifiers before they reach a path: an org id is checked by
    ``_validate_org_id`` and an environment name by ``ENV_NAME_PATTERN``, so neither can carry a
    separator. Re-validating here rather than trusting the caller, because this function's result is
    a directory something else will later delete.
    """
    from provisa.core.db import _validate_org_id
    from provisa.core.environments import EnvironmentNameError, is_env_name

    _validate_org_id(org_id)
    if not is_env_name(env):
        raise EnvironmentNameError(f"{env!r} is not an environment name")
    return env_files_root() / org_id / env


def _local_path(raw: str) -> Path | None:
    """``raw`` as a local filesystem path, or None when it addresses a remote store."""
    scheme = urlparse(raw).scheme
    if scheme and scheme != "file" and len(scheme) > 1:  # a drive letter is not a scheme
        return None
    return Path(urlparse(raw).path if scheme == "file" else raw)


async def fork_file_sources(db: "Database", org_id: str, env: str) -> dict[str, str]:
    """Copy ``env``'s file-backed sources into its own directory and repoint their rows.

    Returns ``{source_id: new_path}`` for the rows that were repointed. Raises
    :class:`SourceFileForkError` when a row names a local path that is not there: the environment
    was about to be handed a writable binding to a file nobody can open, and the caller's rollback
    is a better answer than an environment that half works.
    """
    schema = org_schema(org_id, env)
    scoped = _scoped(org_sources, schema)
    target = env_files_dir(org_id, env)
    repointed: dict[str, str] = {}

    async with db.acquire() as conn, conn.transaction():
        rows = (
            await conn.execute_core(
                select(scoped.c.id, scoped.c.path).where(scoped.c.type.in_(sorted(FILE_TYPES)))
            )
        ).fetchall()
        for row in rows:
            source_id, raw = row._mapping["id"], row._mapping["path"]
            if not raw:
                continue
            origin = _local_path(raw)
            if origin is None:
                continue
            if not origin.exists():
                raise SourceFileForkError(
                    f"source {source_id!r} in {schema} points at {raw!r}, which does not exist; "
                    f"there is nothing to copy into {env!r}"
                )
            target.mkdir(parents=True, exist_ok=True)
            destination = target / f"{source_id}{''.join(origin.suffixes)}"
            if origin.is_dir():
                shutil.copytree(origin, destination, dirs_exist_ok=True)
            else:
                shutil.copy2(origin, destination)
            await conn.execute_core(
                scoped.update().where(scoped.c.id == source_id).values(path=str(destination))
            )
            repointed[source_id] = str(destination)

    if repointed:
        log.info(
            "Org %r environment %r forked %d file-backed source(s) into %s",
            org_id,
            env,
            len(repointed),
            target,
        )
    return repointed


def discard_file_sources(org_id: str, env: str) -> bool:
    """Remove ``env``'s directory of copied source files. True when there was one."""
    directory = env_files_dir(org_id, env)
    if not directory.exists():
        return False
    shutil.rmtree(directory)
    log.info("Org %r environment %r discarded its source files at %s", org_id, env, directory)
    return True
