# Copyright (c) 2026 Kenneth Stott
# Canary: 2d7a6e31-9b04-4c58-8f13-5e0c4b8a7d26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each configuration a test boots with is its own deployment's first start (REQ-1919).

A configuration file seeds the deployment's model store once, into an empty store; a later boot
never applies it again. The tests of one session share one PostgreSQL, and each module boots the
app (in-process, or as the live-server subprocess) with the configuration its tests are written
against. So when a boot names a configuration other than the one that seeded its org's store, the
harness empties that org's model first (``config_loader.reset_model``: everything but what the
deployment seeds into every store, and the people holding the seeded roles) and marks the store
unseeded: the boot is that deployment's first start, and it seeds the store from its own file —
exactly as a new deployment does. Boots with the same configuration share the seeded store, as
restarts of one deployment do. A module that tests the seed itself sets ``REAL_BOOT_SEED = True``
and is left alone.
"""

# Requirements: REQ-1919

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

#: The configuration each deployment org's store was seeded from in this session.
_SEEDED_FROM: dict[str, str] = {}


async def prepare_first_start(config_path: str) -> None:
    """Make a boot with ``config_path`` its deployment's first start, unless the org's store was
    seeded from that same configuration in this session."""
    from sqlalchemy import delete, text

    from provisa.core import model_change
    from provisa.core.config_loader import load_control_plane, reset_model
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.schema_org import model_seed

    path = str(Path(config_path).resolve())
    org_id = load_control_plane(path).resolved_org_id()
    if _SEEDED_FROM.get(org_id) == path:
        return
    _SEEDED_FROM[org_id] = path
    schema = f"org_{org_id}"
    plane = Database(
        create_engine_from_url(os.environ["TENANT_DATABASE_URL"]),
        name="first-start",
        search_path=schema,
    )
    try:
        async with plane.acquire() as conn:
            found = (
                await conn.execute_core(
                    text(
                        "SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = :s AND table_name = 'model_seed'"
                    ).bindparams(s=schema)
                )
            ).fetchone()
            if found is None:
                return  # no store yet: the boot is its first start already
            async with model_change.committed_by_caller(), conn.transaction():
                await reset_model(conn)
                await conn.execute_core(delete(model_seed))
    finally:
        await plane.close()


def each_boot_seeds_its_own_deployment(request: pytest.FixtureRequest) -> Iterator[None]:
    if getattr(request.module, "REAL_BOOT_SEED", False):
        yield
        return
    from _pytest.monkeypatch import MonkeyPatch

    from provisa.api import app as app_module

    real = app_module._load_and_build

    async def _boot_on_its_own_store(config_path=None, *, apply=True):
        path = config_path or app_module.config_path_str()
        if apply and Path(path).exists():
            await prepare_first_start(path)
        await real(config_path, apply=apply)

    mp = MonkeyPatch()
    mp.setattr(app_module, "_load_and_build", _boot_on_its_own_store)
    yield
    mp.undo()
