# Copyright (c) 2026 Kenneth Stott
# Canary: 2d7a6e31-9b04-4c58-8f13-5e0c4b8a7d26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each configuration a test module boots with is its own deployment's first start (REQ-1919).

A configuration file seeds the deployment's model store once, into an empty store; a later boot
never applies it again. The modules of one test session share one PostgreSQL, and each boots the
app with the configuration its tests are written against. So when a module boots the deployment
with a configuration other than the one that seeded the deployment's store, the harness empties
that org's store first: the boot is that deployment's first start, and it seeds the store from
the module's file — exactly as a new deployment does. Boots with the same configuration share the
seeded store, as restarts of one deployment do. A module that tests the seed itself sets
``REAL_BOOT_SEED = True`` and is left alone.
"""

# Requirements: REQ-1919

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

#: The configuration each deployment org's store was seeded from in this session.
_SEEDED_FROM: dict[str, str] = {}


def _empty_the_store(org_id: str) -> None:
    from sqlalchemy import create_engine, text

    url = os.environ["TENANT_DATABASE_URL"]
    engine = create_engine(url)
    try:
        with engine.begin() as conn:
            for schema in (f"org_{org_id}", f"org_{org_id}_mv_cache"):
                conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
    finally:
        engine.dispose()


def each_boot_seeds_its_own_deployment(request: pytest.FixtureRequest) -> Iterator[None]:
    if getattr(request.module, "REAL_BOOT_SEED", False):
        yield
        return
    from _pytest.monkeypatch import MonkeyPatch

    from provisa.api import app as app_module
    from provisa.core.config_loader import load_control_plane

    real = app_module._load_and_build

    async def _boot_on_its_own_store(config_path=None, *, apply=True):
        path = str(Path(config_path or app_module.config_path_str()).resolve())
        if apply and Path(path).exists():
            org_id = load_control_plane(path).resolved_org_id()
            if _SEEDED_FROM.get(org_id) != path:
                _empty_the_store(org_id)
                _SEEDED_FROM[org_id] = path
        await real(config_path, apply=apply)

    mp = MonkeyPatch()
    mp.setattr(app_module, "_load_and_build", _boot_on_its_own_store)
    yield
    mp.undo()
