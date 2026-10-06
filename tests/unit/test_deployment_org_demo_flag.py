# Copyright (c) 2026 Kenneth Stott
# Canary: 4e9b2c71-6d18-4a3f-b5e0-7c2a9f1d8e36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The deployment's own org is a demo organisation only when the deployment is a demo
(REQ-1919, DEMO ORGANISATIONS ARE THEIR CONFIG).

A demo organisation is rebuilt from its configuration at every build; every other org's store is
seeded once and owns its model. The registry's ``seeded_demo`` flag on the bootstrap org is what
the runtime router reads, so it follows the deployment's mode at every start. Runs on a SQLite
platform plane.
"""

# Requirements: REQ-1919, REQ-1296

from __future__ import annotations

import pytest
from sqlalchemy import select

from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import init_registry_schema, orgs


async def _flag(plane: Database) -> bool:
    async with plane.acquire() as conn:
        row = (await conn.execute_core(select(orgs.c.seeded_demo).where(orgs.c.id == "acme"))).one()
    return bool(row.seeded_demo)


@pytest.mark.parametrize(("demo", "expected"), [("", False), ("true", True)])
async def test_the_bootstrap_org_is_a_demo_only_in_a_demo_deployment(monkeypatch, demo, expected):
    monkeypatch.setenv("PROVISA_DEMO", demo)
    plane = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="registry")
    await init_registry_schema(plane, "acme")
    assert await _flag(plane) is expected


async def test_the_flag_follows_the_deployment_at_every_start(monkeypatch):
    plane = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="registry")
    monkeypatch.setenv("PROVISA_DEMO", "true")
    await init_registry_schema(plane, "acme")
    assert await _flag(plane) is True
    monkeypatch.setenv("PROVISA_DEMO", "")
    await init_registry_schema(plane, "acme")
    assert await _flag(plane) is False
