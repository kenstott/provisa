# Copyright (c) 2026 Kenneth Stott
# Canary: 9879f83f-09c6-4c4e-948f-82486402940b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-2014: a test's control plane is a PostgreSQL database of its own, laid out as a deployment
lays out its control plane (tests/pg_control_plane.py)."""

from __future__ import annotations

import asyncio

from sqlalchemy import select

from provisa.core.schema_admin import orgs
from provisa.core.schema_org import domains
from tests.pg_control_plane import clone


async def _org_rows(db) -> list[str]:
    async with db.acquire() as conn:
        result = await conn.execute_core(select(orgs.c.id))
    return [r.id for r in result]


async def _domains(db) -> list[str]:
    async with db.acquire() as conn:
        result = await conn.execute_core(select(domains.c.id))
    return sorted(r.id for r in result)


def test_a_control_plane_is_postgresql_laid_out_as_a_deployment_lays_it_out(control_plane):
    assert control_plane.url.startswith("postgresql+psycopg://")
    assert control_plane.platform.dialect == "postgresql"
    # the platform registry, with the default org it seeds
    assert asyncio.run(_org_rows(control_plane.platform)) == ["default"]
    # org default's schema, with the domains schema.sql seeds
    assert asyncio.run(_domains(control_plane.org())) != []


def test_each_control_plane_is_its_own(embedded_postgres, control_plane_template, control_plane):
    other = clone(embedded_postgres, control_plane_template)
    try:
        asyncio.run(_add_domain(control_plane.org(), "only_here"))
        assert "only_here" in asyncio.run(_domains(control_plane.org()))
        assert "only_here" not in asyncio.run(_domains(other.org()))
    finally:
        other.close()


async def _add_domain(db, domain_id: str) -> None:
    async with db.acquire() as conn:
        await conn.execute_core(domains.insert().values(id=domain_id, description=""))


def test_the_template_is_built_from_inside_a_running_event_loop(embedded_postgres):
    """An async fixture that starts a server with a control plane of its own builds the template
    on its loop (test_dq_scan_history_e2e[duckdb]): the build runs on a thread of its own."""
    from tests.pg_control_plane import TEMPLATE, build_template

    async def _inside_a_loop() -> str:
        return build_template(embedded_postgres)

    assert asyncio.run(_inside_a_loop()) == TEMPLATE
