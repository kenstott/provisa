# Copyright (c) 2026 Kenneth Stott
# Canary: 7b1c0f52-3d5e-4c58-9a0e-2f6d8e4a91c3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A domain's stored graphql_alias reaches the schema build's alias map (REQ-471)."""

# Requirements: REQ-471

from __future__ import annotations

import pytest
from sqlalchemy import insert

from provisa.api.app import _load_build_domains
from provisa.compiler.naming import domain_alias_map
from provisa.core.schema_org import domains

pytestmark = pytest.mark.asyncio


async def test_stored_domain_alias_wins_in_the_build_map(control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(domains).values(id="pet-store", graphql_alias="pets", description="")
        )
        await conn.execute_core(insert(domains).values(id="sales_analytics", description=""))
        loaded = await _load_build_domains(conn)
    aliases = domain_alias_map(loaded)
    assert aliases["pet-store"] == "pets"
    assert aliases["sales_analytics"] == "sa"
