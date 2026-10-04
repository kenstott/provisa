# Copyright (c) 2026 Kenneth Stott
# Canary: cce0e957-4bc8-422a-a7dd-4baed8bfc9e6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-966/REQ-133/REQ-135: a materialized view built on another view (pets -> A -> B).

A view's reference to another view is lowered to the ``__derived__`` sentinel, which is no
engine catalog: B's refresh failed with 'Catalog "__derived__" does not exist'. A refresh acts
for no role, so it reads each view it references by the one rule (``mv.view_read``) with nothing
narrowed: the view's stored rows when it is materialized and fresh, else its own SQL."""

from __future__ import annotations

import pytest

from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.refresh import _build_refresh_sql
from provisa.mv.registry import MVRegistry

A_SQL = 'SELECT "pets"."id", "pets"."price" FROM "pet_store"."main"."pets" AS "pets"'
B_SQL = (
    'SELECT "e2e_mv_a"."id", "e2e_mv_a"."price" FROM "__derived__"."views"."e2e_mv_a" AS "e2e_mv_a"'
)


class _Engine:
    dialect = "duckdb"

    def address_replicas(self, sql: str) -> str:
        return sql


def _mv(name: str, sql: str, status: MVStatus) -> MVDefinition:
    return MVDefinition(
        id=f"view-{name}",
        source_tables=[],
        target_catalog="mat_store",
        target_schema="org_x_mv_cache",
        target_table=f"mv_{name}",
        refresh_interval=300,
        enabled=True,
        sql=sql,
        status=status,
    )


@pytest.fixture
def views(monkeypatch):
    from provisa.api.app import state

    registry = MVRegistry()
    monkeypatch.setattr(state, "mv_registry", registry)
    monkeypatch.setattr(state, "view_sql_map", {"e2e_mv_a": A_SQL, "e2e_mv_b": B_SQL})
    return registry


async def test_a_view_on_a_fresh_materialized_view_is_built_from_its_stored_rows(views):
    views.register(_mv("e2e_mv_a", A_SQL, MVStatus.FRESH))
    b = _mv("e2e_mv_b", B_SQL, MVStatus.STALE)
    views.register(b)
    sql = await _build_refresh_sql(b, _Engine())
    assert "__derived__" not in sql
    assert '"mat_store"."org_x_mv_cache"."mv_e2e_mv_a"' in sql


async def test_a_view_on_a_view_that_is_not_stored_fresh_is_built_from_its_sql(views):
    views.register(_mv("e2e_mv_a", A_SQL, MVStatus.STALE))
    b = _mv("e2e_mv_b", B_SQL, MVStatus.STALE)
    views.register(b)
    sql = await _build_refresh_sql(b, _Engine())
    assert "__derived__" not in sql
    assert '"pet_store"."main"."pets"' in sql
