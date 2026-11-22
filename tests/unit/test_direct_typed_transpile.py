# Copyright (c) 2026 Kenneth Stott
# Canary: 5e1a7c93-2d4b-4f68-a9c0-3b8e6d1f7a42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement sent straight to a Db2 or SAP HANA source (the DIRECT route) is transpiled with
that source's registered column types (REQ-2236), on both pipeline stages that lower one: raw SQL
(``_govern_and_route``) and a compiled statement (``_govern_and_route_compiled``). Without them,
HANA's integer division and either dialect's integer cast of a column are refused."""

# Requirements: REQ-2236

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from provisa.audit.context import audit_identity_scope
from provisa.cache.store import NoopCacheStore
from provisa.compiler.directives import NO_CACHE_HINT
from provisa.compiler.introspect import ColumnMetadata
from provisa.federation.typed_transpile import direct_column_types, source_column_types
from provisa.pgwire import _pipeline, governed_plan
from provisa.transpiler.router import Route, RouteDecision
from tests.unit.test_governed_sql_engine_internals import _state as gov_state

_REGISTERED = {"id": "integer", "qty": "integer", "s": "smallint", "price": "numeric(10,2)"}
_SQL = 'SELECT "o"."qty" / "o"."s", CAST("o"."id" AS BIGINT) FROM "sales"."orders" AS "o"'


def _direct(dialect: str):
    async def _route(exec_sql, governed_sql, gov_ctx, ctx, state, **kwargs):
        decision = RouteDecision(route=Route.DIRECT, source_id="pg", dialect=dialect, reason="t")
        return exec_sql, decision, False, {"pg"}, ()

    return _route


def _registered_state(source_type: str) -> Any:
    state = gov_state(["*"])
    state.tables[0]["columns"] = [
        {"column_name": name, "data_type": kind, "visible_to": ["analyst"]}
        for name, kind in _REGISTERED.items()
    ]
    state.schema_build_cache = {
        "column_types": {1: [ColumnMetadata(n, k, True) for n, k in _REGISTERED.items()]}
    }
    state.kafka_table_physical = {}
    state.source_types = {"pg": source_type}
    state.source_dialects = {"pg": source_type}
    state.source_catalogs = {"pg": "pg"}
    state.source_pools = SimpleNamespace(
        source_ids=["pg"], has=lambda sid: sid == "pg", vector_capability=lambda sid: None
    )
    state.response_cache_store = NoopCacheStore()
    state.settings_overrides = {}
    return state


@pytest.fixture
def direct(monkeypatch):
    import provisa.api.app as app_mod

    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    monkeypatch.setattr("provisa.audit.pipeline.write_audit", AsyncMock(return_value=None))

    def _bind(dialect: str) -> Any:
        state = _registered_state(dialect)
        monkeypatch.setattr(app_mod, "state", state, raising=False)
        return state

    with audit_identity_scope("u-1", "test"):
        yield _bind


# The governed statement reads the table through its role's column projection, capped at the
# role's row ceiling; each dialect writes the cap its own way.
_GOVERNED_FROM = (
    'FROM (SELECT "o"."id" AS "id", "o"."qty" AS "qty", "o"."s" AS "s" '
    'FROM "sales"."orders" AS "o") AS "o"'
)
_EXPECTED = {
    "saphana": (
        'SELECT CAST(ROUND("o"."qty" / "o"."s", 0, ROUND_DOWN) AS INTEGER), '
        f'CAST("o"."id" AS BIGINT) {_GOVERNED_FROM} LIMIT 100'
    ),
    "db2": (
        'SELECT "o"."qty" / "o"."s", CAST("o"."id" AS BIGINT) '
        f"{_GOVERNED_FROM} FETCH FIRST 100 ROWS ONLY"
    ),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("dialect", ["saphana", "db2"])
async def test_a_raw_sql_direct_statement_is_transpiled_with_the_registered_types(
    direct, dialect: str
) -> None:
    direct(dialect)
    with patch.object(
        _pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_direct(dialect))
    ):
        plan = await _pipeline._govern_and_route(_SQL, "analyst")
    assert plan.route == Route.DIRECT
    assert plan.sql == _EXPECTED[dialect]


@pytest.mark.asyncio
@pytest.mark.parametrize("dialect", ["saphana", "db2"])
async def test_a_compiled_direct_statement_is_transpiled_with_the_registered_types(
    direct, dialect: str
) -> None:
    state = direct(dialect)
    with patch.object(
        _pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_direct(dialect))
    ):
        plan = await _pipeline._govern_and_route_compiled(
            _SQL, "analyst", state=state, cache_hint=NO_CACHE_HINT, sdl_joins=True
        )
    assert plan.route == Route.DIRECT
    assert plan.sql == _EXPECTED[dialect]


# -- the source's registered types, by the name a DIRECT statement gives each table -----------


def test_source_types_are_keyed_by_schema_and_table_with_no_catalog() -> None:
    nested = source_column_types(_registered_state("saphana"), "pg")
    assert list(nested) == [""]
    assert {c: t.sql(dialect="postgres") for c, t in nested[""]["sales"]["orders"].items()} == {
        "id": "INT",
        "qty": "INT",
        "s": "SMALLINT",
        "price": "DECIMAL(10, 2)",
    }


def test_another_sources_tables_are_not_typed() -> None:
    assert source_column_types(_registered_state("saphana"), "other") == {}


def test_a_dialect_that_reads_no_types_is_handed_none() -> None:
    assert direct_column_types(_registered_state("postgresql"), "pg", "postgres") is None
