# Copyright (c) 2026 Kenneth Stott
# Canary: 4c8f2a17-9e60-4d3b-b5a2-0f7d1e6c3b98
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table a materialized view reads may not become an input no view may read.

Saving a view over a row-level table is refused (tests/unit/test_mv_readable_inputs.py). The
other order — the view exists, then its table is switched to row-level replication, or its API
endpoint starts needing arguments — is refused where the table or endpoint is stored, naming the
views that read it, instead of leaving the view's next refresh to fail.

A real SQLite control plane through the table repository and the endpoint persist function: the
refusal IS their last write gate."""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
import sqlalchemy as sa

import provisa.api.app as app_mod
from provisa.api_source.models import ApiColumn, ApiColumnType, ApiEndpoint, ParamType
from provisa.api_source.persist import persist_api_endpoint
from provisa.core.database import Database, create_engine_from_url
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import api_endpoints, metadata, registered_tables
from provisa.mv.models import MVDefinition
from provisa.mv.readable_inputs import ViewsReadTable
from provisa.mv.registry import MVRegistry


@asynccontextmanager
async def _conn(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'tenant.db'}")
    with engine.begin() as c:
        metadata.create_all(c)
    try:
        async with Database(engine, name="org").acquire() as conn:
            yield conn
    finally:
        engine.dispose()


@pytest.fixture
def state(monkeypatch):
    """The engine has no connector for the table's source type, so row-level is its reach."""

    async def _sources(_state, _conn=None):
        return [SimpleNamespace(id="graph", type=SimpleNamespace(value="neo4j"))]

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    state = SimpleNamespace(
        mv_registry=MVRegistry(), federation_engine=SimpleNamespace(connectors={})
    )
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    return state


def _clicks(*, row_level: bool, name: str = "clicks") -> Table:
    return Table(
        source_id="graph",
        domain_id="web",
        schema_name="neo4j",
        table_name=name,
        row_materialize=row_level,
        cache_ttl=300,
        columns=[
            Column(name="click_id", data_type="integer", visible_to=[], is_primary_key=True),
            Column(name="region", data_type="varchar", visible_to=[]),
        ],
    )


def _view(name: str, sql: str, *, materialize: bool = True) -> Table:
    return Table(
        source_id="__derived__",
        domain_id="web",
        schema_name="views",
        table_name=name,
        view_sql=sql,
        materialize=materialize,
        columns=[Column(name="region", data_type="varchar", visible_to=[])],
    )


async def _stored_row_level(conn, name: str) -> bool:
    result = await conn.execute_core(
        sa.select(registered_tables.c.row_materialize).where(registered_tables.c.table_name == name)
    )
    return bool(result.scalar_one())


async def test_switching_a_table_a_view_reads_to_row_level_is_refused_naming_the_views(
    tmp_path, state
):
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _clicks(row_level=False), origin="admin")
        await table_repo.upsert(
            conn,
            _view("clicks_by_region", "SELECT region FROM web.clicks GROUP BY region"),
            origin="admin",
        )
        await table_repo.upsert(
            conn, _view("recent_clicks", "SELECT region FROM clicks"), origin="admin"
        )
        await table_repo.upsert(
            conn, _view("orders_by_region", "SELECT region FROM web.orders"), origin="admin"
        )

        with pytest.raises(ViewsReadTable) as raised:
            await table_repo.upsert(conn, _clicks(row_level=True), origin="admin")

        assert raised.value.views == ["view-clicks_by_region", "view-recent_clicks"]
        message = str(raised.value)
        assert "table 'clicks' cannot become a row-level replicated table" in message
        assert "'view-clicks_by_region', 'view-recent_clicks' read it" in message
        assert "only the rows requests have fetched" in message
        assert await _stored_row_level(conn, "clicks") is False  # nothing was stored


async def test_a_view_held_in_memory_from_the_config_refuses_the_switch_too(tmp_path, state):
    state.mv_registry.register(
        MVDefinition(
            id="view-config-clicks",
            source_tables=[],
            target_catalog="store",
            target_schema="org_acme_mv_cache",
            sql="SELECT region FROM web.clicks",
        )
    )
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _clicks(row_level=False), origin="admin")
        with pytest.raises(ViewsReadTable, match="'view-config-clicks' read it"):
            await table_repo.upsert(conn, _clicks(row_level=True), origin="admin")


async def test_the_switch_is_allowed_when_no_materialized_view_reads_the_table(tmp_path, state):
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _clicks(row_level=False), origin="admin")
        # An inline (not materialized) view is expanded into each request: it builds nothing.
        await table_repo.upsert(
            conn,
            _view("inline_clicks", "SELECT region FROM clicks", materialize=False),
            origin="admin",
        )
        await table_repo.upsert(
            conn, _view("orders_by_region", "SELECT region FROM web.orders"), origin="admin"
        )

        await table_repo.upsert(conn, _clicks(row_level=True), origin="admin")
        assert await _stored_row_level(conn, "clicks") is True
        await table_repo.upsert(
            conn, _clicks(row_level=True), origin="admin"
        )  # and it stays storable


async def test_a_table_the_engine_attaches_live_is_not_switched_by_the_flag(tmp_path, state):
    """The flag is ignored where the engine reads the source in place: the view stays readable."""
    state.federation_engine = SimpleNamespace(
        connectors={"neo4j": SimpleNamespace(reads_in_place=True)}
    )
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _clicks(row_level=False), origin="admin")
        await table_repo.upsert(
            conn, _view("clicks_by_region", "SELECT region FROM clicks"), origin="admin"
        )
        await table_repo.upsert(conn, _clicks(row_level=True), origin="admin")
        assert await _stored_row_level(conn, "clicks") is True


def _endpoint(path: str, *columns: ApiColumn) -> ApiEndpoint:
    return ApiEndpoint(source_id="crm", path=path, table_name="contacts", columns=list(columns))


async def test_an_endpoint_a_view_reads_may_not_start_needing_arguments(tmp_path, state):
    async with _conn(tmp_path) as conn:
        await persist_api_endpoint(conn, _endpoint("/contacts"))
        await table_repo.upsert(
            conn, _view("contacts_by_region", "SELECT region FROM contacts"), origin="admin"
        )

        with pytest.raises(ViewsReadTable) as raised:
            await persist_api_endpoint(
                conn,
                _endpoint(
                    "/contacts/{id}",
                    ApiColumn(name="id", type=ApiColumnType.string, param_type=ParamType.path),
                ),
            )
        message = str(raised.value)
        assert "table 'contacts' cannot become a API table that needs arguments" in message
        assert "'view-contacts_by_region' read it" in message and "(id)" in message
        stored = await conn.execute_core(sa.select(api_endpoints.c.path))
        assert [r.path for r in stored.fetchall()] == ["/contacts"]


async def test_an_endpoint_no_view_reads_may_start_needing_arguments(tmp_path, state):
    async with _conn(tmp_path) as conn:
        await persist_api_endpoint(conn, _endpoint("/contacts"))
        await persist_api_endpoint(conn, _endpoint("/contacts/{id}"))
        stored = await conn.execute_core(sa.select(api_endpoints.c.path))
        assert [r.path for r in stored.fetchall()] == ["/contacts/{id}"]
