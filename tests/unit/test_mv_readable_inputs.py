# Copyright (c) 2026 Kenneth Stott
# Canary: 1f7b3d95-6e24-4c8a-a5d0-9b2e4f6a8c13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A materialized view reads only inputs the engine can read whole.

A view over a row-level replicated table, an API table that needs arguments, or a per-request
cache table is refused, with an error naming the view, the input and the reason; a view over a
live table, a whole-table replica, an API endpoint that takes no arguments or another view is
not."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from provisa.api_source.models import ApiColumn, ApiColumnType, ApiEndpoint, ParamType
from provisa.mv import readable_inputs
from provisa.mv.models import MVDefinition, TableIdentity
from provisa.mv.readable_inputs import ViewInputNotReadable
from provisa.mv.registry import MVRegistry


def _endpoint(table: str, path: str, *columns: ApiColumn) -> ApiEndpoint:
    return ApiEndpoint(source_id="crm", path=path, table_name=table, columns=list(columns))


def _col(name: str, param_type: ParamType | None = None) -> ApiColumn:
    return ApiColumn(name=name, type=ApiColumnType.string, param_type=param_type)


@pytest.fixture
def state(monkeypatch):
    row_level = {"clicks": object(), "Clicks": object()}

    async def _row_level(_state):
        return row_level

    async def _sources(_state):
        return [
            SimpleNamespace(id="crm", cache_schema="crm_results"),
            SimpleNamespace(id="sales", cache_schema="api_cache"),
        ]

    monkeypatch.setattr(
        "provisa.federation.query_residency.row_materialized_tables_by_name", _row_level
    )
    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    return SimpleNamespace(
        api_endpoints={
            "contact": _endpoint("contact", "/contacts/{id}", _col("id", ParamType.path)),
            "deal": _endpoint("deal", "/deals/{deal_id}"),
            "countries": _endpoint("countries", "/countries", _col("region", ParamType.query)),
        },
        graphql_remote_sources={
            "gh": {
                "tables": [
                    {"sql_name": "repository", "required_args": ["owner", "name"]},
                    {"sql_name": "licenses", "required_args": []},
                ]
            }
        },
        mv_registry=MVRegistry(),
        # The model a view's inputs resolve against (provisa/mv/view_inputs.py).
        tables=[
            {"id": 1, "source_id": "sales", "schema_name": "sales", "table_name": "orders"},
            {"id": 2, "source_id": "sales", "schema_name": "sales", "table_name": "customers"},
            {"id": 3, "source_id": "web", "schema_name": "web", "table_name": "clicks"},
            {"id": 4, "source_id": "crm", "schema_name": "public", "table_name": "contact"},
        ],
        source_catalogs={},
        contexts={},
        view_sql_map={},  # AppState.view_sql_map: no view reads another here
    )


# The registered tables a join-pattern view joins, by the name it gives each.
_BOUND = {
    "orders": TableIdentity("sales", "sales", "orders"),
    "clicks": TableIdentity("web", "web", "clicks"),
}


def _view(view_id: str, sql: str | None, tables: list[str] | None = None) -> MVDefinition:
    return MVDefinition(
        id=view_id,
        source_tables=tables or [],
        inputs=[] if sql else [_BOUND[t] for t in tables or []],
        target_catalog="store",
        target_schema="org_acme_mv_cache",
        sql=sql,
    )


def _found(sql: str, state) -> list[tuple[str, str]]:
    inputs = readable_inputs.view_inputs(_view("v", sql))
    return [(i.name, i.kind) for i in asyncio.run(readable_inputs.unreadable_inputs(inputs, state))]


def test_a_row_level_replicated_table_is_refused(state):
    assert _found("SELECT region, count(*) FROM web.clicks GROUP BY 1", state) == [
        ("web.clicks", "row-level replicated table")
    ]
    # ... under its semantic alias too
    assert _found('SELECT * FROM "Clicks"', state) == [("Clicks", "row-level replicated table")]


def test_an_api_table_that_needs_arguments_is_refused(state):
    assert _found("SELECT * FROM contact", state) == [("contact", "API table that needs arguments")]
    # a placeholder in the path counts whether or not a column declares it
    assert _found("SELECT * FROM deal", state) == [("deal", "API table that needs arguments")]
    assert _found("SELECT * FROM repository", state) == [
        ("repository", "API table that needs arguments")
    ]


def test_an_api_table_that_takes_no_arguments_is_readable(state):
    assert _found("SELECT * FROM countries", state) == []  # an optional query parameter only
    assert _found("SELECT * FROM licenses", state) == []


def test_a_per_request_cache_table_is_refused_by_its_schema(state):
    for schema in ("api_cache", "org_acme_api_cache", "org_acme_env_dev_gql_cache", "crm_results"):
        assert _found(f"SELECT * FROM {schema}.t_1a2b", state) == [
            (f"{schema}.t_1a2b", "per-request cache table")
        ]


def test_live_tables_replicas_and_other_views_are_readable(state):
    sql = """
        WITH recent AS (SELECT * FROM sales.orders WHERE day > DATE '2026-01-01')
        SELECT c.name, sum(r.total) FROM recent r
        JOIN sales.customers c ON c.id = r.customer_id
        JOIN org_acme_mv_cache.daily_totals d ON d.customer_id = c.id
        GROUP BY 1
    """
    assert _found(sql, state) == []


def test_only_the_unreadable_inputs_are_named(state):
    view = _view(
        "clicks_by_customer",
        "SELECT * FROM sales.customers c JOIN web.clicks k ON k.customer_id = c.id "
        "JOIN contact t ON t.id = c.contact_id",
    )
    with pytest.raises(ViewInputNotReadable) as raised:
        asyncio.run(readable_inputs.require_readable_inputs(view, state))
    message = str(raised.value)
    assert "'clicks_by_customer'" in message
    assert "'web.clicks' is a row-level replicated table" in message
    assert "only the rows requests have fetched" in message
    assert "'contact' is a API table that needs arguments" in message and "(id)" in message
    assert "sales.customers" not in message
    assert [i.name for i in raised.value.inputs] == ["contact", "web.clicks"]


def test_a_join_pattern_view_is_checked_by_the_tables_it_joins(state):
    """A join-pattern view has no SQL of its own: its inputs are the tables it joins."""
    with pytest.raises(ViewInputNotReadable, match="'clicks' is a row-level replicated table"):
        asyncio.run(
            readable_inputs.require_readable_inputs(_view("j", None, ["orders", "clicks"]), state)
        )
    asyncio.run(readable_inputs.require_readable_inputs(_view("j", None, ["orders"]), state))


def test_a_refused_view_is_not_registered(state):
    with pytest.raises(ViewInputNotReadable):
        asyncio.run(readable_inputs.register_view(state, _view("v_clicks", "SELECT * FROM clicks")))
    assert state.mv_registry.get("v_clicks") is None

    kept = _view("v_orders", "SELECT * FROM sales.orders")
    asyncio.run(readable_inputs.register_view(state, kept))
    assert state.mv_registry.get("v_orders") is kept


def test_a_config_load_fails_on_the_first_view_it_cannot_build(state):
    views = [
        _view("v_orders", "SELECT * FROM sales.orders"),
        _view("v_contact", "SELECT * FROM contact"),
        _view("v_clicks", "SELECT * FROM clicks"),
    ]
    with pytest.raises(ViewInputNotReadable) as raised:
        asyncio.run(readable_inputs.require_views_readable(state, views))
    assert raised.value.view == "v_contact"
    assert "'contact' is a API table that needs arguments" in str(raised.value)


class _Engine:
    def __init__(self) -> None:
        self.sqls: list[str] = []

    # The engine seam names its SQL dialect; refresh resolves a view's settings in it.
    dialect = "duckdb"

    def address_replicas(self, sql):
        return sql  # this stand-in's tables are all read where the statement names them

    async def execute_engine(self, sql, *args, **kwargs):
        from provisa.executor.result import QueryResult

        self.sqls.append(sql)
        return QueryResult(rows=[(0,)] if "COUNT(*)" in sql else [], column_names=[])


def test_refreshing_a_view_whose_input_became_row_level_fails_with_the_reason(state, monkeypatch):
    """The view was saved while ``clicks`` was read whole; ``clicks`` is row-level now. Its
    refresh builds nothing — no statement reaches the engine — and the view carries the reason
    as its error (what the admin view list shows as the view's last error)."""
    from provisa.mv.models import MVStatus
    from provisa.mv.refresh import refresh_mv

    monkeypatch.setattr("provisa.api.app.state", state)
    view = _view("v_clicks", "SELECT region, count(*) AS n FROM web.clicks GROUP BY region")
    state.mv_registry.register(view)  # as a saved view comes back into memory
    engine = _Engine()

    asyncio.run(refresh_mv(engine, view, state.mv_registry))

    assert engine.sqls == []
    assert view.status == MVStatus.STALE
    assert view.last_error is not None
    assert "'web.clicks' is a row-level replicated table" in view.last_error
    assert "only the rows requests have fetched" in view.last_error


def test_refreshing_a_view_over_readable_inputs_reaches_the_engine(state, monkeypatch):
    from provisa.mv.refresh import refresh_mv

    monkeypatch.setattr("provisa.api.app.state", state)
    view = _view("v_orders", "SELECT region, count(*) AS n FROM sales.orders GROUP BY region")
    state.mv_registry.register(view)
    engine = _Engine()

    asyncio.run(refresh_mv(engine, view, state.mv_registry))

    assert engine.sqls != []
    assert view.last_error is None


def test_a_config_view_declared_as_a_table_entry_is_checked_like_any_config_view(state):
    """``view_sql`` with ``materialize: true`` on a table entry is a config view too: the load
    takes the view the schema build registered for it and checks it the same way."""
    state.mv_registry.register(_view("view-clicks_t", "SELECT region FROM web.clicks"))
    state.mv_registry.register(_view("view-orders_t", "SELECT region FROM sales.orders"))
    raw_config = {
        "tables": [
            {"table": "orders", "source_id": "sales"},  # an ordinary table
            {"table": "inline_t", "view_sql": "SELECT 1 AS id"},  # an inline view: builds nothing
            {
                "table": "orders_t",
                "view_sql": "SELECT region FROM sales.orders",
                "materialize": True,
            },
            {"table": "clicks_t", "view_sql": "SELECT region FROM web.clicks", "materialize": True},
        ]
    }
    views = readable_inputs.config_table_views(state, raw_config)
    assert [v.id for v in views] == ["view-orders_t", "view-clicks_t"]
    with pytest.raises(ViewInputNotReadable) as raised:
        asyncio.run(readable_inputs.require_views_readable(state, views))
    assert raised.value.view == "view-clicks_t"
    assert "'web.clicks' is a row-level replicated table" in str(raised.value)
