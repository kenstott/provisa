# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a1c6e-9b42-4d7a-8f01-2c5e7d9a4b13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the governed-metric projections (REQ-1319).

Covers the MCP tool surface (list_metrics visible_to filtering, query_metric
semantic-SQL construction + unknown-metric error + governed-pipeline routing)
and the pgwire catalog projection (reserved `metrics` namespace, one relation
per visible metric with a single `value` column, pg_description text).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from provisa.api.mcp import tools
from provisa.executor.result import QueryResult

# --- fixtures ---------------------------------------------------------------


def _metric(name: str, visible_to: list[str], **kw) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        expression=kw.get("expression", "SUM(orders.amount)"),
        datatype=kw.get("datatype"),
        description=kw.get("description"),
        ai_context=kw.get("ai_context"),
        visible_to=visible_to,
        from_fact=kw.get("from_fact"),
    )


def _make_state():
    """Fake AppState: two role contexts + config metrics with mixed visibility."""
    config = SimpleNamespace(
        metrics=[
            _metric(
                "net_revenue",
                ["*"],
                datatype="decimal",
                description="Net revenue",
                ai_context="Revenue after refunds.",
            ),
            _metric("headcount", ["hr"], from_fact="employees"),
        ]
    )
    return SimpleNamespace(
        contexts={"analyst": SimpleNamespace(tables={}), "hr": SimpleNamespace(tables={})},
        config=config,
    )


@pytest.fixture
def state():
    return _make_state()


# --- list_metrics -----------------------------------------------------------


async def test_list_metrics_star_visible_to_all_roles(state):
    result = tools.list_metrics(state, "analyst")
    assert result == [
        {
            "name": "net_revenue",
            "description": "Net revenue",
            "ai_context": "Revenue after refunds.",
            "datatype": "decimal",
            "from_fact": None,
        }
    ]


async def test_list_metrics_role_scoped_visibility(state):
    names = [m["name"] for m in tools.list_metrics(state, "hr")]
    assert names == ["net_revenue", "headcount"]


async def test_list_metrics_missing_role_raises(state):
    with pytest.raises(ValueError):
        tools.list_metrics(state, "")


async def test_list_metrics_unknown_role_raises(state):
    with pytest.raises(PermissionError):
        tools.list_metrics(state, "intruder")


# --- _metric_sql ------------------------------------------------------------


def test_metric_sql_dimensions_and_filters():
    sql = tools._metric_sql("net_revenue", ["region", "year"], "year >= 2024")
    assert sql == (
        "SELECT region, year, value FROM metrics.net_revenue "
        "WHERE year >= 2024 GROUP BY region, year"
    )


def test_metric_sql_no_dimensions_single_row():
    assert tools._metric_sql("net_revenue", [], None) == "SELECT value FROM metrics.net_revenue"


def test_metric_sql_filters_without_dimensions():
    sql = tools._metric_sql("net_revenue", [], "region = 'EU'")
    assert sql == "SELECT value FROM metrics.net_revenue WHERE region = 'EU'"


# --- query_metric -----------------------------------------------------------


async def test_query_metric_unknown_metric_raises(state):
    with pytest.raises(ValueError, match="ghost"):
        await tools.query_metric(state, "analyst", "ghost", [])


async def test_query_metric_invisible_metric_raises(state):
    # headcount is visible only to hr — for analyst it is an unknown metric.
    with pytest.raises(ValueError, match="headcount"):
        await tools.query_metric(state, "analyst", "headcount", [])


async def test_query_metric_routes_through_governed_pipeline(state, monkeypatch):
    import provisa.pgwire._pipeline as pipeline

    execute = AsyncMock(
        return_value=QueryResult(
            rows=[("EU", 10.5), ("US", 20.0)], column_names=["region", "value"]
        )
    )
    monkeypatch.setattr(pipeline, "execute_sql_batch", execute)

    result = await tools.query_metric(
        state, "analyst", "net_revenue", ["region"], filters="year >= 2024"
    )

    execute.assert_awaited_once_with(
        "SELECT region, value FROM metrics.net_revenue WHERE year >= 2024 GROUP BY region",
        "analyst",
        state,
    )
    assert result == [{"region": "EU", "value": 10.5}, {"region": "US", "value": 20.0}]


async def test_query_metric_governance_error_propagates(state, monkeypatch):
    import provisa.pgwire._pipeline as pipeline

    execute = AsyncMock(side_effect=PermissionError("denied"))
    monkeypatch.setattr(pipeline, "execute_sql_batch", execute)
    with pytest.raises(PermissionError):
        await tools.query_metric(state, "analyst", "net_revenue", [])


# --- pgwire catalog projection ----------------------------------------------


def _catalog_state(metrics: list) -> MagicMock:
    """Fake pgwire state mirroring tests/unit/pgwire/test_catalog.py, plus config metrics."""
    tm = SimpleNamespace(
        table_id=1,
        field_name="orders",
        catalog_name="provisa",
        schema_name="public",
        table_name="orders",
        domain_id="",
        source_id="",
        type_name="",
        source_type="",
        original_table_name="",
        display_name="",
        column_presets={},
    )
    ctx = MagicMock()
    ctx.tables = {"orders": tm}
    state = MagicMock()
    state.contexts = {"testrole": ctx}
    state.schema_build_cache = {
        "column_types": {
            1: [SimpleNamespace(column_name="id", data_type="integer", is_nullable=False)]
        },
        "tables": [],
        "domains": [],
    }
    state.engine_conn = None
    state.config = SimpleNamespace(metrics=metrics)
    return state


def test_catalog_metrics_namespace_relation_and_description():
    from provisa.pgwire.catalog_populate import _build_catalog_db

    metrics = [
        _metric(
            "net_revenue",
            ["*"],
            datatype="decimal",
            ai_context="Revenue after refunds.",
        )
    ]
    db = _build_catalog_db("testrole", _catalog_state(metrics))

    ns = dict(db.execute("SELECT nspname, oid FROM _pg_namespace").fetchall())
    assert "metrics" in ns

    rel = db.execute(
        "SELECT oid, relnamespace FROM _pg_class WHERE relname = 'net_revenue'"
    ).fetchall()
    assert rel == [(60000, ns["metrics"])]

    cols = db.execute("SELECT attname FROM _pg_attribute WHERE attrelid = 60000").fetchall()
    assert cols == [("value",)]

    # description falls back to ai_context when no description is set (REQ-1319).
    desc = db.execute(
        "SELECT description FROM _pg_description WHERE objoid = 60000 AND objsubid = 0"
    ).fetchall()
    assert desc == [("Revenue after refunds.",)]


def test_catalog_metrics_visible_to_filters_role():
    from provisa.pgwire.catalog_populate import _build_catalog_db

    metrics = [
        _metric("net_revenue", ["*"]),
        _metric("headcount", ["hr"]),
    ]
    db = _build_catalog_db("testrole", _catalog_state(metrics))
    names = [r[0] for r in db.execute("SELECT relname FROM _pg_class").fetchall()]
    assert "net_revenue" in names
    assert "headcount" not in names


def test_metric_flight_info_shape():
    """REQ-1319: a metric flight descriptor carries the grain shape (dims + value)."""
    from provisa.api.flight.catalog import metric_to_flight_info

    info = metric_to_flight_info("net_revenue", ["region", "month"], description="Net revenue")
    names = [f.name for f in info.schema]
    assert names == ["region", "month", "value"]
    assert info.schema.metadata[b"metric"] == b"net_revenue"
    assert info.schema.metadata[b"description"] == b"Net revenue"
    assert [p.decode() for p in info.descriptor.path] == [
        "metrics",
        "net_revenue",
        "region",
        "month",
    ]
