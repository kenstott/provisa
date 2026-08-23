# Copyright (c) 2026 Kenneth Stott
# Canary: 3e8b1f6a-9c24-47d5-8a1e-6f2c9d4b7e03
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1319: `CALL provisa.metric(name, dimensions)` over Bolt/Cypher runs the grain-closed
metric read through the same governed path as Cypher-compiled SQL, and REQ-1320: role-tagged
tables expose Fact/Dimension labels via db.labels()."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.bolt.session import (
    _maybe_invoke_metric_call,
    _parse_metric_dimensions,
    _split_top_level_args,
)
from provisa.compiler.metric_expand import metric_semantic_sql
from provisa.core.models import Metric

_asyncio = pytest.mark.asyncio(loop_scope="session")


def _state():
    return SimpleNamespace(
        metrics={
            "total_revenue": Metric(
                name="total_revenue",
                expression="SUM(orders.amount)",
                visible_to=["*"],
            ),
            "secret_margin": Metric(
                name="secret_margin",
                expression="SUM(orders.margin)",
                visible_to=["finance"],
            ),
        }
    )


# ---------------------------------------------------------------------------
# metric_semantic_sql — the SQL the procedure builds
# ---------------------------------------------------------------------------


def test_semantic_sql_with_dimensions():
    assert (
        metric_semantic_sql("total_revenue", ["region", "country"])
        == "SELECT region, country, value FROM metrics.total_revenue GROUP BY region, country"
    )


def test_semantic_sql_without_dimensions():
    assert metric_semantic_sql("total_revenue", []) == "SELECT value FROM metrics.total_revenue"


def test_semantic_sql_rejects_injection():
    with pytest.raises(ValueError, match="invalid metric identifier"):
        metric_semantic_sql("total_revenue", ["region; DROP TABLE x"])
    with pytest.raises(ValueError, match="invalid metric identifier"):
        metric_semantic_sql("m; --", ["region"])


# ---------------------------------------------------------------------------
# CALL argument parsing
# ---------------------------------------------------------------------------


def test_split_top_level_args_respects_list_nesting():
    assert _split_top_level_args("'total_revenue', ['region', 'country']") == [
        "'total_revenue'",
        "['region', 'country']",
    ]


def test_parse_metric_dimensions_forms():
    assert _parse_metric_dimensions("['region', 'country']") == ["region", "country"]
    assert _parse_metric_dimensions("'region'") == ["region"]
    assert _parse_metric_dimensions("null") == []
    assert _parse_metric_dimensions("[]") == []


# ---------------------------------------------------------------------------
# _maybe_invoke_metric_call
# ---------------------------------------------------------------------------


@_asyncio
async def test_metric_call_executes_governed_path():
    plan = object()
    result = SimpleNamespace(column_names=["region", "value"], rows=[("east", 10), ("west", 5)])
    with (
        patch(
            "provisa.pgwire._pipeline._govern_and_route_compiled",
            new=AsyncMock(return_value=plan),
        ) as govern,
        patch(
            "provisa.pgwire._pipeline._execute_plan",
            new=AsyncMock(return_value=result),
        ) as execute,
    ):
        out = await _maybe_invoke_metric_call(
            "CALL provisa.metric('total_revenue', ['region'])", "admin", _state()
        )
    assert out == (["region", "value"], [["east", 10], ["west", 5]])
    govern.assert_awaited_once()
    assert govern.await_args is not None
    assert (
        govern.await_args.args[0]
        == "SELECT region, value FROM metrics.total_revenue GROUP BY region"
    )
    assert govern.await_args.args[1] == "admin"
    execute.assert_awaited_once_with(plan)


@_asyncio
async def test_metric_call_no_dimensions():
    result = SimpleNamespace(column_names=["value"], rows=[(42,)])
    with (
        patch(
            "provisa.pgwire._pipeline._govern_and_route_compiled",
            new=AsyncMock(return_value=object()),
        ) as govern,
        patch("provisa.pgwire._pipeline._execute_plan", new=AsyncMock(return_value=result)),
    ):
        out = await _maybe_invoke_metric_call(
            "CALL provisa.metric('total_revenue')", "admin", _state()
        )
    assert out == (["value"], [[42]])
    assert govern.await_args is not None
    assert govern.await_args.args[0] == "SELECT value FROM metrics.total_revenue"


@_asyncio
async def test_metric_call_yield_clause_tolerated():
    result = SimpleNamespace(column_names=["value"], rows=[(1,)])
    with (
        patch(
            "provisa.pgwire._pipeline._govern_and_route_compiled",
            new=AsyncMock(return_value=object()),
        ),
        patch("provisa.pgwire._pipeline._execute_plan", new=AsyncMock(return_value=result)),
    ):
        out = await _maybe_invoke_metric_call(
            "CALL provisa.metric('total_revenue') YIELD value RETURN value", "admin", _state()
        )
    assert out == (["value"], [[1]])


@_asyncio
async def test_unknown_metric_is_hard_error():
    with pytest.raises(ValueError, match="Unknown metric: 'nope'"):
        await _maybe_invoke_metric_call("CALL provisa.metric('nope')", "admin", _state())


@_asyncio
async def test_invisible_metric_reads_as_unknown():
    # visible_to=["finance"] — role "admin" must get the same error as a nonexistent metric.
    with pytest.raises(ValueError, match="Unknown metric: 'secret_margin'"):
        await _maybe_invoke_metric_call("CALL provisa.metric('secret_margin')", "admin", _state())


@_asyncio
async def test_visible_role_passes_visibility_gate():
    result = SimpleNamespace(column_names=["value"], rows=[])
    with (
        patch(
            "provisa.pgwire._pipeline._govern_and_route_compiled",
            new=AsyncMock(return_value=object()),
        ),
        patch("provisa.pgwire._pipeline._execute_plan", new=AsyncMock(return_value=result)),
    ):
        out = await _maybe_invoke_metric_call(
            "CALL provisa.metric('secret_margin')", "finance", _state()
        )
    assert out == (["value"], [])


@_asyncio
async def test_non_metric_call_falls_through():
    assert await _maybe_invoke_metric_call("CALL random_python_set(3)", "admin", _state()) is None
    assert await _maybe_invoke_metric_call("MATCH (n) RETURN n", "admin", _state()) is None


# ---------------------------------------------------------------------------
# REQ-1320: db.labels() exposes Fact/Dimension for role-tagged tables
# ---------------------------------------------------------------------------


def _label_map_with_roles():
    from provisa.cypher.label_map import CypherLabelMap, NodeMapping

    def _nm(type_name: str, table_label: str, table_id: int, role: str | None) -> NodeMapping:
        return NodeMapping(
            label=table_label,
            type_name=type_name,
            domain_label=None,
            table_label=table_label,
            table_id=table_id,
            source_id="pg",
            id_column="id",
            pk_columns=[],
            catalog_name="postgresql",
            schema_name="public",
            table_name=table_label.lower(),
            properties={"id": "id"},
            modeling_role=role,
        )

    return CypherLabelMap(
        nodes={
            "Orders": _nm("Orders", "Orders", 1, "fact"),
            "Regions": _nm("Regions", "Regions", 2, "dimension"),
            "Notes": _nm("Notes", "Notes", 3, None),
        },
        relationships={},
    )


def test_db_labels_includes_fact_and_dimension():
    from provisa.bolt.session import _system_query

    with patch("provisa.bolt.session._bolt_label_map", return_value=_label_map_with_roles()):
        result = _system_query("CALL db.labels()", object(), "admin", True, SimpleNamespace())
    assert result is not None
    cols, rows = result
    assert cols == ["result"]
    labels = rows[0][0]["data"]
    assert "Fact" in labels
    assert "Dimension" in labels
    # role-less table contributes no role label; its own label is still present
    assert "Notes" in labels and "Orders" in labels and "Regions" in labels
