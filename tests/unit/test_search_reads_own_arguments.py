# Copyright (c) 2026 Kenneth Stott
# Canary: c6600cc9-6d72-45c8-a82a-6304ec652c89
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""A statement that reads one search table more than once, each read under arguments of its own,
reads each under its own (REQ-1983, REQ-1986).

The sonnets demo's view theme_lines is the union of five theme views, each one search of
``lines_like`` under its own phrase. Expanded into the statement (a theme view not fresh is read
through its definition), only the first read's arguments were taken out: the others stayed in
the WHERE, on the rows the search's read had become, which hold no such column -- Binder Error:
"s" does not have a column named "_nf_query"."""

# Requirements: REQ-1983, REQ-1986

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
import sqlglot
from sqlglot import exp

from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.nf_extractor import bind_parameters, extract_nf_args, read_base
from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_types import TableMeta
from provisa.federation.replica_address import ReplicaRoutes
from tests.role_state import RoleState

_PARAMS = {"_nf_query", "_nf_k"}


def _theme(phrase: str) -> str:
    return (
        "SELECT l.id AS line_id, s.score FROM lines_like AS s JOIN line AS l ON l.id = s.id "
        f"WHERE s._nf_query = '{phrase}' AND s._nf_k = 20"
    )


_TWO_THEMES = (
    f"SELECT t.line_id FROM (SELECT * FROM ({_theme('time')}) AS a "
    f"UNION ALL SELECT * FROM ({_theme('beauty')}) AS b) AS t"
)


def _tables(sql: str) -> list[tuple[str, str]]:
    return [
        (t.name, t.alias_or_name)
        for t in sqlglot.parse_one(sql, read="postgres").find_all(exp.Table)
    ]


def _bound(sql: str):
    return bind_parameters(sql, [], _PARAMS).bound()


def test_each_read_of_a_search_table_is_named_with_its_own_arguments():
    bound = _bound(_TWO_THEMES)
    assert sorted(bound.reads.values(), key=lambda r: r[1]["query"]) == [
        ("lines_like", {"query": "beauty", "k": 20}),
        ("lines_like", {"query": "time", "k": 20}),
    ]
    assert "_nf_" not in bound.sql  # every read's arguments taken out of its own WHERE
    named = [(name, alias) for name, alias in _tables(bound.sql) if name != "line"]
    assert sorted(named) == [("lines_like__nf1", "s"), ("lines_like__nf2", "s")]
    assert {read_base(name) for name, _alias in named} == {"lines_like"}


def test_a_single_read_keeps_its_name_and_gives_the_statement_its_arguments():
    one = _theme("time")
    bound = _bound(one)
    assert bound.reads == {} and bound.nf_args == {"query": "time", "k": 20}
    clean, _params, args = extract_nf_args(one, [], _PARAMS)
    assert args == {"query": "time", "k": 20} and "_nf_" not in clean
    assert clean == bound.sql


def test_an_unaliased_read_keeps_its_name_for_its_columns():
    sql = (
        "SELECT * FROM (SELECT lines_like.id FROM lines_like WHERE lines_like._nf_query = 'a') AS x "
        "UNION ALL SELECT * FROM (SELECT lines_like.id FROM lines_like "
        "WHERE lines_like._nf_query = 'b') AS y"
    )
    bound = _bound(sql)
    assert len(bound.reads) == 2
    assert sorted(_tables(bound.sql)) == [
        ("lines_like__nf1", "lines_like"),
        ("lines_like__nf2", "lines_like"),
    ]


# --- through the pipeline: a SQL read of a view over two theme views -------------------------


def _state() -> RoleState:
    ctx = CompilationContext()
    ctx.tables = {
        name: TableMeta(
            table_id=tid,
            field_name=name,
            type_name=name.capitalize(),
            source_id=source,
            catalog_name=source,
            schema_name="default" if source == "vectors" else "sonnets",
            table_name=name,
            domain_id="sonnets",
        )
        for tid, name, source in ((1, "lines_like", "vectors"), (2, "line", "sonnets"))
    }
    ctx.native_filter_columns = {1: {"_nf_query": "varchar", "_nf_k": "integer"}}
    columns = {1: ["id", "score", "_nf_query", "_nf_k"], 2: ["id", "text"]}
    tables = [
        {
            "id": tid,
            "source_id": source,
            "schema_name": "default" if source == "vectors" else "sonnets",
            "table_name": name,
            "domain_id": "sonnets",
            "columns": [
                {"column_name": c, "data_type": "varchar", "visible_to": ["analyst"]}
                for c in columns[tid]
            ],
        }
        for tid, name, source in ((1, "lines_like", "vectors"), (2, "line", "sonnets"))
    ]
    return RoleState(
        model_stamp=1,
        admin_db=None,
        contexts={"analyst": ctx},
        rls_contexts={},
        roles={"analyst": {"capabilities": ["ignore_relationships"], "domain_access": ["*"]}},
        masking_rules={},
        tables=tables,
        relationships=[],
        source_types={"vectors": "qdrant", "sonnets": "postgresql"},
        source_catalogs={},
        federation_engine=None,
        view_sql_map=None,
        security_high=False,
        metrics={},
        schema_boot_id="test-boot",
        schema_version=1,
        compiled_query_cache=CompiledQueryCache(),
        routing_cache=CompiledQueryCache(),
        replica_routes=ReplicaRoutes(),
    )


@pytest.mark.asyncio
async def test_the_pipeline_hands_each_read_its_own_arguments(monkeypatch):
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline

    monkeypatch.setattr(app_mod, "state", _state(), raising=False)
    captured: dict = {}

    class _Captured(Exception):
        """What the optimization stage was handed: the test ends there."""

    async def _route(exec_sql, governed_sql, gov_ctx, ctx, state, **kwargs):
        captured["exec_sql"] = exec_sql
        captured["read_args"] = kwargs.get("read_args")
        raise _Captured

    with patch.object(_pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_route)):
        with pytest.raises(_Captured):
            await _pipeline._govern_and_route(_TWO_THEMES, "analyst")

    assert "_nf_" not in captured["exec_sql"]
    assert sorted(args["query"] for _table, args in captured["read_args"].values()) == [
        "beauty",
        "time",
    ]
    assert {table for table, _args in captured["read_args"].values()} == {"lines_like"}
