# Copyright (c) 2026 Kenneth Stott
# Canary: 35fbc742-b105-480a-8704-4d28e811914b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship feeds a Qdrant search's query from its source table (REQ-2099, REQ-1983).

The same feed as a code table's: a statement joining the search to the relationship's source
reads the source's mapped columns through the one pipeline as its role, and the search is read
live once per distinct set of values, with the statement's own arguments for the parameters the
relationship does not feed. A search's points hold nothing of the query that found them, so a
relationship that joins on the query's parameter column has each row carry the value its search
was given there, typed as the source column it came from: the join matches each line to the
points found for it."""

# Requirements: REQ-2099, REQ-1983
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import duckdb
import pytest

from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.executor.result import QueryResult
from provisa.federation.replica_address import ReplicaRoutes
from provisa.mongodb.pipeline_table import PipelineRead
from tests.helpers import governed_for_role

pytestmark = pytest.mark.unit

LINES, NEAR = 7, 8
_LINES = [
    (101, 1, "From fairest creatures we desire increase,"),
    (102, 1, "That thereby beauty's rose might never die,"),
    (201, 2, "When forty winters shall besiege thy brow,"),
]


def _param(name: str, data_type: str, required: bool) -> dict:
    return {
        "column_name": f"_nf_{name}",
        "data_type": data_type,
        "native_filter_type": "query_arg",
        "native_filter_required": required,
        "required_by_steward": False,
        "path": None,
    }


def _output(name: str, data_type: str, path: str | None = None) -> dict:
    return {"column_name": name, "data_type": data_type, "native_filter_type": None, "path": path}


def _lines_table() -> dict:
    return {
        "id": LINES,
        "source_id": "pg",
        "schema_name": "sonnets",
        "table_name": "line",
        "columns": [
            _output("id", "integer"),
            _output("sonnet_number", "integer"),
            _output("text", "varchar"),
        ],
    }


def _search_table() -> dict:
    return {
        "id": NEAR,
        "source_id": "vectors",
        "schema_name": "default",
        "table_name": "lines_like",
        "cache_ttl": 0,
        "query_template": (
            '{"collection": "sonnet_lines", "query": "@query", "using": "text", '
            '"filter": {"must_not": [{"key": "sonnet_number", "match": {"value": "@sonnet"}}]}, '
            '"limit": "@k"}'
        ),
        "columns": [
            _output("id", "bigint", "@id"),
            _output("score", "double", "@score"),
            _output("sonnet_number", "integer"),
            _param("query", "array(double)", True),
            _param("sonnet", "integer", False),
            _param("k", "integer", False),
        ],
    }


def _meta(table_id: int, name: str, source_id: str) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=name,
        type_name=name.capitalize(),
        source_id=source_id,
        catalog_name=source_id,
        schema_name="public",
        domain_id="public",
        table_name=name,
    )


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {
        "line": _meta(LINES, "line", "pg"),
        "lines_like": _meta(NEAR, "lines_like", "vectors"),
    }
    ctx.joins = {}
    return ctx


def _state(target_column: str = "_nf_query") -> SimpleNamespace:
    engine = MagicMock()
    engine.land_source_table = AsyncMock()
    return SimpleNamespace(
        hot_manager=None,
        api_endpoints={},
        graphql_remote_sources={},
        source_types={"vectors": "qdrant", "pg": "postgresql"},
        replica_routes=ReplicaRoutes(),
        tables=[_lines_table(), _search_table()],
        relationships=[
            {
                "id": "line-echoes",
                "source_table_id": LINES,
                "source_column": "text",
                "target_table_id": NEAR,
                "target_column": target_column,
                "parameters": {"query": "text", "sonnet": "sonnet_number"},
            }
        ],
        federation_engine=engine,
        source_cache={},
        response_cache_default_ttl=300,
    )


_JOINED = (
    'SELECT "l"."id", "s"."id", "s"."score" FROM "public"."line" AS "l" '
    'JOIN "public"."lines_like" AS "s" ON "s"."_nf_query" = "l"."text"'
)


async def _searched(sql: str, table_ids: list[int], *, nf_args=None, target_column="_nf_query"):
    """Run the materialize stage on ``sql`` as the reader, the feed's read answered from the
    lines above and each search from the points below. Returns the argument sets each search
    was given, what the stage put in for the engine, and the governed feed statements."""
    from provisa.api.data.materialization import _materialize_api_to_engine_cache

    ctx = _ctx()
    governed: list[str] = []
    searched: list = []

    async def govern(statement, role_id, *, session_vars=None):
        governed.append(governed_for_role(statement, ctx, role_id))
        return governed[-1]

    async def execute(plan):
        db = duckdb.connect()
        db.execute("CREATE TABLE line (id INTEGER, sonnet_number INTEGER, text VARCHAR)")
        db.executemany("INSERT INTO line VALUES (?, ?, ?)", _LINES)
        cursor = db.execute(plan.replace('"public".', ""))
        return QueryResult(
            rows=sorted(cursor.fetchall()), column_names=[d[0] for d in cursor.description]
        )

    async def read(state, table, given):
        searched.append(dict(given))
        # The nearest point of another sonnet, its score by the query's length.
        other = 201 if given.get("sonnet") == 1 else 101
        return PipelineRead(
            rows=[{"id": other, "score": len(given["query"]) / 100, "sonnet_number": 2}],
            columns=[("id", "bigint"), ("score", "double"), ("sonnet_number", "integer")],
        )

    with (
        patch("provisa.pgwire._pipeline._govern_and_route", side_effect=govern),
        patch("provisa.pgwire._pipeline._execute_plan", side_effect=execute),
        patch("provisa.qdrant.table.read", side_effect=read),
        patch("provisa.api_source.engine_cache.cache_location"),
    ):
        rewrites, inlined, _ = await _materialize_api_to_engine_cache(
            sql,
            _state(target_column),
            nf_args=nf_args,
            table_ids=table_ids,
            ctx=ctx,
            role_id="reader",
        )
    return searched, {**rewrites, **inlined}, governed


async def test_each_line_is_searched_with_its_own_words_and_sonnet():
    searched, put_in, governed = await _searched(_JOINED, [LINES, NEAR])
    assert sorted(searched, key=lambda s: s["query"]) == sorted(
        [{"query": text, "sonnet": sonnet} for _id, sonnet, text in _LINES],
        key=lambda s: s["query"],
    )
    assert governed == [
        'SELECT DISTINCT "text" AS "query", "sonnet_number" AS "sonnet" FROM "public"."line"'
    ]
    assert set(put_in) == {"lines_like"}


async def test_each_point_carries_the_words_it_was_found_for():
    _, put_in, _ = await _searched(_JOINED, [LINES, NEAR])
    landed = put_in["lines_like"]
    assert landed.column_names == ["id", "score", "sonnet_number", "_nf_query"]
    assert sorted(r["_nf_query"] for r in landed.rows) == sorted(t for _i, _s, t in _LINES)


async def test_the_statements_own_arguments_reach_every_search():
    searched, _, _ = await _searched(_JOINED, [LINES, NEAR], nf_args={"k": 3})
    assert [s["k"] for s in searched] == [3, 3, 3]


async def test_a_search_read_without_the_source_is_refused_before_qdrant_is_asked():
    with pytest.raises(ValueError, match=r"requires the parameter\(s\)"):
        await _searched('SELECT "id" FROM "public"."lines_like"', [NEAR])


async def test_a_relationship_joining_on_a_parameter_it_does_not_feed_is_refused():
    with pytest.raises(ValueError, match="joins on '_nf_k', a parameter it does not feed"):
        await _searched(_JOINED, [LINES, NEAR], target_column="_nf_k")


async def test_a_statement_that_gives_its_own_query_is_not_fed():
    """A theme: one search for a phrase of its own, its points joined back to their lines."""
    searched, put_in, governed = await _searched(
        'SELECT "l"."text", "s"."score" FROM "public"."line" AS "l" '
        'JOIN "public"."lines_like" AS "s" ON "l"."id" = "s"."id"',
        [LINES, NEAR],
        nf_args={"query": "the ravages of time", "k": 20},
    )
    assert searched == [{"query": "the ravages of time", "k": 20}]
    assert governed == []
    assert put_in["lines_like"].column_names == ["id", "score", "sonnet_number"]
