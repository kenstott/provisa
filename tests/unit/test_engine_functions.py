# Copyright (c) 2026 Kenneth Stott
# Canary: ae52c719-f750-4b83-af32-8da0b654f0fb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Cypher read's map, list and quantifier functions are the engine's (REQ-345): a source whose
own SQL lacks one is answered by the engine, which reads the meta domain's views over the control
plane as it reads any table -- so a hand-written Cypher over meta using a map literal and a list
function answers instead of failing in PostgreSQL ("function map(text[], json[]) does not
exist")."""

# Requirements: REQ-345, REQ-423, REQ-2014

import asyncio

import pytest
from sqlalchemy import insert

from provisa.core import schema_org
from provisa.cypher.parser import parse_cypher
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
from provisa.federation.native_backend import libpq_dsn
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap, NodeMapping
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.engine_functions import uncomputable
from provisa.transpiler.transpile import transpile_to_duckdb

_MAPPED = "MATCH (t:GlossaryTerms) RETURN {name: t.name, kept: NOT t.retired} AS term"
_LISTED = "MATCH (t:GlossaryTerms) RETURN [x IN [t.name, 'x'] WHERE x <> 'x'] AS kept"


def _terms() -> CypherLabelMap:
    terms = NodeMapping(
        label="GlossaryTerms",
        type_name="GlossaryTerms",
        domain_label=None,
        table_label="GlossaryTerms",
        table_id=1,
        source_id="provisa-admin",
        id_column="id",
        pk_columns=[],
        catalog_name="provisa_admin",
        schema_name="org_default",
        table_name="glossary_terms_meta",
        properties={"id": "id", "name": "name", "retired": "retired"},
    )
    return CypherLabelMap(nodes={"GlossaryTerms": terms}, relationships={})


def _sql(cypher: str) -> str:
    lm = _terms()
    sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
    return apply_graph_rewrites(sql_ast, graph_vars, lm).sql(dialect="postgres")


@pytest.mark.parametrize(
    "cypher, function",
    [
        (_MAPPED, "MAP"),
        (_LISTED, "filter"),
        ("MATCH (t:GlossaryTerms) RETURN [t.name, 'x'][0] AS first", "element_at"),
        ("MATCH (t:GlossaryTerms) RETURN any(x IN [t.name] WHERE x = 'a') AS a", "any_match"),
    ],
)
def test_a_cypher_function_postgres_lacks_is_named(cypher, function):
    assert uncomputable(_sql(cypher), "postgres") == function
    # The engines' own dialects compute them.
    assert uncomputable(_sql(cypher), "trino") is None


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT COUNT(*) FILTER (WHERE x > 1) FROM t",  # SQL's FILTER clause, not the function
        "SELECT cardinality(arr), j ->> 'a' FROM t",  # PostgreSQL computes these
        "SELECT mapping FROM maps",  # a name, not a call
    ],
)
def test_what_postgres_computes_is_not_named(sql):
    assert uncomputable(sql, "postgres") is None


@pytest.fixture
def engine(control_plane):
    """The engine over a control plane whose org holds the meta views, as startup seeds them,
    with two terms."""
    from provisa.api._meta_views import _META_TABLE_VIEWS
    from provisa.api.startup_seed import _seed_view

    async def _seed() -> None:
        async with control_plane.org().acquire() as conn:
            for ddl in _META_TABLE_VIEWS.values():
                await _seed_view(conn, ddl)
            await conn.execute_core(
                insert(schema_org.glossary_terms).values(
                    [{"name": "party", "retired": False}, {"name": "old", "retired": True}]
                )
            )

    asyncio.run(_seed())
    rt = DuckDBFederationRuntime()
    rt.attach_control_plane(libpq_dsn(control_plane.org().engine.url))
    yield rt
    rt.close()


def test_the_engine_answers_a_map_and_a_list_function_over_the_meta_views(engine):
    # Each map value is JSON-encoded (to_json), as the IR writes it.
    mapped = engine.run_sync(transpile_to_duckdb(_sql(_MAPPED))).rows
    assert sorted((dict(row[0]) for row in mapped), key=repr) == [
        {"name": '"old"', "kept": "false"},
        {"name": '"party"', "kept": "true"},
    ]
    listed = engine.run_sync(transpile_to_duckdb(_sql(_LISTED))).rows
    assert sorted(listed) == [(["old"],), (["party"],)]


def test_cypher_range_is_the_engine_s_sequence_which_a_postgres_source_does_not_compute():
    sql = "SELECT x FROM UNNEST(SEQUENCE(1, 3)) AS t(x)"
    assert uncomputable(sql, "postgres") == "sequence"
    assert uncomputable(sql, "trino") is None
