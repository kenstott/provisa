# Copyright (c) 2026 Kenneth Stott
# Canary: 506cfeb5-4452-45e4-b7a1-5e68edb59e26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``cosine_similarity`` through the one pipeline (REQ-423, REQ-426, REQ-429, REQ-430).

The call is governed with the whole statement (stage2), its query embedded or bound to a vector
of the column's model, and lowered to the terminal that runs it. DuckDB and plain PostgreSQL run
here for real; the other dialects' forms are checked as SQL.
"""

# Requirements: REQ-423, REQ-426, REQ-429, REQ-430, REQ-652

from __future__ import annotations

from typing import LiteralString, cast

import duckdb
import pytest
import sqlglot

from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.transpiler.transpile import transpile
from provisa.vector import similarity
from provisa.vector.registry import VectorModel, VectorModelRegistry
from provisa.vector.similarity import SimilarityNotPermitted, SimilarityRefused

DOCS, NOTES = 1, 2
_MODEL = "mini"
_REGISTRY = VectorModelRegistry([VectorModel(_MODEL, "openai", 3), VectorModel("big", "openai", 4)])


def _gov(*, visible: frozenset[str] | None = None) -> GovernanceContext:
    gov = GovernanceContext()
    gov.role_id = "analyst"
    gov.table_map = {"docs": DOCS, "notes": NOTES}
    gov.all_columns = {
        DOCS: [("id", "integer"), ("vec", "array(float)"), ("title", "varchar")],
        NOTES: [("id", "integer"), ("vec", "array(float)"), ("other", "array(float)")],
    }
    gov.visible_columns = {DOCS: visible, NOTES: None}
    gov.embedding_columns = {DOCS: {"vec": _MODEL}, NOTES: {"vec": _MODEL, "other": "big"}}
    gov.embedding_dims = {_MODEL: 3, "big": 4}
    return gov


def _governed(sql: str, gov: GovernanceContext | None = None) -> str:
    return apply_governance(sql, gov or _gov())


# --- 1. governed with the statement -----------------------------------------------------------


def test_a_search_is_completed_with_its_columns_model_and_dimensions():
    out = _governed("SELECT d.id, cosine_similarity(d.vec, $1) AS s FROM docs d")
    assert "COSINE_SIMILARITY(d.vec, $1, 'mini', 3)" in out


def test_an_unqualified_column_is_found_in_the_one_table_holding_it():
    out = _governed("SELECT id, cosine_similarity(vec, '[1, 0, 0]') AS s FROM docs")
    assert "COSINE_SIMILARITY(vec, '[1.0, 0.0, 0.0]', 'mini', 3)" in out


def test_a_column_that_is_no_embedding_is_refused_naming_it():
    with pytest.raises(SimilarityRefused, match="'d.title' is not an embedding column"):
        _governed("SELECT cosine_similarity(d.title, $1) FROM docs d")


def test_a_column_the_role_is_not_served_is_refused_naming_it():  # REQ-426
    gov = _gov(visible=frozenset({"id", "title"}))
    with pytest.raises(SimilarityNotPermitted, match="may not search embedding column 'd.vec'"):
        _governed("SELECT d.id FROM docs d ORDER BY cosine_similarity(d.vec, $1) DESC", gov)


def test_two_columns_of_different_models_cannot_be_compared():  # REQ-429
    with pytest.raises(SimilarityRefused, match="cannot be compared"):
        _governed("SELECT cosine_similarity(n.vec, n.other) FROM notes n")


def test_two_columns_of_one_model_are_compared():
    out = _governed(
        "SELECT a.id, b.id, cosine_similarity(a.vec, b.vec) AS s FROM docs a JOIN notes b ON true"
    )
    assert "COSINE_SIMILARITY(a.vec, b.vec, 'mini', 3)" in out


def test_a_literal_vector_of_other_dimensions_is_refused():  # REQ-429
    with pytest.raises(SimilarityRefused, match="2 dimensions; the column's model 'mini' has 3"):
        _governed("SELECT cosine_similarity(vec, '[1, 0]') FROM docs")


def test_an_unregistered_model_is_refused():
    gov = _gov()
    gov.embedding_dims = {}
    with pytest.raises(SimilarityRefused, match="'mini' of column 'vec' is not registered"):
        _governed("SELECT cosine_similarity(vec, $1) FROM docs", gov)


def test_a_call_of_other_arity_is_refused():
    with pytest.raises(SimilarityRefused, match="was given 1 arguments"):
        _governed("SELECT cosine_similarity(vec) FROM docs")


def test_a_query_that_is_no_vector_text_or_column_is_refused():
    with pytest.raises(SimilarityRefused, match="not 'id \\+ 1'"):
        _governed("SELECT cosine_similarity(vec, id + 1) FROM docs")


def test_a_search_in_a_subquery_is_governed_with_its_own_tables():
    out = _governed(
        "SELECT x.id FROM (SELECT d.id, cosine_similarity(d.vec, $1) AS s FROM docs d) x "
        "ORDER BY x.s DESC LIMIT 3"
    )
    assert "COSINE_SIMILARITY(d.vec, $1, 'mini', 3)" in out


# --- 2-3. query vectors -----------------------------------------------------------------------


class _Provider:
    def __init__(self, vector):
        self.vector = vector
        self.texts: list[str] = []

    async def embed(self, texts, _model):
        self.texts.extend(texts)
        return [self.vector for _ in texts]


@pytest.mark.asyncio
async def test_a_text_literal_is_embedded_once_with_the_statement():  # REQ-430
    governed = _governed("SELECT id, cosine_similarity(vec, 'red shoes') AS s FROM docs")
    provider = _Provider([0.0, 1.0, 0.0])
    out = await similarity.embed_text_queries(governed, _REGISTRY, provider)
    assert provider.texts == ["red shoes"]
    assert "'[0.0, 1.0, 0.0]', 'mini', 3" in out


@pytest.mark.asyncio
async def test_bound_values_become_vectors_of_the_model():  # REQ-429, REQ-430
    governed = _governed(
        "SELECT id FROM docs WHERE id = $1 ORDER BY cosine_similarity(vec, $2) DESC"
    )
    provider = _Provider([0.5, 0.5, 0.0])
    assert await similarity.bind_query_vectors(governed, [7, [1, 0, 0]], _REGISTRY) == [
        7,
        "[1.0, 0.0, 0.0]",
    ]
    assert await similarity.bind_query_vectors(governed, [7, "[0, 1, 0]"], _REGISTRY) == [
        7,
        "[0.0, 1.0, 0.0]",
    ]
    bound = await similarity.bind_query_vectors(governed, [7, "comfy"], _REGISTRY, provider)
    assert bound == [7, "[0.5, 0.5, 0.0]"] and provider.texts == ["comfy"]


@pytest.mark.asyncio
async def test_a_bound_vector_of_other_dimensions_or_type_is_refused():
    governed = _governed("SELECT cosine_similarity(vec, $1) FROM docs")
    with pytest.raises(SimilarityRefused, match="2 dimensions"):
        await similarity.bind_query_vectors(governed, [[1, 0]], _REGISTRY)
    with pytest.raises(SimilarityRefused, match="must be a vector or a text"):
        await similarity.bind_query_vectors(governed, [42], _REGISTRY)


# --- 4. lowered to the terminal -----------------------------------------------------------------

_GOVERNED = "SELECT d.id, cosine_similarity(d.vec, $1, 'mini', 3) AS s FROM docs AS d"


@pytest.mark.parametrize(
    "dialect, pgvector, form",
    [
        ("duckdb", False, "ARRAY_COSINE_SIMILARITY(CAST(d.vec AS REAL[3]), CAST($1 AS REAL[3]))"),
        (
            "trino",
            False,
            "COSINE_SIMILARITY(CAST(d.vec AS ARRAY(DOUBLE)), CAST(JSON_PARSE(@1) AS ARRAY(DOUBLE)))",
        ),
        (
            "snowflake",
            False,
            # Snowflake's identifiers are always quoted (transpile's identify).
            'VECTOR_COSINE_SIMILARITY(CAST("d"."vec" AS VECTOR(FLOAT, 3)), '
            "CAST($1 AS VECTOR(FLOAT, 3)))",
        ),
        ("postgres", True, "(1 - COSINE_DISTANCE(CAST(d.vec AS VECTOR), CAST($1 AS VECTOR)))"),
        ("postgres", False, "FROM UNNEST(CAST(d.vec AS DOUBLE PRECISION[])"),
    ],
)
def test_each_terminal_computes_it_in_its_own_sql(dialect, pgvector, form):
    assert form in transpile(_GOVERNED, dialect, pgvector=pgvector)


def test_a_terminal_without_a_form_refuses_rather_than_passing_the_call_on():
    with pytest.raises(SimilarityRefused, match="cannot be computed in mysql SQL"):
        transpile(_GOVERNED, "mysql")


def test_an_ungoverned_call_never_reaches_a_terminal():
    with pytest.raises(SimilarityRefused, match="was not governed"):
        transpile("SELECT cosine_similarity(vec, '[1,0,0]') FROM docs", "duckdb")


def test_duckdb_ranks_a_governed_search_by_similarity():
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE docs AS SELECT * FROM (VALUES (1, [1.0, 0.0, 0.0]), "
        "(2, [0.0, 1.0, 0.0]), (3, [1.0, 1.0, 0.0])) v(id, vec)"
    )
    governed = _governed(
        "SELECT d.id, cosine_similarity(d.vec, $1) AS s FROM docs d ORDER BY s DESC LIMIT 2"
    )
    rows = con.execute(transpile(governed, "duckdb"), ["[1.0, 0.0, 0.0]"]).fetchall()
    assert [r[0] for r in rows] == [1, 3]
    assert rows[0][1] == pytest.approx(1.0) and rows[1][1] == pytest.approx(0.7071, abs=1e-4)


def test_plain_postgres_ranks_a_governed_search_by_similarity(embedded_postgres):
    import psycopg

    governed = _governed(
        "SELECT d.id, cosine_similarity(d.vec, $1) AS s FROM docs d ORDER BY s DESC LIMIT 2"
    )
    physical = cast(LiteralString, transpile(governed, "postgres").replace("$1", "%s"))
    with psycopg.connect(embedded_postgres.dsn()) as conn:
        conn.execute("CREATE TEMP TABLE docs (id int, vec real[])")
        conn.execute("INSERT INTO docs VALUES (1, '{1,0,0}'), (2, '{0,1,0}'), (3, '{1,1,0}')")
        rows = conn.execute(physical, ["[1.0, 0.0, 0.0]"]).fetchall()
    assert [r[0] for r in rows] == [1, 3]
    assert rows[1][1] == pytest.approx(0.7071, abs=1e-4)


# --- routing ------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "dialect, to_engine",
    [("postgres", False), ("snowflake", False), ("mysql", True), ("bigquery", True)],
)
def test_a_search_runs_on_a_source_only_where_its_sql_computes_it(dialect, to_engine):
    # REQ-863: cosine_similarity is a construct of the dialect capability list.
    from provisa.transpiler.dialect_capabilities import unsupported

    assert (unsupported(_GOVERNED, dialect) == "cosine_similarity") is to_engine
    assert unsupported("SELECT 1", dialect) is None


def test_lowering_leaves_a_statement_without_a_search_alone():
    sql = "SELECT id FROM docs"
    assert similarity.lower(sql, "duckdb") == sql
    assert sqlglot.parse_one(transpile(sql, "duckdb"))
