# Copyright (c) 2026 Kenneth Stott
# Canary: 22e718c0-5bb5-4434-8674-f52b68dfd802
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Open as query → SPARQL (REQ-2136): a GraphQL read's compile result carries the read as SPARQL
beside its Cypher, exact or refused by name. The SPARQL is answered on DuckDB, read back by the
SPARQL front end, to the rows the read's own SQL answers."""

# Requirements: REQ-2136, REQ-2135

from types import SimpleNamespace

import pytest
import sqlglot
from graphql import parse

from provisa.api.admin import dev_queries
from provisa.compiler.params import embed_params_comment
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_rewrite import make_semantic_sql
from provisa.gql.assembler import assemble_rows, take_cut
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.sparql_query.compiler import compile_sparql
from provisa.sparql_query.vocabulary import Grounding, build_vocabulary, column_types_of
from tests.unit import test_dev_queries_cypher_per_root as _per_root
from tests.unit.test_dev_queries_cypher_per_root import ROLE
from tests.unit.test_sql_gen import _build_schema_and_ctx
from tests.unit.test_where_relationships import RELATIONSHIPS

BASE = "https://data.example.com"
con = _per_root.con  # the sales DuckDB fixture


@pytest.fixture
def model(monkeypatch):
    monkeypatch.setenv("PROVISA_PUBLIC_URL", BASE)
    schema, ctx = _build_schema_and_ctx(relationships=RELATIONSHIPS)
    lm = CypherLabelMap.from_schema(ctx, domain_access=["*"])
    vocab = build_vocabulary(lm, BASE, column_types_of(lm, ctx), Grounding())

    async def _vocabulary(_state, _role_id, _ctx, _lm):
        return vocab

    monkeypatch.setattr("provisa.api.rest.cypher_exec._build_label_map", lambda *_a: lm)
    monkeypatch.setattr("provisa.api.rest.sparql_router.vocabulary_for", _vocabulary)
    return ctx, lm, vocab


async def _export(ctx, query: str) -> list[tuple]:
    """Each root's (sql, compiled_sparql, sparql_error)."""
    doc = parse(query)
    state = SimpleNamespace(schema_build_cache={}, source_catalogs=None)
    out = []
    for i, root in enumerate(compile_query(doc, ctx)):
        semantic = make_semantic_sql(embed_params_comment(root.sql, root.params), ctx)
        cypher, error, _ = dev_queries._compile_cypher_for_result(
            root, ctx, state, ROLE, doc, None, semantic, False, False, False, i
        )
        sparql, why = await dev_queries._compile_sparql_for_result(
            cypher, error, ctx, state, "org_admin"
        )
        out.append((make_semantic_sql(root.sql, ctx), root.params, sparql, why))
    return out


def _sparql_rows(con, ctx, lm, vocab, sparql: str) -> list[tuple]:
    compiled = compile_sparql(sparql, vocab, lm)
    sql, _, graph_vars = cypher_to_sql(compiled.ast, lm, {})
    sql = apply_graph_rewrites(sql, graph_vars, lm)
    semantic = make_semantic_sql(sql.sql(dialect="postgres"), ctx)
    cur = con.execute(sqlglot.transpile(semantic, read="postgres", write="duckdb")[0])
    names = [d[0] for d in cur.description]
    rows = assemble_rows(take_cut([dict(zip(names, r)) for r in cur.fetchall()], []), graph_vars)
    return sorted(
        tuple(None if r[v] is None else r[v]["literal"] for v in compiled.variables) for r in rows
    )


async def test_a_read_is_sparql_that_answers_its_own_rows(model, con):
    ctx, lm, vocab = model
    ((_sql, _params, sparql, why),) = await _export(
        ctx, '{ orders(where: {region: {eq: "us"}}) { id status } }'
    )
    assert why is None and sparql is not None
    assert sparql.startswith("PREFIX ") and "SELECT ?id ?status" in sparql
    expected = sorted(
        con.execute("SELECT id, status FROM sales.orders WHERE region = 'us'").fetchall()
    )
    assert _sparql_rows(con, ctx, lm, vocab, sparql) == expected


async def test_a_root_with_no_sparql_form_says_why(model):
    ctx, _lm, _vocab = model
    ((_, _, sparql, why),) = await _export(ctx, "{ orders { id customer { name } } }")
    assert sparql is None and why is not None and "nested object" in why
