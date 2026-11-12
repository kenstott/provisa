# Copyright (c) 2026 Kenneth Stott
# Canary: 476ee14f-407c-4e11-977c-31c8480a8774
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2143 (PAGE SEARCH USES THE WEB-SEARCH SYNTAX): the Ontology View's root under each shared
search case keeps the terms the page's own filter keeps. Each case's term (its name and
definition) is a row of the glossary's terms; the root the UI builds for the case's search
(tests/fixtures/graphql_golden/ontology_search_queries.json, ontologyQueryCorpus.test.ts) is
compiled and answered on DuckDB, and lowered to the graph IR and answered too."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from provisa.graphql_front.lower import GRAPHQL
from tests.unit.test_graphql_front_projections import (
    _DUCKDB_TYPES,
    _META,
    _T,
    _cypher,
    _cypher_answer,
    _graphql_answer,
    _model,
)

_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
_CASES = json.loads((_FIXTURES / "page_search_cases.json").read_text())["cases"]
_ROOTS = json.loads((_FIXTURES / "graphql_golden" / "ontology_search_queries.json").read_text())


@pytest.fixture(scope="module")
def terms():
    """One term per case, its id the case's place in the table plus one."""
    _golden, cypher_golden, _schema, _ctx, _lm = _model()
    _si, tables, _rels, types = cypher_golden.model_inputs(_META)
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS provisa_admin")
    con.execute("CREATE SCHEMA provisa_admin.org_default")
    for t in tables:
        if "view_sql" in t or t["id"] not in types:
            continue
        cols = ", ".join(
            f'"{c.column_name}" {_DUCKDB_TYPES.get(c.data_type, "VARCHAR")}' for c in types[t["id"]]
        )
        con.execute(f'CREATE TABLE {_T}"{t["table_name"]}" ({cols})')
    con.executemany(
        f"INSERT INTO {_T}_terms (id, name, definition) VALUES (?, ?, ?)",
        [(i + 1, c["texts"][0], c["texts"][1]) for i, c in enumerate(_CASES)],
    )
    return con


@pytest.mark.parametrize("query", sorted(_ROOTS))
def test_the_root_keeps_what_the_page_keeps(terms, query):
    kept = {row["id"] for row in _graphql_answer(terms, _ROOTS[query])}
    mine = [i for i, c in enumerate(_CASES) if c["query"] == query]
    assert {i + 1 for i in mine if _CASES[i]["kept"]} == kept & {i + 1 for i in mine}


@pytest.mark.parametrize("query", sorted(q for q in _ROOTS if "-" in q))
def test_lowered_to_the_graph_it_keeps_the_same_terms(terms, query):
    graphql = {row["id"] for row in _graphql_answer(terms, _ROOTS[query])}
    rows = _cypher_answer(terms, _cypher(_ROOTS[query], GRAPHQL))
    graph = {row["id"] for row in rows}
    assert graph == graphql
