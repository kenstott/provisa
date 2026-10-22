# Copyright (c) 2026 Kenneth Stott
# Canary: 18198a11-aacf-4639-8f73-0941989de494
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement's conditions and LIMIT on a SPARQL table are pushed into the table's SELECT
(REQ-2138): only what narrows the rows the statement can read, never what would drop one."""

# Requirements: REQ-2138

from __future__ import annotations

import pytest

from provisa.api_source.models import ApiColumn, ApiColumnType, ApiEndpoint
from provisa.sparql.pushdown import QUERY_ARG, narrowed_query, pushed_arguments

T = ApiColumnType
TEMPLATE = (
    "PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n"
    "SELECT ?name ?employees ?listed ?founded WHERE { ?c wdt:P1 ?name }"
)
COLUMNS = {
    "name": ("name", T.string),
    "employees": ("employees", T.integer),
    "listed": ("listed", T.boolean),
    "founded": ("founded", T.date),
}
INNER = "{ SELECT ?name ?employees ?listed ?founded WHERE { ?c wdt:P1 ?name } }"


def _narrowed(sql: str) -> str | None:
    return narrowed_query(TEMPLATE, sql, "co", COLUMNS)


def test_conditions_and_limit_become_filters_and_a_limit_after_the_prefixes():
    sql = "SELECT name FROM s.co WHERE name = 'Acme \"x\"' AND employees >= 10 LIMIT 5"
    assert _narrowed(sql) == (
        "PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n"
        f"SELECT * WHERE {{\n  {INNER}\n"
        '  FILTER(STR(?name) = "Acme \\"x\\"")\n'
        "  FILTER(?employees >= 10)\n"
        "}\nLIMIT 5"
    )


def test_in_lists_and_booleans_are_pushed_with_an_alias():
    sql = "SELECT c.name FROM s.co AS c WHERE c.name IN ('a', 'b') AND c.listed = TRUE"
    narrowed = _narrowed(sql)
    assert narrowed is not None
    assert 'FILTER(STR(?name) IN ("a", "b"))' in narrowed
    assert "FILTER(?listed = true)" in narrowed
    assert "LIMIT" not in narrowed


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT name FROM s.co WHERE name > 'm'",  # text order is the engine's collation
        "SELECT name FROM s.co WHERE founded = DATE '2020-01-01'",  # dates not pushed
        "SELECT name FROM s.co WHERE upper(name) = 'A'",
        "SELECT name FROM s.co WHERE name = $1",
        "SELECT a.name FROM s.co a JOIN s.x b ON a.name = b.n WHERE a.name = 'z' LIMIT 1",
        "SELECT name FROM s.co UNION ALL SELECT name FROM s.co WHERE name = 'z'",
    ],
)
def test_nothing_is_pushed_that_is_not_safely_the_endpoints_to_answer(sql):
    assert _narrowed(sql) is None


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count(*) FROM s.co LIMIT 1",
        "SELECT name FROM s.co ORDER BY name LIMIT 3",
        "SELECT DISTINCT name FROM s.co LIMIT 3",
        "SELECT name FROM s.co LIMIT 3 OFFSET 2",
        "SELECT name, employees FROM s.co GROUP BY name, employees LIMIT 3",
    ],
)
def test_a_limit_is_pushed_only_when_the_first_rows_are_the_answer(sql):
    assert _narrowed(sql) is None


def test_a_limit_is_kept_back_when_a_condition_is_not_pushed():
    narrowed = _narrowed("SELECT name FROM s.co WHERE name = 'a' AND upper(name) = 'A' LIMIT 3")
    assert narrowed is not None
    assert 'FILTER(STR(?name) = "a")' in narrowed and "LIMIT" not in narrowed


def test_a_bare_limit_is_pushed():
    assert _narrowed("SELECT name FROM s.co LIMIT 3") == (
        "PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n"
        f"SELECT * WHERE {{\n  {INNER}\n}}\nLIMIT 3"
    )


def _endpoint(normalizer: str | None) -> ApiEndpoint:
    return ApiEndpoint(
        source_id="wd",
        path="/sparql",
        method="POST",
        table_name="co",
        columns=[ApiColumn(name="legalName", type=T.string)],
        body_encoding="form",
        query_template="SELECT ?legalName WHERE { ?c <urn:n> ?legalName }",
        response_normalizer=normalizer,
    )


def test_the_argument_reads_columns_by_their_sql_names():
    args = pushed_arguments(
        _endpoint("sparql_bindings"), "SELECT legal_name FROM co WHERE legal_name = 'A'", "co"
    )
    assert 'FILTER(STR(?legalName) = "A")' in args[QUERY_ARG]


def test_only_a_sparql_table_is_narrowed():
    assert pushed_arguments(_endpoint(None), "SELECT legal_name FROM co LIMIT 1", "co") == {}
    assert pushed_arguments(_endpoint("sparql_bindings"), "SELECT legal_name FROM co", "co") == {}
