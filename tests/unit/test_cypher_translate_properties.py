# Copyright (c) 2026 Kenneth Stott
# Canary: 2f6a9f61-e4a8-4feb-b82e-bdc9e8cf1df5
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Property-based tests for Cypher -> SQL translation (REQ-345, REQ-347, REQ-352).

cypher_to_sql lowers a parsed Cypher MATCH/RETURN to SQL against a label map — the
core of the Bolt read path, a silent-wrong-answer surface. A bug references a table
the query never named, drops or renames a returned property, or emits invalid SQL.
Generate MATCH (n:Person) ... RETURN queries over a fixed label map and assert:

  * the translated SQL parses;
  * it references ONLY the mapped table (persons);
  * every returned property reaches the SQL under its physical column name;
  * every RETURN alias (or the bare property name) is an output column of the SQL.
"""

from __future__ import annotations

import sqlglot
from hypothesis import given, settings
from hypothesis import strategies as st
from sqlglot import exp

from provisa.cypher.parser import parse_cypher
from provisa.cypher.translator import cypher_to_sql
from tests.unit.test_cypher_translator import _make_label_map

_LM = _make_label_map()  # Person -> persons(name, age, scores), Company -> companies
_PROPS = ["name", "age", "scores"]


@st.composite
def _match_return(draw):
    """MATCH (n:Person) [WHERE n.age > k] RETURN n.p [AS alias], ...
    Returns (query, expected_output_names, requested_props)."""
    props = draw(st.lists(st.sampled_from(_PROPS), min_size=1, max_size=3, unique=True))
    items: list[str] = []
    expected_names: list[str] = []
    for p in props:
        if draw(st.booleans()):
            alias = f"a_{p}"
            items.append(f"n.{p} AS {alias}")
            expected_names.append(alias)
        else:
            items.append(f"n.{p}")
            expected_names.append(p)
    query = "MATCH (n:Person)"
    threshold = draw(st.one_of(st.none(), st.integers(min_value=0, max_value=100)))
    if threshold is not None:
        query += f" WHERE n.age > {threshold}"
    query += " RETURN " + ", ".join(items)
    return query, set(expected_names), set(props)


@settings(max_examples=300, deadline=None)
@given(case=_match_return())
def test_cypher_translation_is_valid_and_faithful(case) -> None:
    query, expected_names, props = case
    sql_ast, _params, _vars = cypher_to_sql(parse_cypher(query), _LM, {})
    sql = sql_ast.sql(dialect="postgres")

    tree = sqlglot.parse_one(sql, read="postgres")
    assert {t.name for t in tree.find_all(exp.Table)} == {"persons"}

    # Every returned property reaches the SQL under its physical column name.
    for p in props:
        assert f'"{p}"' in sql, f"property {p} was dropped from the translation"

    # Every RETURN alias / bare property is an output column of the SELECT.
    output_names = {s.alias_or_name for s in tree.selects}
    assert expected_names <= output_names, "a returned item is missing from the projection"


_COMPANY_PROPS = ["name", "founded"]


@st.composite
def _traversal(draw):
    """MATCH (n:Person)-[:WORKS_AT]->(c:Company) RETURN n.p AS .., c.p AS ..
    Returns (query, expected_output_names)."""
    p_props = draw(st.lists(st.sampled_from(_PROPS), min_size=1, max_size=2, unique=True))
    c_props = draw(st.lists(st.sampled_from(_COMPANY_PROPS), min_size=1, max_size=2, unique=True))
    items, names = [], []
    for p in p_props:
        items.append(f"n.{p} AS p_{p}")
        names.append(f"p_{p}")
    for c in c_props:
        items.append(f"c.{c} AS c_{c}")
        names.append(f"c_{c}")
    query = "MATCH (n:Person)-[:WORKS_AT]->(c:Company) RETURN " + ", ".join(items)
    return query, set(names)


@settings(max_examples=200, deadline=None)
@given(case=_traversal())
def test_relationship_traversal_joins_both_tables(case) -> None:
    """A one-hop traversal joins exactly the two mapped tables and projects the
    aliased properties of both endpoints — no endpoint dropped, no stray table."""
    query, expected_names = case
    sql_ast, _params, _vars = cypher_to_sql(parse_cypher(query), _LM, {})
    sql = sql_ast.sql(dialect="postgres")
    tree = sqlglot.parse_one(sql, read="postgres")

    assert {t.name for t in tree.find_all(exp.Table)} == {"persons", "companies"}
    assert any(isinstance(j, exp.Join) for j in tree.find_all(exp.Join)), "traversal did not join"
    assert expected_names <= {s.alias_or_name for s in tree.selects}
