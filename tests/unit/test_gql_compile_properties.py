# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Property-based tests for GraphQL -> SQL compilation, Stage 1 (REQ-262, REQ-265).

compile_graphql lowers a GraphQL selection to SQL — the front of every read path. A
bug selects columns the client did not ask for (over-fetch / leak), references a
table the query never named (injection surface), drops the LIMIT, or emits invalid
SQL. Generate valid queries over a fixed two-table schema and assert, for each:

  * the SQL parses;
  * it references ONLY the root's table — nothing the query didn't name;
  * its projected columns are exactly the requested fields (no over/under-fetch);
  * a LIMIT/OFFSET argument appears iff the query carried one, with its value bound
    as a parameter (never inlined);
  * root_field echoes the query's root.
"""

from __future__ import annotations

import sqlglot
from graphql import parse as gql_parse
from hypothesis import given, settings
from hypothesis import strategies as st
from sqlglot import exp

from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.stage1 import compile_graphql

_ROOTS = {"orders": "Orders", "customers": "Customers"}
_COLS = ["id", "amount", "region", "name", "status", "created_at"]


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    for i, (field, type_name) in enumerate(_ROOTS.items(), start=1):
        ctx.tables[field] = TableMeta(
            table_id=i,
            field_name=field,
            type_name=type_name,
            source_id="test-pg",
            catalog_name="test_pg",
            schema_name="public",
            table_name=field,
        )
    return ctx


@st.composite
def _gql_query(draw):
    """A single-root GraphQL read over the fixed schema, optionally with limit/offset.
    Returns (query, root, fields, limit, offset)."""
    root = draw(st.sampled_from(sorted(_ROOTS)))
    fields = draw(st.lists(st.sampled_from(_COLS), min_size=1, max_size=5, unique=True))
    limit = draw(st.one_of(st.none(), st.integers(min_value=1, max_value=1000)))
    offset = draw(st.one_of(st.none(), st.integers(min_value=0, max_value=1000)))
    args = []
    if limit is not None:
        args.append(f"limit: {limit}")
    if offset is not None:
        args.append(f"offset: {offset}")
    arg_str = f"({', '.join(args)})" if args else ""
    query = f"{{ {root}{arg_str} {{ {' '.join(fields)} }} }}"
    return query, root, fields, limit, offset


@settings(max_examples=300, deadline=None)
@given(case=_gql_query())
def test_compile_is_valid_governed_and_faithful(case) -> None:
    query, root, fields, limit, offset = case
    results = compile_graphql(gql_parse(query), _ctx())
    assert len(results) == 1
    compiled = results[0]
    assert compiled.root_field == root

    tree = sqlglot.parse_one(compiled.sql, read="postgres")

    # References only the table the query named.
    assert {t.name for t in tree.find_all(exp.Table)} == {root}

    # Projects exactly the requested fields — no over- or under-fetch.
    projected = {c.name for c in tree.find_all(exp.Column)}
    assert projected == set(fields)

    # LIMIT/OFFSET appear iff requested; their values are bound as params, not inlined.
    assert (tree.args.get("limit") is not None) == (limit is not None)
    assert (tree.args.get("offset") is not None) == (offset is not None)
    expected_params = sum(x is not None for x in (limit, offset))
    assert len(compiled.params) == expected_params
    for literal in (limit, offset):
        if literal is not None:
            assert literal in compiled.params
