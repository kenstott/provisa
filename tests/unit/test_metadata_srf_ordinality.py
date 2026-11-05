# Copyright (c) 2026 Kenneth Stott
# Canary: 53993579-aacd-4321-b3e7-53ce903d0915
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A set-returning catalog function in FROM WITH ORDINALITY keeps its ordinality column
(REQ-2167, REQ-2204): the replacement yields it, named by the alias list."""

from __future__ import annotations

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler.metadata_facts import CatalogFacts
from provisa.transpiler.metadata_functions import expand

_FACTS = CatalogFacts("analyst", privileges=((16385, "SELECT"),))

# psql \d+ (ids 26 and 38 of the psql tool replay): the trigger's parent, through the ancestors.
_TRIGGER_PARENT = """
SELECT (SELECT u.tgrelid::pg_catalog.regclass
        FROM pg_catalog.pg_trigger AS u,
             pg_catalog.pg_partition_ancestors(t.tgrelid) WITH ORDINALITY AS a(relid, depth)
        WHERE u.tgname = t.tgname AND u.tgrelid = a.relid AND u.tgparentid = 0
        ORDER BY a.depth LIMIT 1) AS parent
FROM pg_catalog.pg_trigger t
"""


def _expanded(sql: str) -> exp.Expr:
    return expand(sqlglot.parse_one(sql, read="postgres"), _FACTS)


def _ordinality_subquery(tree: exp.Expr, alias: str) -> exp.Subquery:
    found = [s for s in tree.find_all(exp.Subquery) if s.alias == alias]
    assert len(found) == 1
    return found[0]


def test_the_psql_trigger_parent_query_gives_the_alias_its_two_columns():
    sub = _ordinality_subquery(_expanded(_TRIGGER_PARENT), "a")
    assert [c.name for c in sub.args["alias"].columns] == ["relid", "depth"]
    assert sub.this.find(exp.RowNumber) is not None


@pytest.mark.parametrize(
    ("call", "columns"),
    [
        ("pg_catalog.pg_partition_ancestors(1)", ["relid", "depth"]),
        ("pg_catalog.pg_partition_tree(1)", ["relid", "parentrelid", "isleaf", "level", "n"]),
        (
            "pg_catalog.pg_get_keywords()",
            ["word", "catcode", "barelabel", "catdesc", "baredesc", "n"],
        ),
    ],
)
def test_any_set_returning_form_yields_the_ordinality_named_by_the_alias_list(call, columns):
    sub = _ordinality_subquery(
        _expanded(f"SELECT * FROM {call} WITH ORDINALITY AS a({', '.join(columns)})"), "a"
    )
    assert [c.name for c in sub.args["alias"].columns] == columns
    assert sub.this.selects[-1].alias == "ordinality"
    assert sub.this.find(exp.RowNumber) is not None


def test_without_an_alias_list_the_ordinality_column_is_named_ordinality():
    sub = _ordinality_subquery(
        _expanded("SELECT * FROM pg_catalog.pg_partition_ancestors(1) WITH ORDINALITY AS a"), "a"
    )
    assert sub.this.selects[-1].alias == "ordinality"


def test_a_function_without_ordinality_gets_no_row_number():
    sub = _ordinality_subquery(
        _expanded("SELECT * FROM pg_catalog.pg_partition_ancestors(1) AS a(relid)"), "a"
    )
    assert sub.this.find(exp.RowNumber) is None
