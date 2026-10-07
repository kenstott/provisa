# Copyright (c) 2026 Kenneth Stott
# Canary: 5c9e2b17-a4f3-4d80-9b61-e7d0c3a8f245
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An aliased column's published name is lowered to its physical name in raw SQL (issue #140).

``events.placed`` is registered with ``alias: placed_on``, so the catalog publishes it as
``placed_on``; the source holds ``placed``. Every reference the raw-SQL pipeline lowers -- select
list, WHERE, GROUP/ORDER BY, function arguments, subqueries, CTEs, qualified and unqualified --
reaches the source as ``placed``, and a selected column keeps its published name in the result.
"""

from __future__ import annotations

import duckdb
import pytest
import sqlglot

from provisa.compiler.sql_rewrite import normalize_table_refs, rewrite_semantic_to_physical
from provisa.compiler.sql_types import CompilationContext, TableMeta

_EVENTS = TableMeta(
    table_id=1,
    field_name="events",
    type_name="Events",
    source_id="pg",
    catalog_name="pg",
    schema_name="public",
    table_name="events",
    domain_id="sales",
)
_USERS = TableMeta(
    table_id=2,
    field_name="users",
    type_name="Users",
    source_id="pg",
    catalog_name="pg",
    schema_name="public",
    table_name="users",
    domain_id="sales",
)


@pytest.fixture
def source():
    """The physical tables, as the source holds them: no column carries a published name."""
    db = duckdb.connect()
    db.execute("CREATE SCHEMA IF NOT EXISTS public")
    db.execute("CREATE TABLE public.events (id INTEGER, placed DATE, user_id INTEGER)")
    db.execute("CREATE TABLE public.users (id INTEGER, name VARCHAR)")
    db.execute("INSERT INTO public.events VALUES (1, DATE '2026-02-01', 1)")
    db.execute("INSERT INTO public.users VALUES (1, 'Ann')")
    return db


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {"events": _EVENTS, "users": _USERS}
    ctx.physical_to_sql = {
        (1, "id"): "id",
        (1, "placed"): "placed_on",
        (1, "user_id"): "user_id",
        (2, "id"): "id",
        (2, "name"): "full_name",
    }
    return ctx


def _lowered(sql: str) -> str:
    return rewrite_semantic_to_physical(sql, _ctx())


def _columns(sql: str) -> set[str]:
    return {c.name for c in sqlglot.parse_one(sql, read="postgres").find_all(sqlglot.exp.Column)}


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT placed_on FROM sales.events",
        "SELECT t.placed_on FROM sales.events t",
        'SELECT "t"."placed_on" FROM "sales"."events" AS "t"',
        "SELECT id FROM sales.events WHERE placed_on > DATE '2026-01-01'",
        "SELECT MAX(EXTRACT(EPOCH FROM t.placed_on)) FROM sales.events t",
        "SELECT placed_on, COUNT(*) FROM sales.events GROUP BY placed_on ORDER BY placed_on",
        "SELECT x.c FROM (SELECT t.placed_on AS c FROM sales.events t) x",
        "SELECT e.placed_on, u.full_name FROM sales.events e JOIN sales.users u ON u.id = e.user_id",
        "SELECT id FROM sales.events WHERE id IN (SELECT id FROM sales.events WHERE placed_on IS NULL)",
    ],
)
def test_a_published_alias_reaches_the_source_as_its_physical_name(source, sql):
    """The lowered statement runs against the physical tables, which hold no published name; a
    name left is an output name the statement itself defines (ORDER BY placed_on)."""
    lowered = _lowered(sql)
    assert "full_name" not in _columns(lowered), lowered
    source.execute(sqlglot.transpile(lowered, read="postgres", write="duckdb")[0]).fetchall()


def test_a_selected_column_keeps_its_published_name():
    lowered = _lowered("SELECT placed_on, t.id FROM sales.events t")
    select = sqlglot.parse_one(lowered, read="postgres")
    assert select.named_selects == ["placed_on", "id"], lowered


def test_an_outer_query_reads_a_derived_tables_published_name():
    """A CTE selecting placed_on publishes placed_on; the outer reference to it is left alone."""
    lowered = _lowered("WITH e AS (SELECT placed_on FROM sales.events) SELECT e.placed_on FROM e")
    tree = sqlglot.parse_one(lowered, read="postgres")
    assert tree.named_selects == ["placed_on"]
    cte = tree.find(sqlglot.exp.CTE)
    assert cte is not None and cte.this.named_selects == ["placed_on"], lowered


def test_lowering_is_idempotent_and_never_renames_a_physical_name():
    once = normalize_table_refs("SELECT placed_on FROM sales.events", _ctx())
    assert normalize_table_refs(once, _ctx()) == once
    # Already physical: left as it is.
    physical = normalize_table_refs('SELECT "placed" FROM "public"."events"', _ctx())
    assert _columns(physical) == {"placed"}


def test_a_truncate_target_is_qualified_with_no_alias():
    """REQ-1942: TRUNCATE's grammar has no place for an alias, and a TRUNCATE has no column
    qualifiers for one to keep bound."""
    import sqlglot

    lowered = normalize_table_refs("TRUNCATE TABLE sales.events", _ctx())
    assert " AS " not in lowered.upper()
    assert isinstance(sqlglot.parse_one(lowered, read="postgres"), sqlglot.exp.TruncateTable)
