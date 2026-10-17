# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table is linked to its registration by its domain and its name as queried.

Every registered table carries ``_domain_`` (its domain) and ``_name_`` (its name as a statement
queries it, as the compiler names it -- its alias when it has one, no domain prefix). ``meta.registered_tables``
answers each registration's name as queried in ``table_query_name``, which Provisa computes by
table id from the same naming, so nothing stored can go stale; the pair is unique. The GraphQL
``_meta`` join, a SQL join and the pgwire catalog's foreign key all match on that pair -- for an
aliased table too, where the registered table_name is not the name it is queried by."""

# Requirements: REQ-641

from __future__ import annotations

import re

import duckdb
import pytest
from graphql import parse

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_rewrite import rewrite_semantic_to_physical

_V = ["admin"]


def _col(name: str, data_type: str = "varchar") -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=False)


def _table(table_id, domain, name, columns, **extra):
    return {
        "id": table_id,
        "source_id": "provisa-meta" if domain == "meta" else "sales-pg",
        "domain_id": domain,
        "schema_name": "public",
        "table_name": name,
        "columns": [{"column_name": c, "visible_to": _V} for c in columns],
        "write_ops": [],
        "write_returns_rows": True,
        "write_refused_forms": [],
        **extra,
    }


@pytest.fixture
def ctx():
    si = SchemaInput(
        tables=[
            _table(1, "sales", "orders", ["id"], alias="purchases"),
            _table(2, "sales", "customers", ["id"]),
            _table(3, "meta", "registered_tables", ["id", "domain_id", "table_name"]),
        ],
        relationships=[],
        column_types={
            1: [_col("id", "integer")],
            2: [_col("id", "integer")],
            3: [_col("id", "integer"), _col("domain_id"), _col("table_name")],
        },
        naming_rules=[],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": ""}, {"id": "meta", "description": ""}],
    )
    generate_schema(si)
    return build_context(si)


@pytest.fixture
def source():
    """The physical tables: registered_tables as the control plane holds it."""
    db = duckdb.connect()
    db.execute("CREATE SCHEMA IF NOT EXISTS public")
    db.execute("CREATE TABLE public.orders (id INTEGER)")
    db.execute("INSERT INTO public.orders VALUES (10), (11)")
    db.execute("CREATE TABLE public.customers (id INTEGER)")
    db.execute(
        "CREATE TABLE public.registered_tables (id INTEGER, domain_id VARCHAR, table_name VARCHAR)"
    )
    db.execute(
        "INSERT INTO public.registered_tables VALUES "
        "(1, 'sales', 'orders'), (2, 'sales', 'customers'), (3, 'meta', 'registered_tables')"
    )
    return db


def test_name_is_the_name_queried_and_domain_the_domain(ctx):
    assert ctx.virtual_columns[1] == {"_name_": "purchases", "_domain_": "sales"}
    assert ctx.virtual_columns[2] == {"_name_": "customers", "_domain_": "sales"}


def test_a_registration_answers_the_name_its_table_is_queried_by(ctx, source):
    sql = (
        'SELECT m.id, m.table_query_name FROM "meta"."registered_tables" AS m '
        "WHERE m.domain_id = 'sales' ORDER BY m.id"
    )
    rows = source.execute(rewrite_semantic_to_physical(sql, ctx)).fetchall()
    assert rows == [(1, "purchases"), (2, "customers")]


def test_a_sql_join_on_domain_and_name_finds_the_tables_own_registration(ctx, source):
    sql = (
        'SELECT o.id, m.id FROM "sales"."purchases" AS o JOIN "meta"."registered_tables" AS m '
        "ON m.domain_id = o._domain_ AND m.table_query_name = o._name_ ORDER BY o.id"
    )
    rows = source.execute(rewrite_semantic_to_physical(sql, ctx)).fetchall()
    assert rows == [(10, 1), (11, 1)]


def test_the_graphql_meta_join_finds_the_aliased_tables_own_registration(ctx, source):
    [compiled] = compile_query(parse("{ purchases { id _meta { id } } }"), ctx, flat=True)
    rows = source.execute(compiled.sql).fetchall()
    assert sorted(rows) == [(10, 1), (11, 1)]


def test_the_catalog_publishes_the_link_as_a_composite_foreign_key(ctx):
    from provisa.pgwire.catalog_constraints import _populate_pg_constraint
    from provisa.pgwire.catalog_populate import _build_catalog_index
    from provisa.pgwire.catalog_rewrite import _rewrite_for_duckdb

    col_types = {
        1: [_col("id", "integer")],
        2: [_col("id", "integer")],
        3: [_col("id", "integer"), _col("domain_id"), _col("table_name")],
    }
    idx = _build_catalog_index(ctx, col_types)
    db = duckdb.connect()
    db.execute("CREATE TABLE _pg_namespace (oid INTEGER, nspname VARCHAR)")
    db.executemany(
        "INSERT INTO _pg_namespace VALUES (?, ?)", [(o, n) for n, o in idx.ns_map.items()]
    )
    db.execute("CREATE TABLE _pg_class (oid INTEGER, relname VARCHAR, relnamespace INTEGER)")
    db.executemany(
        "INSERT INTO _pg_class VALUES (?, ?, ?)",
        [(toid, name, idx.ns_map[sch]) for _cat, sch, name, _tid, toid in idx.tables],
    )
    _populate_pg_constraint(db, ctx, idx)
    toid = idx.table_id_to_oid[1]
    rows = db.execute(
        _rewrite_for_duckdb(
            "SELECT r.conname, pg_catalog.pg_get_constraintdef(r.oid, true) FROM "
            f"pg_catalog.pg_constraint r WHERE r.conrelid = {toid} AND r.contype = 'f'",
            "admin",
            "provisa",
        )
    ).fetchall()
    defs = [d for _n, d in rows]
    assert defs == [
        "FOREIGN KEY (_domain_, _name_) REFERENCES meta.registered_tables(domain_id, "
        "table_query_name)"
    ]
    m = re.search(r"FOREIGN KEY \((.*?)\) REFERENCES (?:(.*?)\.)?(.*?)\((.*?)\)", defs[0])
    assert m is not None and m.group(3) == "registered_tables"


def test_a_role_reads_the_link_columns_of_every_table_it_reads():
    """Virtual columns are served with the table: a role that sees only some of a table's columns
    still reads its _domain_/_name_, and registered_tables' table_query_name."""
    import sqlglot

    from provisa.compiler.rls import RLSContext
    from provisa.compiler.sql_validator import _check_column_visibility
    from provisa.compiler.stage2 import build_governance_context

    analyst = ["analyst"]
    raw = [
        _table(1, "sales", "orders", ["id"], alias="purchases"),
        _table(3, "meta", "registered_tables", ["id", "domain_id", "table_name"]),
    ]
    raw[0]["columns"].append({"column_name": "secret", "visible_to": ["admin"]})
    for t in raw:
        for c in t["columns"]:
            c["visible_to"] = c["visible_to"] if c["column_name"] == "secret" else analyst
    si = SchemaInput(
        tables=raw,
        relationships=[],
        column_types={
            1: [_col("id", "integer"), _col("secret")],
            3: [_col("id", "integer"), _col("domain_id"), _col("table_name")],
        },
        naming_rules=[],
        role={"id": "analyst", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": ""}, {"id": "meta", "description": ""}],
    )
    ctx = build_context(si)
    role = {"id": "analyst", "domain_access": ["*"], "capabilities": ["query_development"]}
    gov = build_governance_context("analyst", RLSContext.empty(), {}, ctx, raw, role)
    for sql in (
        'SELECT o.id, o._domain_, o._name_ FROM "sales"."purchases" AS o',
        'SELECT m.id, m.table_query_name FROM "meta"."registered_tables" AS m',
    ):
        assert _check_column_visibility(sqlglot.parse_one(sql, read="postgres"), gov) == [], sql
    refused = _check_column_visibility(
        sqlglot.parse_one('SELECT o.secret FROM "sales"."purchases" AS o', read="postgres"), gov
    )
    assert [v.code for v in refused] == ["V003"]


def test_governance_keeps_the_link_columns_in_a_projection_and_a_join():
    """A column outside a table's visible set is dropped from a projection and read as NULL in a
    predicate; the virtual columns are served, so neither happens to them."""
    import sqlglot

    from provisa.compiler.rls import RLSContext
    from provisa.compiler.stage2 import build_governance_context, govern_fragment

    raw = [
        _table(1, "sales", "orders", ["id"], alias="purchases"),
        _table(3, "meta", "registered_tables", ["id", "domain_id", "table_name"]),
    ]
    si = SchemaInput(
        tables=raw,
        relationships=[],
        column_types={
            1: [_col("id", "integer")],
            3: [_col("id", "integer"), _col("domain_id"), _col("table_name")],
        },
        naming_rules=[],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": ""}, {"id": "meta", "description": ""}],
    )
    ctx = build_context(si)
    role = {"id": "admin", "domain_access": ["*"], "capabilities": ["query_development"]}
    gov = build_governance_context("admin", RLSContext.empty(), {}, ctx, raw, role)
    governed = sqlglot.parse_one(
        govern_fragment(
            'SELECT o.id, m.table_query_name FROM "sales"."purchases" AS o '
            'JOIN "meta"."registered_tables" AS m '
            "ON m.domain_id = o._domain_ AND m.table_query_name = o._name_",
            gov,
        ),
        read="postgres",
    )
    assert "table_query_name" in [e.alias_or_name for e in governed.selects]
    on = governed.find(sqlglot.exp.Join).args["on"]
    assert {c.name for c in on.find_all(sqlglot.exp.Column)} >= {
        "table_query_name",
        "_name_",
        "_domain_",
    }
