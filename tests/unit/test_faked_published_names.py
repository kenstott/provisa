# Copyright (c) 2026 Kenneth Stott
# Canary: c2dc42e6-71f0-40f7-97aa-b4b45f13d5b2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A faked column reads its fake under every name it has (REQ-1494).

A table with faked columns is read through its faked projection, whose columns carry the names
the table is read by physically. A column published under an alias is written in semantic SQL
by that alias, so a reference to it reads the projection's column of that column: by its alias,
by its physical name, from a star, in a filter, and through GraphQL. Run on DuckDB with the
engine's own fake functions, lowered as every surface lowers a governed statement.
"""

# Requirements: REQ-1494, REQ-040

from __future__ import annotations

import duckdb
import pytest
import sqlglot

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.parser import parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_rewrite import lower_to_catalog_physical
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import apply_governance, build_governance_context
from provisa.fakes import digest as digest_mod
from provisa.fakes.duckdb_functions import register
from provisa.security.masking import MaskingRule, MaskType

ROLE = {"id": "tester", "capabilities": ["query_development"], "domain_access": ["*"]}
REAL = {i: f"user{i}@real.example" for i in range(1, 21)}

TABLES = [
    {
        "id": 1,
        "source_id": "pg",
        "domain_id": "shop",
        "schema_name": "sales",
        "table_name": "customers",
        "columns": [
            {"column_name": "id", "visible_to": ["tester"]},
            {"column_name": "email", "visible_to": ["tester"], "alias": "mail"},
        ],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }
]


@pytest.fixture
def model(monkeypatch):
    monkeypatch.setattr(digest_mod, "_key", b"k" * 32)
    si = SchemaInput(
        tables=TABLES,
        relationships=[],
        column_types={
            1: [
                ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
                ColumnMetadata(column_name="email", data_type="varchar", is_nullable=True),
            ]
        },
        naming_rules=[],
        role=ROLE,
        domains=[{"id": "shop", "description": "Shop"}],
    )
    schema, ctx = generate_schema(si), build_context(si)
    masks = {(1, "tester"): {"email": (MaskingRule(MaskType.fake, fake="email()"), "varchar")}}
    gov = build_governance_context("tester", RLSContext(rules={}), masks, ctx, TABLES, ROLE, [])
    con = duckdb.connect()
    register(con)
    con.execute("ATTACH ':memory:' AS pg")
    con.execute("CREATE SCHEMA pg.sales")
    con.execute("CREATE TABLE pg.sales.customers (id INTEGER, email VARCHAR)")
    for i, email in REAL.items():
        con.execute("INSERT INTO pg.sales.customers VALUES (?, ?)", [i, email])
    return schema, ctx, gov, con


def _run(model, sql: str) -> tuple[list[str], list[tuple]]:
    _schema, ctx, gov, con = model
    assert validate_sql(sql, ctx, gov, ROLE, TABLES) == []
    lowered = lower_to_catalog_physical(apply_governance(sql, gov), ctx)
    cur = con.execute(sqlglot.transpile(lowered, read="postgres", write="duckdb")[0])
    return [d[0] for d in cur.description], cur.fetchall()


def _fakes(model) -> dict[int, str]:
    _names, rows = _run(model, 'SELECT "id", "email" FROM "shop"."customers"')
    fakes = dict(rows)
    assert len(fakes) == len(REAL) and not set(fakes.values()) & set(REAL.values())
    return fakes


def test_an_aliased_faked_column_reads_its_fake_under_its_alias(model):
    fakes = _fakes(model)
    names, rows = _run(model, 'SELECT "id", "mail" FROM "shop"."customers"')
    assert names == ["id", "mail"]
    assert dict(rows) == fakes


def test_a_qualified_alias_reads_the_fake(model):
    fakes = _fakes(model)
    names, rows = _run(model, 'SELECT "c"."id", "c"."mail" AS "m" FROM "shop"."customers" "c"')
    assert names == ["id", "m"]
    assert dict(rows) == fakes


def test_a_star_reads_the_fake_under_the_alias(model):
    fakes = _fakes(model)
    names, rows = _run(model, 'SELECT * FROM "shop"."customers"')
    assert names == ["id", "mail"]
    assert dict(rows) == fakes


def test_a_filter_on_the_alias_reads_the_fake(model):
    fakes = _fakes(model)
    _names, rows = _run(model, f'SELECT "id" FROM "shop"."customers" WHERE "mail" = \'{fakes[7]}\'')
    assert rows == [(7,)]
    _names, rows = _run(model, f'SELECT "id" FROM "shop"."customers" WHERE "mail" = \'{REAL[7]}\'')
    assert rows == []


def test_graphql_reads_the_fake_under_the_alias(model):
    fakes = _fakes(model)
    schema, ctx, _gov, _con = model
    (compiled,) = compile_query(parse_query(schema, "{ customers { id mail } }", ctx=ctx), ctx)
    names, rows = _run(model, compiled.sql)
    assert names == ["id", "mail"]
    assert dict(rows) == fakes
