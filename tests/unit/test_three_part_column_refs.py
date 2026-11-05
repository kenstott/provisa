# Copyright (c) 2026 Kenneth Stott
# Canary: 4957ea60-927a-4086-92a1-da4c8df1b333
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column a statement qualifies by its table's schema ("conformance"."bi_customer"."id", as
Metabase writes every data query) is read by the name its table has in FROM once the statement is
lowered. Lowering pins an unaliased table's own name as its alias (normalize_table_refs), and
PostgreSQL then refuses the schema-qualified reference: 42P01 "invalid reference to FROM-clause
entry for table". The Metabase statements are the BI replay's (REQ-2204), ids 24-36."""

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_rewrite import lower_to_catalog_physical, rewrite_semantic_to_physical
from provisa.compiler.sql_types import TableMeta
from provisa.compiler.stage2 import GovernanceContext, apply_governance, context_table_map

_COLUMNS = {
    "bi_customer": [("id", "integer"), ("name", "text"), ("region", "text")],
    "bi_order": [
        ("id", "integer"),
        ("customer_id", "integer"),
        ("total", "numeric"),
        ("placed", "date"),
    ],
    "probe_identity": [("id", "integer"), ("note", "text")],
    "types_fixture": [
        (name, "text")
        for name in (
            "c_bool c_jsonb c_json c_int2 c_int4 c_int8 c_float4 c_float8 c_numeric "
            "c_numeric_10_2 c_text c_varchar c_varchar_20 c_bpchar_5 c_date c_time c_timetz "
            "c_timestamp c_timestamptz c_uuid c_inet c_money"
        ).split()
    ],
}

# The replay's Metabase statements (scripts/conformance/bi_tools/metabase.yaml on
# feat/bi-catalog-replay), by id.
_METABASE = {
    24: 'SELECT CASE WHEN 50000 < LENGTH(CAST("conformance"."types_fixture"."c_jsonb" AS TEXT)) '
    'THEN NULL ELSE "conformance"."types_fixture"."c_jsonb" END AS "c_jsonb", CASE WHEN 50000 < '
    'LENGTH(CAST("conformance"."types_fixture"."c_json" AS TEXT)) THEN NULL ELSE '
    '"conformance"."types_fixture"."c_json" END AS "c_json" FROM "conformance"."types_fixture" '
    "LIMIT 500",
    26: '-- Metabase\nSELECT "conformance"."bi_customer"."id" AS "id", '
    'SUBSTRING("conformance"."bi_customer"."name", 1, 1234) AS "substring1859", '
    'SUBSTRING("conformance"."bi_customer"."region", 1, 1234) AS "substring1860" '
    'FROM "conformance"."bi_customer" LIMIT 10000',
    28: '-- Metabase\nSELECT "conformance"."bi_order"."id" AS "id", '
    '"conformance"."bi_order"."customer_id" AS "customer_id", '
    '"conformance"."bi_order"."total" AS "total", "conformance"."bi_order"."placed" AS "placed" '
    'FROM "conformance"."bi_order" LIMIT 10000',
    30: '-- Metabase\nSELECT "conformance"."probe_identity"."id" AS "id", '
    'SUBSTRING("conformance"."probe_identity"."note", 1, 1234) AS "substring1950" '
    'FROM "conformance"."probe_identity" LIMIT 10000',
    32: '-- Metabase\nSELECT "conformance"."types_fixture"."c_bool" AS "c_bool", '
    'SUBSTRING(("conformance"."types_fixture"."c_jsonb"#>> (array[$1]::text[]))::text, 1, 1234) '
    'AS "substring1977", SUBSTRING(("conformance"."types_fixture"."c_json"#>> '
    '(array[$2]::text[]))::text, 1, 1234) AS "substring1978", '
    '("conformance"."types_fixture"."c_jsonb"#>> (array[$3]::text[]))::decimal AS "c_jsonb → n", '
    '("conformance"."types_fixture"."c_json"#>> (array[$4]::text[]))::decimal AS "c_json → n", '
    '"conformance"."types_fixture"."c_int2" AS "c_int2", '
    '"conformance"."types_fixture"."c_int4" AS "c_int4", '
    '"conformance"."types_fixture"."c_int8" AS "c_int8", '
    '"conformance"."types_fixture"."c_float4" AS "c_float4", '
    '"conformance"."types_fixture"."c_float8" AS "c_float8", '
    '"conformance"."types_fixture"."c_numeric" AS "c_numeric", '
    '"conformance"."types_fixture"."c_numeric_10_2" AS "c_numeric_10_2", '
    'SUBSTRING("conformance"."types_fixture"."c_text", 1, 1234) AS "substring1979", '
    'SUBSTRING("conformance"."types_fixture"."c_varchar", 1, 1234) AS "substring1980", '
    'SUBSTRING("conformance"."types_fixture"."c_varchar_20", 1, 1234) AS "substring1981", '
    'SUBSTRING("conformance"."types_fixture"."c_bpchar_5", 1, 1234) AS "substring1982", '
    '"conformance"."types_fixture"."c_date" AS "c_date", '
    '"conformance"."types_fixture"."c_time" AS "c_time", '
    '"conformance"."types_fixture"."c_timetz" AS "c_timetz", '
    '"conformance"."types_fixture"."c_timestamp" AS "c_timestamp", '
    '"conformance"."types_fixture"."c_timestamptz" AS "c_timestamptz", '
    '"conformance"."types_fixture"."c_uuid" AS "c_uuid", '
    '"conformance"."types_fixture"."c_inet" AS "c_inet", '
    '"conformance"."types_fixture"."c_money"::numeric AS "c_money" '
    'FROM "conformance"."types_fixture" LIMIT 10000',
    36: '-- Metabase\nSELECT SUBSTRING("conformance"."probe_identity"."note", 1, 1234) '
    'AS "substring2496" FROM "conformance"."probe_identity" LIMIT 10000',
}


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {
        name: TableMeta(
            table_id=i,
            field_name=name,
            type_name=name.title().replace("_", ""),
            source_id="conformance-pg",
            catalog_name="conformance_pg",
            schema_name="conformance",
            table_name=name,
            domain_id="conformance",
        )
        for i, name in enumerate(_COLUMNS, start=1)
    }
    return ctx


def _org_admin(ctx: CompilationContext) -> GovernanceContext:
    """org_admin on the conformance source: every column served, no row filter, no mask."""
    table_map = context_table_map(ctx)
    gov = GovernanceContext(role_id="org_admin")
    gov.table_map = table_map
    gov.all_columns = {ctx.tables[n].table_id: list(cols) for n, cols in _COLUMNS.items()}
    gov.visible_columns = {tid: None for tid in gov.all_columns}
    return gov


def _stranded(sql: str) -> list[str]:
    """Each column reference ``sql`` qualifies by a schema, or by a name no relation in its
    scope has -- what PostgreSQL refuses with 42P01."""
    tree = sqlglot.parse_one(sql, read="postgres")
    bad = []
    for col in tree.find_all(exp.Column):
        if col.args.get("db") is not None:
            bad.append(col.sql(dialect="postgres"))
            continue
        if not col.table:
            continue
        names: set[str] = set()
        select = col.find_ancestor(exp.Select)
        while select is not None:
            names |= {s.alias_or_name for s in select.find_all(exp.Table, exp.Subquery)}
            select = select.find_ancestor(exp.Select)
        if col.table not in names:
            bad.append(col.sql(dialect="postgres"))
    return bad


@pytest.mark.parametrize("statement_id", sorted(_METABASE))
def test_metabase_reads_lower_with_every_column_bound_to_its_from_entry(statement_id):
    """The DIRECT route on a Postgres source: governed, then lowered to physical."""
    ctx = _ctx()
    governed = apply_governance(_METABASE[statement_id], _org_admin(ctx))
    physical = rewrite_semantic_to_physical(governed, ctx)
    assert 'AS "' in physical.split(" FROM ", 1)[1]  # the table is pinned to its own name
    assert _stranded(physical) == [], physical


@pytest.mark.parametrize("statement_id", [26, 32])
def test_metabase_reads_lower_to_catalog_physical_bound_too(statement_id):
    """The ENGINE route lowers the same way, to catalog-physical."""
    ctx = _ctx()
    governed = apply_governance(_METABASE[statement_id], _org_admin(ctx))
    assert _stranded(lower_to_catalog_physical(governed, ctx)) == []


def _bound(sql: str) -> str:
    from provisa.compiler.sql_rewrite import bind_schema_qualified_columns

    tree = sqlglot.parse_one(sql, read="postgres")
    bind_schema_qualified_columns(tree)
    return tree.sql(dialect="postgres")


def test_a_schema_qualified_column_reads_its_table_by_name():
    assert _bound("SELECT s.t.a, s.t.b + 1 FROM s.t WHERE s.t.a > 0 ORDER BY s.t.b") == (
        "SELECT t.a, t.b + 1 FROM s.t WHERE t.a > 0 ORDER BY t.b"
    )


def test_a_catalog_qualified_column_reads_its_table_by_name():
    assert _bound("SELECT c.s.t.a FROM c.s.t") == "SELECT t.a FROM c.s.t"


def test_a_correlated_reference_binds_to_the_outer_table():
    assert (
        _bound("SELECT s.t.a FROM s.t WHERE EXISTS (SELECT 1 FROM s.u WHERE s.u.k = s.t.a)")
        == "SELECT t.a FROM s.t WHERE EXISTS(SELECT 1 FROM s.u WHERE u.k = t.a)"
    )


def test_joined_tables_each_bind_their_own_references():
    assert _bound("SELECT s.t.a, s.u.b FROM s.t JOIN s.u ON s.t.k = s.u.k") == (
        "SELECT t.a, u.b FROM s.t JOIN s.u ON t.k = u.k"
    )


def test_a_reference_that_names_no_table_in_scope_is_left_as_written():
    """PostgreSQL's to refuse: an aliased table hides its schema-qualified name, and another
    schema's table of the same name is not this one."""
    for sql in (
        "SELECT s.t.a FROM s.t AS x",
        "SELECT other.t.a FROM s.t",
        "SELECT c2.s.t.a FROM c1.s.t",
    ):
        assert _bound(sql) == sqlglot.transpile(sql, read="postgres", write="postgres")[0]
