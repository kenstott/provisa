# Copyright (c) 2026 Kenneth Stott
# Canary: 8f2d1c60-a7e3-49b5-92c4-1b6e07d3fa52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2190: the metadata functions are the compiler's. A function of its arguments is native on
PostgreSQL and a form elsewhere; a function on a catalog is a form over meta on every dialect, is
never left native, and one with no form is refused by name. The model's oids and names are its own,
not a build's or a role's. Meta's rights hold: the new views are row-scoped as table_columns is, and
served to SQL alone."""

# Requirements: REQ-2190

from __future__ import annotations

import re
from types import SimpleNamespace

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.api._pg_catalog_views import CATALOG_VIEWS
from provisa.api.app_loaders import _META_TABLES
from provisa.pgwire.catalog_pg17 import RELATIONS
from provisa.pgwire.catalog_populate import _physical_to_pg_name, _physical_to_pg_oid
from provisa.security.rights import (
    META_CATALOG_ROW_SCOPES,
    META_DOMAIN_ID,
    META_ROW_SCOPED_VIEWS,
    SQL_CATALOG_TABLES,
    SQL_ONLY_TABLES,
)
from provisa.pgwire.cast_types import described_type, registered_spelling
from provisa.transpiler.function_macros import MACROS, outcome
from provisa.transpiler.metadata_facts import (
    PRIVILEGES,
    UNSERVED,
    CatalogFacts,
    catalog_facts,
    relation,
    role_privileges,
)
from provisa.transpiler.metadata_functions import (
    FORMS,
    READS_THE_SERVER,
    VALUE_FORMS,
    MetadataFunctionRefused,
    expand,
    listed_type_sql,
    pg_type_name_sql,
    pg_type_oid_sql,
    pg_typmod_sql,
)
from provisa.transpiler.metadata_oids import KEY_OID_BASE, metric_oid, table_oid
from provisa.transpiler.metadata_views import (
    CATALOG_VIEW_NAMES,
    LISTS_EVERY_SCHEMA,
    SCOPE_COLUMN,
    SCOPED_BY_COLUMN,
    catalog_view_name,
)
from provisa.transpiler.transpile import transpile, transpile_to_duckdb

_VIEWS = ("table_details", "column_details", "table_keys")
_FACTS = CatalogFacts("analyst", privileges=((16385, "SELECT"),))


def _expanded(sql: str) -> str:
    return expand(sqlglot.parse_one(sql, read="postgres"), _FACTS).sql(dialect="postgres")


# --- functions of their arguments ---------------------------------------------------------------


@pytest.mark.parametrize(("name", "params", "_form"), VALUE_FORMS)
def test_a_value_function_is_a_row_of_the_table_native_on_postgresql(name, params, _form):
    assert (name, len(params)) in MACROS
    assert outcome(name, len(params), "postgres") == "native"
    assert outcome(name, len(params), "duckdb") == "emulated"


def test_a_value_function_stays_native_on_postgresql():
    assert "format_type(23, -1)" in transpile("SELECT format_type(23, -1)", "postgres").lower()


@pytest.mark.parametrize(
    ("call", "answer"),
    (
        ("format_type(23, -1)", "integer"),
        ("format_type(1043, 14)", "character varying(10)"),
        ("format_type(1015, 14)", "character varying(10)[]"),
        ("format_type(1700, 655366)", "numeric(10,2)"),
        ("format_type(1042, -1)", "bpchar"),
        ("format_type(1042, NULL)", "character"),
        ("format_type(1184, 3)", "timestamp(3) with time zone"),
        ("format_type(999999, -1)", "???"),
        ("pg_encoding_to_char(6)", "UTF8"),
        ("pg_encoding_to_char(999)", ""),
    ),
)
def test_a_value_function_has_its_answer_on_a_dialect_without_it(call, answer):
    import duckdb

    assert duckdb.sql(transpile_to_duckdb(f"SELECT {call}")).fetchone() == (answer,)


# --- functions on a catalog ---------------------------------------------------------------------


def test_every_form_reads_back_in_postgresqls_dialect():
    for form in FORMS.values():
        sqlglot.parse_one(form.sql, read="postgres")


def test_no_form_is_left_native_on_the_control_plane():
    """A form that named its own function would run the control plane's: only format_type does,
    which reads no object but a type, and whose every other type is PostgreSQL's own."""
    for (name, _), form in FORMS.items():
        called = {f.name.lower() for f in sqlglot.parse_one(form.sql).find_all(exp.Anonymous)}
        assert name not in called or name == "format_type"


def test_every_form_that_reads_facts_says_so():
    for form in FORMS.values():
        for fact in (PRIVILEGES, UNSERVED):
            assert (fact in form.sql) == (fact in form.facts), (form.name, fact)


def test_the_call_is_replaced_and_its_column_named_for_it():
    out = sqlglot.parse_one(
        _expanded(f"SELECT pg_catalog.pg_get_constraintdef({KEY_OID_BASE})"), read="postgres"
    )
    assert out.selects[0].alias == "pg_get_constraintdef"
    assert not [f for f in out.find_all(exp.Anonymous) if f.name.lower() == "pg_get_constraintdef"]


def test_a_form_reads_the_meta_views_a_statement_is_governed_on():
    assert "meta.table_keys" in _expanded(f"SELECT pg_get_indexdef({KEY_OID_BASE})")
    assert "meta.catalog_pg_description" in _expanded("SELECT col_description(16385, 1)")
    assert "meta.catalog_pg_class" in _expanded("SELECT pg_table_is_visible(16385)")


def test_only_what_a_privilege_function_reads_is_carried():
    assert "WITH" not in _expanded("SELECT pg_table_is_visible(16385)")
    sql = _expanded("SELECT has_table_privilege('orders', 'SELECT')")
    assert f"WITH {PRIVILEGES}" in sql and UNSERVED not in sql
    sql = _expanded("SELECT has_column_privilege('orders', 'id', 'SELECT')")
    assert PRIVILEGES in sql and UNSERVED in sql


def test_a_call_in_an_argument_is_written_too():
    sql = _expanded("SELECT pg_get_expr(pg_get_userbyid(10), 1)")
    assert "pg_get_userbyid" not in sql.lower()


def test_a_statement_with_a_cte_of_its_own_keeps_it():
    sql = _expanded("WITH x AS (SELECT 1 AS a) SELECT has_table_privilege(a, 'SELECT') FROM x")
    tree = sqlglot.parse_one(sql, read="postgres")
    assert "x" in {cte.alias for cte in tree.find_all(exp.CTE)}


def test_a_statement_calling_none_gets_no_facts():
    assert _expanded("SELECT 1") == "SELECT 1"


def test_a_function_on_the_server_with_no_form_is_refused_by_name():
    for name in sorted(READS_THE_SERVER):
        with pytest.raises(MetadataFunctionRefused, match=name) as refused:
            _expanded(f"SELECT {name}(1)")
        assert refused.value.code == "query.function_unsupported"


def test_no_function_is_both_answered_and_refused():
    assert not {name for name, _ in FORMS} & READS_THE_SERVER


@pytest.mark.parametrize(
    "call", ("pg_get_viewdef(1)", "pg_relation_size(1, 'main')", "pg_get_serial_sequence('a', 'b')")
)
def test_what_a_catalog_keeps_nothing_of_is_null(call):
    tree = sqlglot.parse_one(_expanded(f"SELECT {call}"), read="postgres")
    assert isinstance(tree.selects[0].unalias(), exp.Cast)
    assert isinstance(tree.selects[0].unalias().this, exp.Null)


# --- oids, names and facts ----------------------------------------------------------------------


def test_an_oid_is_a_function_of_a_key_the_model_holds():
    assert table_oid(5) == 16389
    assert metric_oid("revenue") == metric_oid("revenue")
    assert metric_oid("revenue") != metric_oid("cost")
    assert 3_000_000_000 <= metric_oid("revenue") < 2**32


def test_a_roles_facts_are_by_relation_oid():
    facts = catalog_facts("analyst", {1: {"INSERT", "SELECT"}}, [(1, "secret")], [metric_oid("m")])
    assert facts.privileges == (
        (16385, "INSERT"),
        (16385, "SELECT"),
        (metric_oid("m"), "SELECT"),
    )
    assert facts.unserved == ((16385, "secret"),)


def test_a_role_holds_select_on_what_it_is_served_and_what_its_grants_and_the_source_take():
    held = role_privileges(
        [1, 2, 3],
        {1: {"insert": {}, "delete": {}}, 3: {"insert": {}}},
        {1: {"insert"}, 3: {"insert"}},
        can_write=True,
    )
    assert held == {
        1: {"SELECT", "INSERT"},
        2: {"SELECT"},
        3: {"SELECT", "INSERT"},
    }
    assert role_privileges([1], {1: {"insert": {}}}, {1: {"insert"}}, can_write=False) == {
        1: {"SELECT"}
    }


def test_facts_with_no_rows_are_an_empty_relation_of_their_columns():
    select = relation(UNSERVED, ())
    assert [s.alias for s in select.selects] == ["relation_oid", "column_name"]
    assert "FALSE" in select.sql(dialect="postgres").upper()


@pytest.mark.parametrize("dialect", ("postgres", "duckdb"))
def test_facts_are_values_a_dialect_reads(dialect):
    assert "SELECT" in relation(PRIVILEGES, _FACTS.privileges).sql(dialect=dialect)


# --- the type a column is listed as, one source -------------------------------------------------


_SPELLINGS = (
    "varchar",
    "VARCHAR(10)",
    "char(3)",
    "character varying(20)",
    "bpchar",
    "numeric",
    "numeric(10,2)",
    "numeric(6)",
    "decimal(5, 1)",
    "integer",
    "bigint",
    "double",
    "timestamp",
    "timestamptz",
    "timestamp with time zone",
    "json",
    "jsonb",
    "row",
    "array",
    "varbinary",
    "uuid",
    "text",
    "unheard",
    "money",
    "inet",
    "geometry",
    "tsvector",
)


def _duckdb_value(sql: str):
    import duckdb

    return duckdb.sql(transpile_to_duckdb(f"SELECT {sql}")).fetchone()[0]


def _quoted(value: str | None) -> str:
    return "CAST(NULL AS TEXT)" if value is None else f"'{value}'"


@pytest.mark.parametrize("spelling", _SPELLINGS)
def test_a_spelling_is_named_and_numbered_as_the_catalog_does(spelling):
    literal = _quoted(spelling)
    assert _duckdb_value(pg_type_name_sql(literal)) == _physical_to_pg_name(spelling)
    assert _duckdb_value(pg_type_oid_sql(literal)) == _physical_to_pg_oid(spelling)


@pytest.mark.parametrize("spelling", _SPELLINGS)
def test_a_spelling_has_the_typmod_pgwire_describes(spelling):
    described = described_type(spelling)
    expected = described.typmod if described is not None else -1
    assert _duckdb_value(pg_typmod_sql(_quoted(spelling))) == expected


@pytest.mark.parametrize(
    ("data_type", "native", "subtype"),
    (
        ("text", "varchar(20)", None),
        ("text", "character(3)", None),
        ("text", "citext", None),
        ("varchar", "varchar(20)", None),
        ("text", None, None),
        ("varchar", None, "inet"),
    ),
)
def test_a_column_is_listed_as_the_catalog_lists_it(data_type, native, subtype):
    sql = listed_type_sql(_quoted(data_type), _quoted(native), _quoted(subtype))
    assert _duckdb_value(sql) == subtype or registered_spelling(data_type, native)


# --- meta's rights ------------------------------------------------------------------------------


@pytest.mark.parametrize("name", _VIEWS)
def test_a_catalog_view_is_registered_scoped_and_served_to_sql_alone(name):
    assert name in _META_TABLE_VIEWS
    assert name in _META_TABLES
    assert name in META_ROW_SCOPED_VIEWS
    assert (META_DOMAIN_ID, name) in SQL_ONLY_TABLES
    assert (META_DOMAIN_ID, name) in SQL_CATALOG_TABLES


def test_a_foreign_key_is_read_where_both_its_tables_are():
    assert META_ROW_SCOPED_VIEWS["table_keys"] == ("table_id", "referenced_table_id")


def test_a_default_tier_role_reads_a_catalog_view_scoped_to_its_neighbourhood():
    from provisa.compiler.stage2 import build_governance_context
    from provisa.compiler.sql_gen import CompilationContext, TableMeta

    ctx = CompilationContext(role_id="analyst")
    for table_id, name in ((10, "table_keys"), (11, "column_details"), (12, "table_details")):
        ctx.tables[name] = TableMeta(
            table_id=table_id,
            field_name=name,
            type_name=name,
            source_id="provisa-admin",
            catalog_name="provisa_admin",
            schema_name="public",
            table_name=name,
            domain_id=META_DOMAIN_ID,
        )
    role = {"id": "analyst", "capabilities": [], "domain_access": ["sales"]}
    tables = [
        {
            "id": 1,
            "domain_id": "sales",
            "source_id": "s",
            "schema_name": "public",
            "table_name": "orders",
            "columns": [{"column_name": "id", "visible_to": ["analyst"]}],
        }
    ]
    gov = build_governance_context("analyst", SimpleNamespace(rules={}), {}, ctx, tables, role, [])
    assert gov.rls_rules[10] == "table_id IN (1) AND referenced_table_id IN (1)"
    assert gov.rls_rules[11] == "table_id IN (1)"
    assert gov.rls_rules[12] == "id IN (1)"


def test_a_catalog_view_names_no_governance_column():
    from provisa.security.rights import GOVERNANCE_META_COLUMNS

    for name in _VIEWS:
        parsed = sqlglot.parse_one(_META_TABLE_VIEWS[name], read="postgres")
        select = parsed.expression
        columns = {e.output_name for e in select.expressions}
        assert not columns & GOVERNANCE_META_COLUMNS, name


def test_a_catalog_view_has_the_columns_of_its_relation_and_no_more():
    """Meta is a superset of the PostgreSQL catalog (REQ-2190): a client's name is all the routing
    a relation needs, so each view is its relation's columns in PostgreSQL 17's order. The one
    column beyond them is the scope of a relation with no oid to scope by, last."""
    for (schema, name), columns in RELATIONS.items():
        view = catalog_view_name(schema, name)
        assert view in CATALOG_VIEWS, view
        select = sqlglot.parse_one(CATALOG_VIEWS[view], read="postgres").expression
        while select.key == "union":
            select = select.this
        written = [e.output_name for e in select.expressions]
        expected = [column for column, _, _ in columns]
        if view in SCOPED_BY_COLUMN:
            expected.append(SCOPE_COLUMN)
        assert written == expected, view


def test_every_relation_is_a_registered_view_served_to_sql_alone():
    assert len(CATALOG_VIEW_NAMES) == len(RELATIONS)
    for name in CATALOG_VIEW_NAMES:
        assert name in _META_TABLES
        assert (META_DOMAIN_ID, name) in SQL_ONLY_TABLES
        assert (META_DOMAIN_ID, name) in SQL_CATALOG_TABLES


def test_a_client_name_is_addressed_by_name_alone():
    assert catalog_view_name("pg_catalog", "pg_class") == "catalog_pg_class"
    assert catalog_view_name("information_schema", "domains") == "is_domains"
    assert "domains" not in CATALOG_VIEW_NAMES and "roles" not in CATALOG_VIEW_NAMES


_MODEL_READS = re.compile(
    r"\b(catalog_tables|catalog_columns|catalog_keys|column_defaults|table_keys|column_details|"
    r"table_details|registered_tables|table_columns)\b"
)


def test_a_view_of_model_rows_names_how_its_rows_are_scoped():
    """A view left out of the scope maps would list every table to every role: nothing else would
    say so. Every relation reading the model has an entry; every entry is a relation."""
    reads_model = {name for name, ddl in CATALOG_VIEWS.items() if _MODEL_READS.search(ddl)}
    missing = reads_model - set(META_CATALOG_ROW_SCOPES) - LISTS_EVERY_SCHEMA
    assert missing == set()
    assert set(META_CATALOG_ROW_SCOPES) <= set(CATALOG_VIEWS)


@pytest.mark.parametrize("name", ("catalog_pg_class", "catalog_pg_attribute", "is_columns"))
def test_the_check_above_catches_a_view_dropped_from_the_map(name):
    scoped = dict(META_CATALOG_ROW_SCOPES)
    del scoped[name]
    reads_model = {n for n, ddl in CATALOG_VIEWS.items() if _MODEL_READS.search(ddl)}
    assert reads_model - set(scoped) - LISTS_EVERY_SCHEMA == {name}


def _meta_context(*names: str):
    from provisa.compiler.sql_gen import CompilationContext, TableMeta

    ctx = CompilationContext(role_id="analyst")
    for table_id, name in enumerate(names, 10):
        ctx.tables[name] = TableMeta(
            table_id=table_id,
            field_name=name,
            type_name=name,
            source_id="provisa-admin",
            catalog_name="provisa_admin",
            schema_name="public",
            table_name=name,
            domain_id=META_DOMAIN_ID,
        )
    return ctx


_ROLE = {"id": "analyst", "capabilities": [], "domain_access": ["sales"]}
_TABLES = [
    {
        "id": 1,
        "domain_id": "sales",
        "source_id": "s",
        "schema_name": "public",
        "table_name": "orders",
        "columns": [{"column_name": "id", "visible_to": ["analyst"]}],
    }
]


def test_a_default_role_scope_lets_system_rows_through_and_no_other_table():
    from provisa.compiler.stage2 import build_governance_context

    ctx = _meta_context("catalog_pg_class", "is_tables", "catalog_pg_constraint")
    gov = build_governance_context(
        "analyst", SimpleNamespace(rules={}), {}, ctx, _TABLES, _ROLE, []
    )
    assert "IS NULL OR" in gov.rls_rules[10]
    assert "IN (1)" in gov.rls_rules[10]
    assert gov.rls_rules[11] == ("((provisa_table_id) IS NULL OR (provisa_table_id) IN (1))")
    assert gov.rls_rules[12].count("IN (1)") == 2  # both ends of a foreign key


def test_a_column_outside_its_relation_is_no_column_of_the_catalog():
    from provisa.compiler.stage2 import build_governance_context

    ctx = _meta_context("catalog_pg_attribute", "is_columns")
    ctx.columns_outside_relation = {(1, "Pets")}
    gov = build_governance_context(
        "analyst", SimpleNamespace(rules={}), {}, ctx, _TABLES, _ROLE, []
    )
    assert "NOT IN ((16385, 'pets'))" in gov.rls_rules[10]
    assert "NOT IN ((16385, 'pets'))" in gov.rls_rules[11]


def test_a_whole_catalog_role_is_not_scoped():
    from provisa.compiler.stage2 import build_governance_context

    ctx = _meta_context("catalog_pg_class")
    role = {**_ROLE, "domain_access": ["*"]}
    gov = build_governance_context("analyst", SimpleNamespace(rules={}), {}, ctx, _TABLES, role, [])
    assert 10 not in gov.rls_rules
