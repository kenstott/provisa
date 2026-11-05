# Copyright (c) 2026 Kenneth Stott
# Canary: 3c8e51a7-d204-4f96-b8e3-7a19c6d0f2e5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2167, REQ-2204: an object identifier cast in a BI tool's catalog statement is PostgreSQL 17's.
A result column cast to regclass or regnamespace is named as PostgreSQL names it and sent as that
type, a pg_catalog-qualified name the same type as a bare one; a digit string is the oid it spells;
a computed text operand is resolved by name when the statement runs; and a constant or a session
keyword with no alias has PostgreSQL's column name. The statements are the tools' own
(scripts/conformance/bi_tools, J595 triage causes 2, 3 and 7)."""

# Requirements: REQ-2167, REQ-2204

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from provisa.core.pg_error import PostgresError
from provisa.pgwire.catalog import answer, classify
from provisa.pgwire.server import _sqlstate_of


def _state():
    ctx = MagicMock()
    ctx.tables = {}
    state = MagicMock()
    state.multitenancy = False
    state.contexts = {"testrole": ctx}
    state.schema_build_cache = {"column_types": {}}
    return state


def _answer(sql: str):
    assert classify(sql) == "INTERCEPT", sql
    return answer(sql, "testrole", _state())


def _rows(sql: str) -> list[tuple]:
    return _answer(sql).rows


def _table() -> tuple[int, str, str]:
    """A table the catalog lists: its oid, schema and name."""
    ((oid, schema, name),) = _rows(
        "SELECT c.oid, n.nspname, c.relname FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "WHERE c.relkind = 'r' ORDER BY c.oid LIMIT 1"
    )
    return oid, schema, name


# --- psql \d: a regclass result column (cause 2) ----------------------------------------------------


def test_psql_extended_statistics_names_the_regclass_column_after_its_operand():
    """psql #13: an unaliased ``stxrelid::pg_catalog.regclass`` is named stxrelid."""
    oid, _, _ = _table()
    result = _answer(
        "SELECT oid, stxrelid::pg_catalog.regclass, "
        "stxnamespace::pg_catalog.regnamespace::pg_catalog.text AS nsp, stxname,\n"
        "pg_catalog.pg_get_statisticsobjdef_columns(oid) AS columns,\n"
        "  'd' = any(stxkind) AS ndist_enabled,\n"
        "  'f' = any(stxkind) AS deps_enabled,\n"
        "  'm' = any(stxkind) AS mcv_enabled,\n"
        "stxstattarget\n"
        "FROM pg_catalog.pg_statistic_ext\n"
        f"WHERE stxrelid = '{oid}'\n"
        "ORDER BY nsp, stxname;"
    )
    assert result.column_names[:3] == ["oid", "stxrelid", "nsp"]
    assert result.column_types[1] == "regclass"


def test_psql_inherited_tables_names_the_regclass_column_oid():
    """psql #15, run on a relation that is its own parent so a row comes back."""
    oid, _, name = _table()
    result = _answer(
        "SELECT c.oid::pg_catalog.regclass\n"
        "FROM pg_catalog.pg_class c\n"
        f"WHERE c.oid = '{oid}'\n"
        "  AND c.relkind != 'p' AND c.relkind != 'I'"
    )
    assert result.column_names == ["oid"]
    assert result.column_types == ["regclass"]
    ((value,),) = result.rows
    assert value["oid"] == oid
    assert value["name"].endswith(name)


def test_psql_foreign_keys_send_an_aliased_regclass_column_as_regclass():
    """psql #21: ``conrelid::pg_catalog.regclass AS ontable`` is a regclass, not jsonb."""
    oid, _, _ = _table()
    result = _answer(
        "SELECT true as sametable, conname,\n"
        "  pg_catalog.pg_get_constraintdef(r.oid, true) as condef,\n"
        "  conrelid::pg_catalog.regclass AS ontable\n"
        "FROM pg_catalog.pg_constraint r\n"
        f"WHERE r.conrelid = '{oid}' AND r.contype = 'f'\n"
        "     AND conparentid = 0\n"
        "ORDER BY conname"
    )
    assert result.column_names == ["sametable", "conname", "condef", "ontable"]
    assert result.column_types[3] == "regclass"


@pytest.mark.parametrize("qualifier", ["", "pg_catalog."])
def test_a_regnamespace_column_is_a_regnamespace_however_its_type_is_named(qualifier):
    oid, schema, _ = _table()
    result = _answer(
        f"SELECT c.relnamespace::{qualifier}regnamespace FROM pg_catalog.pg_class c "
        f"WHERE c.oid = {oid}"
    )
    assert result.column_names == ["relnamespace"]
    assert result.column_types == ["regnamespace"]
    ((value,),) = result.rows
    assert value["name"] == schema


@pytest.mark.parametrize("qualifier", ["", "pg_catalog."])
def test_a_regclass_of_a_literal_is_named_regclass(qualifier):
    _, _, name = _table()
    result = _answer(f"SELECT '{name}'::{qualifier}regclass")
    assert result.column_names == ["regclass"]
    assert result.column_types == ["regclass"]


# --- regclass input from a digit string (cause 3) ---------------------------------------------------


def test_psql_referencing_foreign_keys_read_a_digit_string_as_an_oid():
    """psql #22: ``'<oid>'::pg_catalog.regclass`` is that oid, not a relation named by it."""
    oid, _, _ = _table()
    result = _answer(
        "SELECT conname, conrelid::pg_catalog.regclass AS ontable,\n"
        "       pg_catalog.pg_get_constraintdef(oid, true) AS condef\n"
        "  FROM pg_catalog.pg_constraint c\n"
        f" WHERE confrelid IN (SELECT pg_catalog.pg_partition_ancestors('{oid}')\n"
        f"                     UNION ALL VALUES ('{oid}'::pg_catalog.regclass))\n"
        "       AND contype = 'f' AND conparentid = 0\n"
        "ORDER BY conname;"
    )
    assert result.column_names == ["conname", "ontable", "condef"]


@pytest.mark.parametrize(
    ("reg", "oid"),
    [
        ("regclass", 999999),  # an oid no relation has is still that oid, as regclassin reads it
        ("regclass", 1259),
        ("regtype", 25),
        ("regnamespace", 11),
        ("regrole", 10),
    ],
)
def test_a_digit_string_is_the_oid_it_spells_for_every_reg_type(reg, oid):
    assert _rows(f"SELECT '{oid}'::pg_catalog.{reg}::oid") == [(oid,)]
    assert _rows(f"SELECT '{oid}'::{reg}::oid") == [(oid,)]


def test_a_digit_string_is_the_oid_of_a_regprocedure():
    # 1242 is boolin in every PostgreSQL 17.
    assert _rows(
        "SELECT p.proname FROM pg_catalog.pg_proc p WHERE p.oid = '1242'::regprocedure"
    ) == [("boolin",)]


# --- regclass input from a computed text operand (cause 3) ------------------------------------------


def test_metabase_field_comment_resolves_a_formatted_name_when_it_runs():
    """Metabase #14: ``FORMAT('%I.%I', schema, table)::regclass::oid`` is resolved by name against
    the catalog when the statement runs, never cast to oid as text."""
    oid, schema, name = _table()
    assert _rows(
        "SELECT CAST(CAST(FORMAT('%I.%I', CAST(n.nspname AS TEXT), CAST(c.relname AS TEXT)) "
        "AS REGCLASS) AS OID) FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        f"WHERE c.oid = {oid}"
    ) == [(oid,)]
    assert _rows(
        "SELECT COL_DESCRIPTION(CAST(CAST(FORMAT('%I.%I', "
        f"CAST('{schema}' AS TEXT), CAST('{name}' AS TEXT)) AS REGCLASS) AS OID), 1)"
    ) == [(None,)]


def test_metabase_column_listing_runs():
    """Metabase #14 itself, its parameters bound."""
    _, schema, _ = _table()
    _rows(
        'SELECT "c"."column_name" AS "name", CASE WHEN "c"."udt_schema" IN (\'public\', '
        '\'pg_catalog\') THEN FORMAT(\'%s\', "c"."udt_name") ELSE FORMAT(\'"%s"."%s"\', '
        '"c"."udt_schema", "c"."udt_name") END AS "database-type", "c"."ordinal_position" - 1 AS '
        '"database-position", "c"."table_schema" AS "table-schema", "c"."table_name" AS '
        '"table-name", "pk"."column_name" IS NOT NULL AS "pk?", COL_DESCRIPTION(CAST(CAST('
        'FORMAT(\'%I.%I\', CAST("c"."table_schema" AS TEXT), CAST("c"."table_name" AS TEXT)) '
        'AS REGCLASS) AS OID), "c"."ordinal_position") AS "field-comment", (("column_default" IS '
        "NULL) OR (LOWER(\"column_default\") = 'null')) AND (\"is_nullable\" = 'NO') AND NOT "
        '((("column_default" IS NOT NULL) AND ("column_default" LIKE \'%nextval(%\')) OR '
        '("is_identity" <> \'NO\')) AS "database-required", "column_default" AS '
        '"database-default", (("column_default" IS NOT NULL) AND ("column_default" LIKE '
        "'%nextval(%')) OR (\"is_identity\" <> 'NO') AS \"database-is-auto-increment\", "
        '"is_generated" = \'NEVER\' AS "database-is-generated", "is_nullable" = \'YES\' AS '
        '"database-is-nullable" FROM "information_schema"."columns" AS "c" LEFT JOIN (SELECT '
        '"tc"."table_schema", "tc"."table_name", "kc"."column_name" FROM '
        '"information_schema"."table_constraints" AS "tc" INNER JOIN '
        '"information_schema"."key_column_usage" AS "kc" ON ("tc"."constraint_name" = '
        '"kc"."constraint_name") AND ("tc"."table_schema" = "kc"."table_schema") AND '
        '("tc"."table_name" = "kc"."table_name") WHERE "tc"."constraint_type" = \'PRIMARY KEY\') '
        'AS "pk" ON ("c"."table_schema" = "pk"."table_schema") AND ("c"."table_name" = '
        '"pk"."table_name") AND ("c"."column_name" = "pk"."column_name") WHERE c.table_schema !~ '
        f"'^information_schema|catalog_history|pg_' AND (\"c\".\"table_schema\" IN ('{schema}')) "
        'UNION ALL SELECT "pa"."attname" AS "name", CASE WHEN "ptn"."nspname" IN (\'public\', '
        '\'pg_catalog\') THEN FORMAT(\'%s\', "pt"."typname") ELSE FORMAT(\'"%s"."%s"\', '
        '"ptn"."nspname", "pt"."typname") END AS "database-type", "pa"."attnum" - 1 AS '
        '"database-position", "pn"."nspname" AS "table-schema", "pc"."relname" AS "table-name", '
        'FALSE AS "pk?", NULL AS "field-comment", FALSE AS "database-required", NULL AS '
        '"database-default", FALSE AS "database-is-auto-increment", FALSE AS '
        '"database-is-generated", FALSE AS "database-is-nullable" FROM "pg_catalog"."pg_class" AS '
        '"pc" INNER JOIN "pg_catalog"."pg_namespace" AS "pn" ON "pn"."oid" = "pc"."relnamespace" '
        'INNER JOIN "pg_catalog"."pg_attribute" AS "pa" ON "pa"."attrelid" = "pc"."oid" INNER '
        'JOIN "pg_catalog"."pg_type" AS "pt" ON "pt"."oid" = "pa"."atttypid" INNER JOIN '
        '"pg_catalog"."pg_namespace" AS "ptn" ON "ptn"."oid" = "pt"."typnamespace" WHERE '
        '("pc"."relkind" = \'m\') AND ("pa"."attnum" >= 1) AND ("pn"."nspname" IN '
        f'(\'{schema}\')) ORDER BY "table-schema" ASC, "table-name" ASC, '
        '"database-position" ASC'
    )


def test_a_computed_digit_string_is_the_oid_it_spells():
    oid, _, _ = _table()
    assert _rows(
        "SELECT CAST(CAST(CAST(c.oid AS TEXT) AS regclass) AS oid) FROM pg_catalog.pg_class c "
        f"WHERE c.oid = {oid}"
    ) == [(oid,)]


def test_a_computed_bare_name_is_read_on_the_search_path():
    assert _rows(
        "SELECT CAST(CAST(c.relname AS TEXT) AS regclass)::oid FROM pg_catalog.pg_class c "
        "WHERE c.oid = 1259"
    ) == [(1259,)]


def test_a_computed_null_is_null():
    assert (
        _rows(
            "SELECT CAST(CAST(c.relname AS TEXT) AS regclass)::oid FROM pg_catalog.pg_class c "
            "WHERE c.oid = 1259 AND c.relname IS NULL"
        )
        == []
    )
    assert _rows(
        "SELECT CAST(CAST(CASE WHEN c.oid < 0 THEN c.relname END AS TEXT) AS regclass)::oid "
        "FROM pg_catalog.pg_class c WHERE c.oid = 1259"
    ) == [(None,)]


def test_a_computed_name_of_no_relation_is_postgresqls_undefined_table():
    with pytest.raises(
        PostgresError, match='relation "no_such_pg_class" does not exist'
    ) as refused:
        _rows(
            "SELECT CAST(CAST('no_such_' || c.relname AS regclass) AS oid) "
            "FROM pg_catalog.pg_class c WHERE c.oid = 1259"
        )
    assert _sqlstate_of(refused.value) == "42P01"


def test_every_reg_type_resolves_a_computed_name():
    ((schema_oid,),) = _rows(
        "SELECT CAST(CAST(n.nspname AS TEXT) AS regnamespace)::oid FROM pg_catalog.pg_namespace n "
        "WHERE n.nspname = 'pg_catalog'"
    )
    assert schema_oid == 11
    assert _rows(
        "SELECT CAST(CAST(t.typname AS TEXT) AS regtype)::oid FROM pg_catalog.pg_type t "
        "WHERE t.oid = 23"
    ) == [(23,)]
    ((role, oid),) = _rows(
        "SELECT r.rolname, r.oid FROM pg_catalog.pg_roles r ORDER BY r.oid LIMIT 1"
    )
    assert _rows(
        "SELECT CAST(CAST(r.rolname AS TEXT) AS regrole)::oid FROM pg_catalog.pg_roles r "
        f"WHERE r.oid = {oid}"
    ) == [(oid,)]
    assert role


@pytest.mark.parametrize(
    ("reg", "message", "sqlstate"),
    [
        ("regnamespace", 'schema "no_such_pg_catalog" does not exist', "3F000"),
        ("regtype", 'type "no_such_pg_catalog" does not exist', "42704"),
        ("regrole", 'role "no_such_pg_catalog" does not exist', "42704"),
    ],
)
def test_every_reg_type_refuses_a_computed_name_of_nothing(reg, message, sqlstate):
    with pytest.raises(PostgresError, match=message) as refused:
        _rows(
            f"SELECT CAST(CAST('no_such_' || n.nspname AS {reg}) AS oid) "
            "FROM pg_catalog.pg_namespace n WHERE n.oid = 11"
        )
    assert _sqlstate_of(refused.value) == sqlstate


# --- PostgreSQL's name of a constant or a session keyword (cause 7) ----------------------------------


def test_metabase_current_catalog_is_the_database_named_current_catalog():
    """Metabase #4."""
    result = _answer("select current_catalog")
    assert result.column_names == ["current_catalog"]
    assert result.rows == _rows("SELECT current_database()")


def test_metabase_select_1_is_named_question_column():
    """Metabase #2: an unaliased constant of a statement reading no table is ?column?, which the
    engine would name 1."""
    from provisa.compiler.result_shape import pg_named

    assert pg_named("SELECT 1") == 'SELECT 1 AS "?column?"'
    assert pg_named("select current_catalog") == 'SELECT CURRENT_CATALOG AS "current_catalog"'
    assert pg_named("SELECT 1 AS one, 2") == 'SELECT 1 AS one, 2 AS "?column?"'
    assert pg_named("SELECT '1'::pg_catalog.regclass") == (
        "SELECT CAST('1' AS pg_catalog.regclass) AS \"regclass\""
    )


@pytest.mark.parametrize(
    ("sql", "name"),
    [
        ("SELECT stxrelid::pg_catalog.regclass", "stxrelid"),
        ("SELECT '1'::pg_catalog.regclass", "regclass"),
        ("SELECT 'pg_class'::regclass::oid", "oid"),  # the outer cast names a constant's cast
        ("SELECT c.oid::regclass::text", "oid"),  # a column names it through every cast
        ("SELECT CASE WHEN true THEN 1 END::text", "text"),  # CASE is weaker than a cast
        ("SELECT 1", "?column?"),
    ],
)
def test_an_unaliased_cast_is_named_by_postgresqls_rule(sql, name):
    import sqlglot

    from provisa.compiler.result_shape import _pg_column_name

    (select,) = sqlglot.parse_one(sql, read="postgres").selects
    assert _pg_column_name(select) == name


async def test_metabase_select_1_reaches_the_engine_named_question_column():
    """Metabase #2: the statement the engine runs for a read of no table names its constant as
    PostgreSQL does, so the RowDescription the engine's names make says ?column?."""
    from types import SimpleNamespace

    from provisa.compiler.rls import RLSContext
    from provisa.compiler.sql_gen import CompilationContext
    from provisa.compiler.stage2 import build_governance_context
    from provisa.federation.replica_address import ReplicaRoutes
    from provisa.mv.registry import MVRegistry
    from provisa.pgwire._pipeline import _optimize_and_route
    from tests.helpers import unscoped_role

    ctx = CompilationContext()
    gov_ctx = build_governance_context(
        "analyst", RLSContext.empty(), {}, ctx, tables=[], role=unscoped_role("analyst")
    )
    state = SimpleNamespace(
        _active_runtime=lambda: SimpleNamespace(unbound_sources=frozenset()),
        mv_registry=MVRegistry(),
        hot_manager=None,
        api_endpoints={},
        graphql_remote_sources={},
        source_types={},
        source_dialects={},
        source_dsns={},
        source_pools=SimpleNamespace(source_ids=set(), has=lambda _: False),
        replica_routes=ReplicaRoutes(),
        tables=[],
    )
    exec_sql, _decision, _optimized, sources, _labels = await _optimize_and_route(
        "SELECT 1", "SELECT 1", gov_ctx, ctx, state, table_ids=()
    )
    assert not sources
    assert exec_sql == 'SELECT 1 AS "?column?"'
