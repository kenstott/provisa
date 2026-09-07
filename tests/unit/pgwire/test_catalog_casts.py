# Copyright (c) 2026 Kenneth Stott
# Canary: d4e5f6a7-b8c9-0123-defa-123456789013
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PG-type cast rewriting for the DuckDB-backed catalog intercept.

DataGrip / psql catalog probes carry PG-only casts (``::oid``, ``::xid``,
``::regclass``, ``::name`` and array forms). DuckDB has none of those types, so
``_rewrite_for_duckdb`` must rewrite each cast into something DuckDB accepts
*without* dropping the operand's value — a value-losing rewrite silently breaks
predicates like ``relnamespace = 2215::oid``.
"""

from __future__ import annotations

import duckdb
import pytest

from provisa.pgwire.catalog_rewrite import _rewrite_for_duckdb


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    yield c
    c.close()


def _scalar(con, sql: str):
    return con.execute(_rewrite_for_duckdb(sql)).fetchone()[0]


def test_oid_literal_value_preserved(con):
    # Regression: `2215::oid` previously collapsed to literal 0.
    assert _scalar(con, "SELECT 2215::oid AS x") == 2215


def test_oid_predicate_preserved():
    rewritten = _rewrite_for_duckdb("SELECT * FROM pg_class WHERE relnamespace = 2215::oid")
    assert "= 0" not in rewritten
    assert "2215" in rewritten


@pytest.mark.parametrize("t", ["oid", "xid", "tid", "cid"])
def test_system_id_casts_keep_value(con, t):
    assert _scalar(con, f"SELECT 42::{t} AS x") == 42


def test_nested_varchar_xid_cast_executes(con):
    # DataGrip emits age(0::varchar::xid); the nested casts must run in DuckDB.
    assert _scalar(con, "SELECT 0::varchar::xid AS x") == 0


def test_oid_array_cast_executes(con):
    assert _scalar(con, "SELECT '{16395}'::oid[] AS arr") == [16395]


@pytest.mark.parametrize(
    "regtype",
    [
        "regclass",
        "regtype",
        "regproc",
        "regprocedure",
        "regoper",
        "regoperator",
        "regconfig",
        "regdictionary",
        "regrole",
        "regnamespace",
    ],
)
def test_reg_casts_stripped_to_operand(con, regtype):
    # reg* casts drop to the bare operand (the numeric oid / identifier value).
    assert _scalar(con, f"SELECT n.oid::{regtype} AS rc FROM (SELECT 5 AS oid) n") == 5


def test_name_cast_becomes_varchar(con):
    assert _scalar(con, "SELECT 'public'::name AS n") == "public"


def test_qualified_regclass_literal_shortened():
    # pg_description.classoid stores the short relation name ('pg_class'), so
    # DataGrip's `classoid = 'pg_catalog.pg_class'::regclass` filter must map the
    # schema-qualified literal to its last component or every comment drops.
    out = _rewrite_for_duckdb(
        "SELECT description FROM pg_description WHERE classoid = 'pg_catalog.pg_class'::regclass"
    )
    assert "'pg_class'" in out
    assert "pg_catalog.pg_class" not in out


def test_unqualified_regclass_literal_unchanged(con):
    assert _scalar(con, "SELECT 'pg_class'::regclass AS c") == "pg_class"


def test_regclass_oid_chain_maps_to_class_oid(con):
    # DataGrip's RetrieveExtensionMembers filters
    # `refclassid = 'pg_extension'::regclass::oid` against INTEGER refclassid.
    # The chained cast previously crashed sqlglot's cast builder, the rewriter
    # fell back to the raw SQL, and DuckDB failed with "Table with name
    # pg_extension does not exist".
    assert _scalar(con, "SELECT 'pg_extension'::regclass::oid AS c") == 3079
    assert _scalar(con, "SELECT 'pg_catalog.pg_class'::regclass::oid AS c") == 1259


def test_regclass_oid_chain_rewrites_table_refs():
    out = _rewrite_for_duckdb(
        "select E.oid as extension_id, D.objid as member_id\n"
        "from pg_extension E\n"
        "     join pg_depend D on E.oid = D.refobjid and\n"
        "                         D.refclassid = 'pg_extension'::regclass::oid\n"
        "where D.deptype = 'e'\n"
        "order by extension_id"
    )
    assert "_pg_extension" in out
    assert "_pg_depend" in out
    assert "3079" in out


def test_pg_available_extension_versions_maps_to_pg_extension():
    # DataGrip's RetrieveExtensions joins pg_available_extension_versions() for
    # (name, version); DuckDB has no such TVF, so it maps to _pg_extension.
    out = _rewrite_for_duckdb(
        "select name, array_agg(version) from pg_available_extension_versions() group by name"
    )
    assert "pg_available_extension_versions()" not in out
    assert "_pg_extension" in out


def test_format_type_strips_schema_qualifier_from_embedded_column():
    # Regression: sqlglot's transform() visits parents before children, so the
    # format_type/obj_description/col_description handlers previously spliced
    # their *untransformed* argument straight into a new subquery, leaving the
    # `pg_catalog.` qualifier in place. DuckDB's binder rejected the result
    # with "Referenced table 'pg_catalog.pg_attribute' not found!" — this is
    # exactly the SQL SQLAlchemy's PG reflection issues.
    out = _rewrite_for_duckdb(
        "SELECT format_type(pg_catalog.pg_attribute.atttypid, null) AS x "
        "FROM pg_catalog.pg_attribute"
    )
    assert "pg_catalog.pg_attribute" not in out
    assert "_pg_attribute AS pg_attribute" in out


def test_obj_description_strips_schema_qualifier_from_embedded_column():
    out = _rewrite_for_duckdb(
        "SELECT obj_description(pg_catalog.pg_class.oid) AS x FROM pg_catalog.pg_class"
    )
    assert "pg_catalog.pg_class" not in out


def test_col_description_strips_schema_qualifier_from_embedded_columns():
    out = _rewrite_for_duckdb(
        "SELECT col_description(pg_catalog.pg_class.oid, pg_catalog.pg_attribute.attnum) AS x "
        "FROM pg_catalog.pg_class, pg_catalog.pg_attribute"
    )
    assert "pg_catalog.pg_class" not in out
    assert "pg_catalog.pg_attribute" not in out


def test_pg_get_serial_sequence_nested_in_cast_chain_is_nulled():
    # Regression: SQLAlchemy's PG column reflection wraps
    # pg_get_serial_sequence(...) in `::regclass::oid` inside a WHERE predicate.
    # _rewrite_pg_cast's oid-cast branch spliced the (untransformed) inner
    # expression into a fresh CAST(... AS BIGINT) instead of running it back
    # through the top-level rewrite, so the call's own `pg_get_` handling never
    # fired and it reached DuckDB verbatim: "Scalar Function with name
    # pg_get_serial_sequence does not exist!"
    out = _rewrite_for_duckdb(
        "SELECT * FROM pg_catalog.pg_sequence WHERE pg_catalog.pg_sequence.seqrelid = "
        "CAST(CAST(pg_catalog.pg_get_serial_sequence("
        "CAST(CAST(pg_catalog.pg_attribute.attrelid AS REGCLASS) AS TEXT), "
        "pg_catalog.pg_attribute.attname) AS REGCLASS) AS OID)"
    )
    assert "pg_get_serial_sequence" not in out
    assert "CAST(NULL AS BIGINT)" in out


def test_format_type_maps_internal_typname_to_display_name():
    # Regression: _pg_type.typname stores PG's internal names (int4, bool,
    # varchar, ...), but SQLAlchemy's PG dialect parses format_type()'s
    # *display* name (integer, boolean, character varying, ...) against
    # ischema_names and silently falls back to NullType for anything else —
    # breaking DDL generation for every reflected column ("Can't generate DDL
    # for NullType()").
    c = duckdb.connect()
    c.execute("CREATE TABLE _pg_type (oid INTEGER, typname VARCHAR)")
    c.execute("INSERT INTO _pg_type VALUES (23, 'int4'), (16, 'bool'), (1043, 'varchar')")
    assert _scalar(c, "SELECT format_type(23, null) AS x") == "integer"
    assert _scalar(c, "SELECT format_type(16, null) AS x") == "boolean"
    assert _scalar(c, "SELECT format_type(1043, null) AS x") == "character varying"
    c.close()


def test_json_build_object_maps_to_json_object(con):
    # DuckDB has no json_build_object; SQLAlchemy's identity-column reflection
    # query emits it, and sqlglot's duckdb generator does not translate the
    # Anonymous call, so it reached DuckDB verbatim: "Scalar Function with
    # name json_build_object does not exist! Did you mean 'json_object'?"
    out = _rewrite_for_duckdb("SELECT json_build_object('a', 1) AS j")
    assert "json_build_object" not in out.lower()
    assert con.execute(out).fetchone()[0] == '{"a":1}'
