# Copyright (c) 2026 Kenneth Stott
# Canary: 2e7b9c41-5d8a-4f13-a6e0-9c3b1d7f4a68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A subtype keeps its native semantics (REQ-2171, amended 2026-10-27).

Where a sort, filter, join or operator touches a subtyped column, the routed dialect gives it its
native type; where the dialect has none, a declared emulation answers as the native type would;
otherwise the operation is refused by name, never answered over the column's text form. A
column read as a value keeps its text form. Each emulation is checked here against PostgreSQL's
native answer: the emulation runs in DuckDB, the native operator in PostgreSQL."""

# Requirements: REQ-2171

from __future__ import annotations

import duckdb
import pytest
import sqlglot

from provisa.compiler.native_subtype import mark_subtype_operations
from provisa.transpiler.native_subtype import SubtypeOperationRefused
from provisa.transpiler.transpile import transpile

_COLUMNS = {
    ("net", "hosts"): {
        "ip": "inet",
        "mac": "macaddr",
        "span": "int4range",
        "area": "geometry",
        "words": "tsvector",
        "note": "xml",
    }
}


def _marked(where: str, order: str = "") -> str:
    return mark_subtype_operations(
        f"SELECT h.ip, h.mac FROM net.hosts AS h WHERE {where}{order}", _COLUMNS
    )


# --- what is marked ------------------------------------------------------------------------------


def test_a_sort_filter_or_operator_on_a_subtyped_column_is_marked():
    sql = _marked(
        "h.ip <<= '10.0.0.0/8' AND h.mac = '08:00:2b:01:02:03' AND h.span && '[1,5)'"
        " AND ST_Contains(h.area, 'POINT(1 1)') AND h.words @@ to_tsquery('a')",
        " ORDER BY h.ip",
    )
    tree = sqlglot.parse_one(sql, read="postgres")
    markers = [
        (n.expressions[0].sql(), n.expressions[1].name)
        for n in tree.find_all(sqlglot.exp.Anonymous)
        if str(n.this).lower() == "provisa_subtype"
    ]
    assert sorted(markers) == sorted(
        [
            ("h.ip", "inet"),
            ("h.mac", "macaddr"),
            ("h.span", "int4range"),
            ("h.area", "geometry"),
            ("h.words", "tsvector"),
            ("h.ip", "inet"),
        ]
    )


def test_a_subtyped_column_read_as_a_value_keeps_its_text_form():
    # The select list, a function's argument, and a subtype whose base semantics are its own.
    sql = _marked("UPPER(h.mac) = 'X' AND h.note = '<a/>'")
    assert "provisa_subtype" not in sql.lower()


# --- the routed dialect's native type ------------------------------------------------------------


def test_postgresql_and_duckdb_give_the_column_its_native_type():
    sql = _marked("h.ip = '10.0.0.1'", " ORDER BY h.ip")
    assert transpile(sql, "postgres").endswith(
        "WHERE CAST(h.ip AS INET) = CAST('10.0.0.1' AS INET) ORDER BY CAST(h.ip AS INET)"
    )
    assert "CAST(h.ip AS INET)" in transpile(sql, "duckdb")
    # The select list is the text form still.
    assert transpile(sql, "postgres").startswith("SELECT h.ip, h.mac FROM")


def test_a_statement_at_a_source_with_the_type_runs_the_types_own_operator_there():
    # A single-source statement is routed to its source, whose dialect is then the routed one.
    assert "CAST(h.span AS INT4RANGE) && '[1,5)'" in transpile(
        _marked("h.span && '[1,5)'"), "postgres"
    )
    assert "@@" in transpile(_marked("h.words @@ to_tsquery('a')"), "postgres")


def test_a_spatial_predicate_gives_the_column_and_its_wkt_the_native_type():
    sql = _marked("ST_Contains(h.area, 'POINT(1 1)')")
    assert "ST_CONTAINS(ST_GEOMFROMTEXT(h.area), ST_GEOMFROMTEXT('POINT(1 1)'))" in transpile(
        sql, "duckdb"
    )
    assert "ST_GEOMETRYFROMTEXT(h.area)" in transpile(sql, "trino")


# --- refused by name where the dialect has neither ---------------------------------------------


@pytest.mark.parametrize(
    ("where", "order", "dialect", "subtype", "operation"),
    [
        ("h.ip = '10.0.0.1'", "", "clickhouse", "inet", "="),
        ("TRUE", " ORDER BY h.span", "duckdb", "int4range", "ORDER BY"),
        ("ST_Contains(h.area, 'POINT(1 1)')", "", "clickhouse", "geometry", "ST_CONTAINS"),
        ("h.words @@ to_tsquery('a')", "", "duckdb", "tsvector", "@@"),
    ],
)
def test_an_operation_the_dialect_cannot_answer_natively_is_refused_by_name(
    where, order, dialect, subtype, operation
):
    with pytest.raises(SubtypeOperationRefused) as refused:
        transpile(_marked(where, order), dialect)
    assert refused.value.code == "query.subtype_operation_unsupported"
    assert refused.value.params == {"subtype": subtype, "operation": operation, "dialect": dialect}


# --- each emulation answers as the native type does ----------------------------------------------

_RANGES = ["[1,5)", "[5,10)", "(1,5]", "[3,3]", "empty", "[,4)", "[4,)", "(,)", "[2,8)"]
_ELEMENTS = [0, 1, 3, 5, 9]


def _duckdb_answer(where: str) -> bool:
    """The emulated predicate, as Provisa writes it for DuckDB, run in DuckDB over one row."""
    statement = transpile(_marked(where), "duckdb")
    tree = sqlglot.parse_one(statement, read="duckdb")
    predicate = tree.args["where"].this.sql(dialect="duckdb")
    return duckdb.sql(f"SELECT {predicate} FROM (SELECT * FROM _row) AS h").fetchone()[0]


@pytest.fixture
def pg(control_plane):
    return control_plane.platform


@pytest.mark.asyncio
async def test_the_range_emulation_answers_as_postgresql_does(pg):
    """Overlap and containment from a range's bounds and their inclusivity, against
    PostgreSQL's own int4range operators over every pair."""
    cases = [(a, op, b) for a in _RANGES for b in _RANGES for op in ("&&", "@>", "<@")]
    cases += [(a, "@>", e) for a in _RANGES for e in _ELEMENTS]
    async with pg.acquire() as conn:
        for a, op, b in cases:
            other = f"'{b}'" if isinstance(b, str) else str(b)
            native = await conn.fetchval(
                f"SELECT '{a}'::int4range {op} {other}{'::int4range' if isinstance(b, str) else ''}"
            )
            duckdb.sql(f"CREATE OR REPLACE TABLE _row AS SELECT '{a}' AS span")
            assert _duckdb_answer(f"h.span {op} {other}") == native, (a, op, b)


@pytest.mark.asyncio
async def test_the_mac_emulation_compares_and_orders_as_postgresql_does(pg):
    macs = ["08:00:2b:01:02:03", "08-00-2B-01-02-03", "0800.2b01.0203", "08:00:2b:01:02:04"]
    async with pg.acquire() as conn:
        for a in macs:
            for b in macs:
                for op in ("=", "<", ">="):
                    native = await conn.fetchval(f"SELECT '{a}'::macaddr {op} '{b}'::macaddr")
                    duckdb.sql(f"CREATE OR REPLACE TABLE _row AS SELECT '{a}' AS mac")
                    assert _duckdb_answer(f"h.mac {op} '{b}'") == native, (a, op, b)


@pytest.mark.asyncio
async def test_duckdbs_inet_orders_and_contains_as_postgresqls_does(pg):
    """DuckDB's own inet, as Provisa casts to it: '10.0.0.9' < '10.0.0.10' (as text it is not)."""
    addresses = ["10.0.0.9", "10.0.0.10", "10.0.0.1/24", "10.0.0.0/32", "10.1.0.0/16", "::1"]
    async with pg.acquire() as conn:
        for a in addresses:
            for b in addresses:
                for op in ("<", "=", "<<", "<<=", ">>", ">>=", "&&"):
                    native = await conn.fetchval(f"SELECT '{a}'::inet {op} '{b}'::inet")
                    duckdb.sql(f"CREATE OR REPLACE TABLE _row AS SELECT '{a}' AS ip")
                    assert _duckdb_answer(f"h.ip {op} '{b}'") == native, (a, op, b)
    assert "10.0.0.9" < "z" and not ("10.0.0.9" < "10.0.0.10")  # the text form's own order


def test_trinos_inet_emulation_orders_by_family_network_mask_then_address():
    """Run on Trino in the engine lane; here, its shape: the key and containment are built from
    Trino's own address functions, never from the text."""
    written = transpile(_marked("h.ip <<= '10.0.0.0/8'", " ORDER BY h.ip"), "trino")
    # Trino 481 has no prefix function: the network is the address's 128 bits, each 64-bit half
    # masked, from its IPADDRESS's bytes.
    assert "IP_PREFIX" not in written and "IP_SUBNET_MIN" not in written
    assert "FROM_BIG_ENDIAN_64(SUBSTR(CAST(CAST(SPLIT_PART(h.ip, '/', 1) AS IPADDRESS)" in written
    assert "BITWISE_AND(" in written and "BITWISE_LEFT_SHIFT(CAST(-1 AS BIGINT)" in written


# --- the registered subtype reaches the physical statement --------------------------------------


def test_the_physical_rewrite_marks_a_registered_subtype_column():
    from provisa.compiler.context import build_context
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput
    from provisa.compiler.sql_rewrite import rewrite_semantic_to_catalog_physical

    si = SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "pg",
                "domain_id": "net",
                "schema_name": "public",
                "table_name": "hosts",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin"]},
                    {"column_name": "ip", "visible_to": ["admin"], "subtype": "inet"},
                    {"column_name": "doc", "visible_to": ["admin"], "subtype": "xml"},
                ],
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
        ],
        relationships=[],
        column_types={
            1: [
                ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
                ColumnMetadata("ip", "text", True, subtype="inet"),
                ColumnMetadata("doc", "text", True, subtype="xml"),
            ]
        },
        naming_rules=[],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "net", "description": "Network"}],
    )
    ctx = build_context(si)
    # xml has its base's semantics: nothing to mark for it.
    assert ctx.subtype_columns == {("public", "hosts"): {"ip": "inet"}}
    physical = rewrite_semantic_to_catalog_physical(
        'SELECT "h"."id" FROM "public"."hosts" AS "h" WHERE "h"."ip" = \'10.0.0.1\' ORDER BY "h"."ip"',
        ctx,
    )
    assert physical.lower().count("provisa_subtype(") == 2


# --- a subtyped column lands in its own form, or is left out and refused by name ----------------

from types import SimpleNamespace  # noqa: E402


def _landing_source(dialect, kind="postgresql"):
    return SimpleNamespace(
        id="pg", type=SimpleNamespace(value=kind), dialect=dialect, change_signal="ttl"
    )


def _landing_table(*columns):
    return SimpleNamespace(
        source_id="pg",
        schema_name="net",
        table_name="hosts",
        change_signal=None,
        watermark_column=None,
        probe_type=None,
        landing_filter=None,
        live=None,
        columns=[
            SimpleNamespace(name=n, data_type=t, subtype=s, is_primary_key=False, embedding=False)
            for n, t, s in columns
        ],
    )


_HOSTS_TABLE = (("id", "integer", None), ("ip", "text", "inet"), ("span", "text", "int4range"))


def test_a_column_no_reader_can_land_in_its_own_form_is_left_out_and_named():
    from provisa.federation.data_replicator import COLUMN_NOT_LANDED, not_landed_notes
    from provisa.federation.residency import resolve_landing_args

    # A PostgreSQL source on the DuckDB engine: the engine has a form for inet, not for a range.
    args = resolve_landing_args(
        _landing_source("postgres"), _landing_table(*_HOSTS_TABLE), platform="duckdb"
    )
    assert args.columns == [("id", "integer"), ("ip", "text")]
    assert args.not_landed == (("span", "int4range", "duckdb"),)
    (note,) = not_landed_notes(args.not_landed)
    assert (note.code, note.params) == (
        COLUMN_NOT_LANDED,
        {"column": "span", "subtype": "int4range", "reader": "duckdb"},
    )
    # On the PostgreSQL engine every reader has a form: every column lands.
    whole = resolve_landing_args(
        _landing_source("postgres"), _landing_table(*_HOSTS_TABLE), platform="postgres"
    )
    assert [n for n, _ in whole.columns] == ["id", "ip", "span"] and whole.not_landed == ()


def test_the_landing_reads_each_subtyped_column_in_its_own_form():
    from provisa.transpiler.native_json import source_projection

    table = _landing_table(
        ("id", "integer", None),
        ("ip", "text", "inet"),
        ("area", "text", "geometry"),
        ("span", "text", "int4range"),
    )
    assert source_projection(table.columns, "postgres") == (
        '"id", CAST("ip" AS TEXT) AS "ip", ST_AsText("area") AS "area", '
        'CAST("span" AS TEXT) AS "span"'
    )
    # Never hex WKB as a geometry: a column left out is not read at all.
    assert source_projection(table.columns, "duckdb", frozenset({"span"})) == (
        '"id", CAST("ip" AS VARCHAR) AS "ip", ST_AsText("area") AS "area"'
    )


def test_an_adapter_source_lands_only_what_it_delivers_in_the_subtypes_form():
    from provisa.federation.residency import resolve_landing_args

    es = _landing_source(None, kind="elasticsearch")
    args = resolve_landing_args(
        es,
        _landing_table(("ip", "text", "inet"), ("where", "text", "geography")),
        platform="duckdb",
    )
    assert args.columns == [("ip", "text")]
    assert args.not_landed == (("where", "geography", "elasticsearch"),)


def test_a_read_of_a_column_not_landed_is_refused_by_name():
    from provisa.federation.replica_address import (
        ColumnNotLanded,
        ReplicaRoute,
        ReplicaRoutes,
        address_replicas,
    )

    key = ("pg", "net", "hosts")
    routes = ReplicaRoutes(
        routes={
            key: ReplicaRoute(
                source_id="pg",
                table_name="hosts",
                target=("store", "replicas", "pg__net__hosts"),
                not_landed=(("span", "int4range", "duckdb"),),
            )
        }
    )
    # The columns it holds read from it.
    assert "pg__net__hosts" in address_replicas(
        'SELECT "h"."ip" FROM "pg"."net"."hosts" AS "h"', routes
    )
    with pytest.raises(ColumnNotLanded) as refused:
        address_replicas('SELECT "h"."span" FROM "pg"."net"."hosts" AS "h"', routes)
    assert refused.value.code == "query.column_not_landed"
    assert refused.value.params == {"column": "span", "subtype": "int4range", "dialect": "duckdb"}
    with pytest.raises(ColumnNotLanded):
        address_replicas('SELECT span FROM "pg"."net"."hosts"', routes)
