# Copyright (c) 2026 Kenneth Stott
# Canary: 5b2e8d14-7c3a-4f69-a0d1-3e9f6b8c2a75
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""IR subtypes (REQ-2171).

Some IR types are subtypes of another: inet, cidr, macaddr, xml and point of text, oid of bigint,
money of numeric. Registration always records the subtype when a source's native type has one
(a Postgres inet, a Trino ipaddress, a ClickHouse IPv4 registers as inet). The column is stored as
its base type carrying the subtype, so every surface that needs only the base type reads the base;
pgwire and its catalog report the subtype's PostgreSQL type."""

# Requirements: REQ-2171

from __future__ import annotations

import contextlib
import decimal
import ipaddress
import struct
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, select

from buenavista.postgres import BVTYPE_TO_PGTYPE
from provisa.api.admin.introspect import _pg_column_typing
from provisa.compiler.introspect import ColumnMetadata, introspect_tables
from provisa.compiler.result_shape import derive_result_shape
from provisa.core import secrets_store
from provisa.core.ir_types import SUBTYPES, column_typing, to_ir, to_physical
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, sources, table_columns
from provisa.pgwire.catalog_data import _PG_OID_ATTR_META, _TYPEINFO, pg_type_rows
from provisa.pgwire.catalog_populate import _physical_to_pg_name, _physical_to_pg_oid
from provisa.pgwire.server import _sql_type_to_bvtype

# name: (base IR type, PostgreSQL type OID): one subtype per distinct PostgreSQL type.
_FIRST_SET = {
    "inet": ("text", 869),
    "cidr": ("text", 650),
    "macaddr": ("text", 829),
    "macaddr8": ("text", 774),
    "xml": ("text", 142),
    "oid": ("bigint", 26),
    "money": ("numeric", 790),
    "point": ("text", 600),
    "line": ("text", 628),
    "lseg": ("text", 601),
    "box": ("text", 603),
    "path": ("text", 602),
    "polygon": ("text", 604),
    "circle": ("text", 718),
    "geometry": ("text", 8310),
    "geography": ("text", 8311),
    "int4range": ("text", 3904),
    "int8range": ("text", 3926),
    "numrange": ("text", 3906),
    "tsrange": ("text", 3908),
    "tstzrange": ("text", 3910),
    "daterange": ("text", 3912),
    "bit": ("text", 1560),
    "varbit": ("text", 1562),
    "tsvector": ("text", 3614),
}


def test_each_subtype_is_declared_once_with_its_base_and_postgresql_type():
    assert {name: (to_ir(s.base), s.pg_oid) for name, s in SUBTYPES.items()} == _FIRST_SET


@pytest.mark.parametrize("name", sorted(_FIRST_SET))
def test_a_subtype_is_its_base_type_to_every_surface_that_needs_only_the_base(name):
    base = _FIRST_SET[name][0]
    assert to_ir(name) == base
    assert to_physical(name, "postgresql") == to_physical(SUBTYPES[name].base, "postgresql")


# --- registration records the subtype -----------------------------------------------------------


@pytest.mark.parametrize(
    ("native", "platform", "subtype"),
    [
        ("ipaddress", "trino", "inet"),
        ("IPv4", "clickhouse", "inet"),
        ("IPv6", "clickhouse", "inet"),
        ("INET", "duckdb", "inet"),
        ("money", "sqlserver", "money"),
        ("smallmoney", "sqlserver", "money"),
        ("xml", "sqlserver", "xml"),
        ("geometry", "sqlserver", "geometry"),
        ("geography", "sqlserver", "geography"),
        ("xmltype", "oracle", "xml"),
        ("point", "mysql", "geometry"),
        ("geometry", "mysql", "geometry"),
        ("linestring", "mysql", "geometry"),
        ("polygon", "mysql", "geometry"),
        ("multipolygon", "mariadb", "geometry"),
        ("bit(8)", "mysql", "bit"),
        ("bit varying(12)", "postgresql", "varbit"),
    ],
)
def test_a_native_type_equivalent_to_a_subtype_registers_as_it(native, platform, subtype):
    assert column_typing(native, platform).data_type == subtype
    # Everything that maps the native type to the IR without registering it reads the base.
    assert to_ir(native, platform) == to_ir(subtype)


_POSTGIS = {"geometry", "geography"}


@pytest.mark.parametrize("kind", ["postgresql", "cockroachdb", "yugabytedb"])
@pytest.mark.parametrize("native", sorted(set(_FIRST_SET) - _POSTGIS))
def test_a_postgres_wire_column_of_a_subtype_registers_as_it(kind, native):
    data_type = "bit varying" if native == "varbit" else native
    assert _pg_column_typing(kind, data_type, native, native, "b").data_type == native


@pytest.mark.parametrize("native", sorted(_POSTGIS))
def test_a_postgis_column_registers_as_its_subtype(native):
    # information_schema names an extension's type USER-DEFINED, and its udt_name.
    assert _pg_column_typing("postgresql", "USER-DEFINED", native, native, "b").data_type == native


def test_a_platforms_own_meaning_of_a_subtypes_name_is_kept():
    # SQL Server's bit is a boolean, not a bit string.
    assert column_typing("bit", "sqlserver").data_type == "boolean"


@pytest.mark.parametrize(
    ("es_type", "registered"),
    [
        ("ip", "inet"),
        ("geo_point", "geography"),
        ("geo_shape", "geography"),
        ("point", "geometry"),
        ("shape", "geometry"),
        ("integer_range", "int4range"),
        ("long_range", "int8range"),
        ("float_range", "numrange"),
        ("double_range", "numrange"),
        ("date_range", "tstzrange"),
        ("ip_range", "varchar"),
    ],
)
def test_an_elasticsearch_field_registers_as_its_subtype(es_type, registered):
    from provisa.elasticsearch.source import discover_schema

    (column,) = discover_schema({"f": {"type": es_type}})
    assert column["type"].lower() == registered


@pytest.mark.parametrize(
    ("cql_type", "registered"),
    [("inet", "inet"), ("varint", "numeric(38,0)"), ("duration", "interval")],
)
def test_a_cassandra_column_registers_as_its_ir_type(cql_type, registered):
    from provisa.cassandra.source import discover_schema

    (column,) = discover_schema(
        {"columns": [{"name": "c", "type": cql_type}], "partition_keys": [], "clustering_keys": []}
    )
    assert column["type"] == registered


@pytest.mark.parametrize(
    ("native", "platform", "registered"),
    [("year", "mysql", "integer")],
)
def test_a_source_type_with_no_subtype_maps_onto_a_base(native, platform, registered):
    assert to_ir(native, platform) == registered


# --- the store keeps the base type and the subtype ----------------------------------------------


@pytest.fixture(autouse=True)
def no_vault(monkeypatch):
    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)


@pytest.fixture
async def org_plane(control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="net"))
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
    return db


def _hosts(*columns: tuple[str, str]) -> Table:
    return Table(
        source_id="pg",
        domain_id="net",
        schema_name="public",
        table_name="hosts",
        columns=[Column(name=n, data_type=t, visible_to=["admin"]) for n, t in columns],
    )


async def _stored(db) -> dict[str, tuple[str, str | None]]:
    async with db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(
                    table_columns.c.column_name, table_columns.c.data_type, table_columns.c.subtype
                )
            )
        ).fetchall()
    return {r.column_name: (r.data_type, r.subtype) for r in rows}


@pytest.mark.asyncio
async def test_a_column_registered_as_a_subtype_is_stored_as_its_base_carrying_the_subtype(
    org_plane,
):
    async with org_plane.acquire() as conn:
        await table_repo.upsert(
            conn, _hosts(("id", "integer"), ("ip", "inet"), ("fee", "money"), ("row", "oid"))
        )
    assert await _stored(org_plane) == {
        "id": ("integer", None),
        "ip": ("text", "inet"),
        "fee": (SUBTYPES["money"].base, "money"),
        "row": ("bigint", "oid"),
    }


@pytest.mark.asyncio
async def test_resaving_a_column_as_its_base_keeps_the_subtype_and_a_new_type_drops_it(org_plane):
    async with org_plane.acquire() as conn:
        await table_repo.upsert(conn, _hosts(("ip", "inet"), ("mac", "macaddr")))
        # An edit of the table restates each column's stored type, the base.
        await table_repo.upsert(conn, _hosts(("ip", "text"), ("mac", "integer")))
    assert await _stored(org_plane) == {"ip": ("text", "inet"), "mac": ("integer", None)}


@pytest.mark.asyncio
async def test_the_tables_a_server_builds_from_carry_each_columns_subtype(org_plane):
    from provisa.api.admin.db_queries import fetch_tables

    async with org_plane.acquire() as conn:
        await table_repo.upsert(conn, _hosts(("id", "integer"), ("ip", "inet")))
        tables = await fetch_tables(conn)
    (hosts,) = [t for t in tables if t["table_name"] == "hosts"]
    assert {c["column_name"]: c["subtype"] for c in hosts["columns"]} == {"id": None, "ip": "inet"}
    (metadata,) = introspect_tables(None, [hosts], {"pg": {"type": "postgresql"}}).values()
    assert [(m.column_name, m.data_type, m.subtype) for m in metadata] == [
        ("id", "integer", None),
        ("ip", "text", "inet"),
    ]


# --- the pgwire catalog reports the PostgreSQL type ---------------------------------------------


@pytest.mark.parametrize("name", sorted(_FIRST_SET))
def test_the_catalog_reports_a_subtype_as_its_postgresql_type(name):
    oid = _FIRST_SET[name][1]
    assert _physical_to_pg_oid(name) == oid
    assert _physical_to_pg_name(name) == name
    assert {row[0]: row[1] for row in pg_type_rows()}[oid] == name
    assert oid in _PG_OID_ATTR_META
    assert _TYPEINFO[oid][1] == name


def _catalog_ctx():
    meta = SimpleNamespace(
        table_id=1,
        domain_id="net",
        schema_name="public",
        table_name="hosts",
        display_name="hosts",
        field_name="net__hosts",
    )
    return SimpleNamespace(
        tables={"hosts": meta},
        physical_to_sql={},
        virtual_columns={},
        row_virtual_columns={},
        columns_outside_relation=set(),
        nested_columns={},
    )


_HOSTS = {
    1: [
        ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
        ColumnMetadata(column_name="ip", data_type="text", is_nullable=True, subtype="inet"),
        ColumnMetadata(column_name="name", data_type="text", is_nullable=True),
    ]
}


def test_the_catalog_lists_a_subtyped_column_as_the_subtype():
    from provisa.pgwire.catalog_populate import _build_catalog_index

    idx = _build_catalog_index(_catalog_ctx(), _HOSTS)
    assert [(c[1], c[2]) for c in idx.all_cols] == [
        ("id", "integer"),
        ("ip", "inet"),
        ("name", "text"),
    ]


# --- pgwire results report the PostgreSQL type --------------------------------------------------


def test_a_statement_reading_a_subtyped_column_is_described_as_the_subtype():
    tables = {"net.hosts": 1, "hosts": 1}
    ctx = _catalog_ctx()
    assert derive_result_shape("SELECT id, ip FROM net.hosts", tables, ctx, _HOSTS) == [
        ("id", "integer"),
        ("ip", "inet"),
    ]
    joined = derive_result_shape(
        "SELECT h.ip, upper(h.ip) AS shout FROM net.hosts h JOIN net.hosts g ON h.id = g.id",
        tables,
        ctx,
        _HOSTS,
    )
    # A bare column is the subtype; an expression over it is typed by the base, as text.
    assert joined[0] == ("ip", "inet")
    assert joined[1][0] == "shout" and joined[1][1].lower() in {"text", "varchar"}


@pytest.mark.parametrize("name", sorted(_FIRST_SET))
def test_pgwire_sends_a_subtype_with_its_postgresql_type_oid(name):
    assert BVTYPE_TO_PGTYPE[_sql_type_to_bvtype(name)][0] == _FIRST_SET[name][1]


def _text(name: str, value):
    return BVTYPE_TO_PGTYPE[_sql_type_to_bvtype(name)][1](value)


def _binary(name: str, value):
    return BVTYPE_TO_PGTYPE[_sql_type_to_bvtype(name)][2](value)


def test_pgwire_sends_each_subtype_in_postgresqls_text_form():
    assert _text("inet", "10.0.0.1") == "10.0.0.1"
    assert _text("inet", ipaddress.ip_interface("10.0.0.1/32")) == "10.0.0.1"
    assert _text("inet", ipaddress.ip_interface("10.0.0.1/24")) == "10.0.0.1/24"
    assert _text("cidr", ipaddress.ip_network("10.0.0.0/8")) == "10.0.0.0/8"
    assert _text("macaddr", "08:00:2b:01:02:03") == "08:00:2b:01:02:03"
    assert _text("xml", "<a>1</a>") == "<a>1</a>"
    assert _text("point", "(1,2)") == "(1,2)"
    assert _text("point", (1.0, 2.5)) == "(1,2.5)"
    assert _text("oid", 42) == "42"
    # money in PostgreSQL's own form (lc_monetary C): a dollar sign, thousands, two decimals.
    assert _text("money", decimal.Decimal("1234.5")) == "$1,234.50"
    assert _text("money", decimal.Decimal("-1234.567")) == "-$1,234.57"
    assert _text("money", "$1,234.50") == "$1,234.50"


def test_pgwire_sends_each_subtype_in_postgresqls_binary_form():
    assert _binary("inet", "10.0.0.1") == bytes([2, 32, 0, 4, 10, 0, 0, 1])
    assert _binary("inet", "10.0.0.1/24") == bytes([2, 24, 0, 4, 10, 0, 0, 1])
    assert _binary("cidr", "10.0.0.0/8") == bytes([2, 8, 1, 4, 10, 0, 0, 0])
    assert _binary("inet", "::1") == bytes([3, 128, 0, 16]) + bytes(15) + b"\x01"
    assert _binary("macaddr", "08:00:2b:01:02:03") == bytes([8, 0, 0x2B, 1, 2, 3])
    assert _binary("xml", "<a>1</a>") == b"<a>1</a>"
    assert _binary("point", "(1,2.5)") == struct.pack("!dd", 1.0, 2.5)
    assert _binary("oid", 42) == struct.pack("!I", 42)
    assert _binary("money", decimal.Decimal("1234.5")) == struct.pack("!q", 123450)
    assert _binary("money", "-$1,234.57") == struct.pack("!q", -123457)


def test_an_undescribed_statements_subtyped_columns_are_sent_as_their_postgresql_type():
    """The simple protocol types columns by what the engine returned: an engine holding inet as
    text answers varchar, and the column still goes out as inet."""
    from provisa.executor.result import QueryResult
    from provisa.pgwire.server import ProvisaQueryResult

    engine = QueryResult(
        rows=[(1, "10.0.0.1")], column_names=["id", "ip"], column_types=["integer", "varchar"]
    )
    result = ProvisaQueryResult(engine, "SELECT id, ip FROM net.hosts", read_shape=_SHAPE)
    assert [BVTYPE_TO_PGTYPE[result.column(i)[1]][0] for i in range(2)] == [23, 869]
    with pytest.raises(RuntimeError, match="returned 2"):
        ProvisaQueryResult(engine, "SELECT id, ip FROM net.hosts", read_shape=_SHAPE[:1])


_SHAPE = [("id", "integer"), ("ip", "inet")]


def _kept(table_map):
    from provisa.pgwire._pipeline import _kept_result_shape

    state = SimpleNamespace(schema_build_cache={"column_types": {**_HOSTS, 2: _HOSTS[1][:1]}})
    return _kept_result_shape({}, "SELECT id, ip FROM net.hosts", table_map, _catalog_ctx(), state)


def test_only_a_statement_reading_a_subtyped_column_derives_its_shape_for_the_wire():
    assert _kept({"net.hosts": 1}).subtyped() == _SHAPE
    assert _kept({"net.ids": 2}).subtyped() is None


@pytest.mark.asyncio
async def test_a_money_column_registers_as_money_and_is_never_sampled_as_an_unsized_numeric():
    """PostgreSQL has no scale(money): sampling a money column as an unsized numeric failed the
    pgwire conformance seed (run 40559811408)."""
    from provisa.api.admin.introspect import native_column_typings

    class _Pool:
        statements: list[str] = []

        def has(self, _source_id):
            return True

        async def execute(self, _source_id, sql, _params=None):
            self.statements.append(sql)
            if "scale(" in sql:
                assert '"price"' in sql, sql  # only the unsized numeric is sampled
                return SimpleNamespace(rows=[(2,)])
            return SimpleNamespace(
                rows=[
                    ("fee", "money", "money", "money", "b"),
                    ("price", "numeric", "numeric", "numeric", "b"),
                ]
            )

    pool = _Pool()
    typings = await native_column_typings("pg", "postgresql", "public", "t", pool)  # type: ignore[arg-type]
    assert [(n, t.data_type) for n, t in typings or []] == [
        ("fee", "money"),
        ("price", "numeric(38,2)"),
    ]
    assert sum("scale(" in s for s in pool.statements) == 1


def test_pgwire_sends_the_shapes_ranges_bit_strings_and_tsvector_in_binary_form():
    assert _binary("macaddr8", "08:00:2b:01:02:03:04:05") == bytes.fromhex("08002b0102030405")
    assert _binary("macaddr8", "08:00:2b:01:02:03") == bytes.fromhex("08002bfffe010203")
    assert _binary("line", "{1,-1,0}") == struct.pack("!ddd", 1, -1, 0)
    assert _binary("lseg", "[(0,0),(1,1)]") == struct.pack("!dddd", 0, 0, 1, 1)
    assert _binary("box", "(1,1),(0,0)") == struct.pack("!dddd", 1, 1, 0, 0)
    assert _binary("circle", "<(1,2),3>") == struct.pack("!ddd", 1, 2, 3)
    assert _binary("path", "((0,0),(1,0),(1,1))") == struct.pack("!bi6d", 1, 3, 0, 0, 1, 0, 1, 1)
    assert _binary("path", "[(0,0),(1,1)]") == struct.pack("!bi4d", 0, 2, 0, 0, 1, 1)
    assert _binary("polygon", "((0,0),(1,0),(1,1))") == struct.pack("!i6d", 3, 0, 0, 1, 0, 1, 1)
    assert _binary("bit", "101") == struct.pack("!i", 3) + bytes([0b10100000])
    assert _binary("varbit", "101100001") == struct.pack("!i", 9) + bytes([0b10110000, 0x80])
    assert _binary("int4range", "[1,10)") == (
        bytes([0x02]) + struct.pack("!ii", 4, 1) + struct.pack("!ii", 4, 10)
    )
    assert _binary("int8range", "(,10)") == bytes([0x08]) + struct.pack("!iq", 8, 10)
    assert _binary("int4range", "empty") == bytes([0x01])
    assert _binary("daterange", "[2026-01-01,2026-02-01)") == (
        bytes([0x02])
        + struct.pack("!i", 4)
        + struct.pack("!i", 9497)
        + struct.pack("!i", 4)
        + struct.pack("!i", 9528)
    )
    assert _binary("tstzrange", '["2026-01-01 00:00:00+00",)')[:1] == bytes([0x02 | 0x10])
    assert _binary("tsvector", "'a':1A 'b''c':2,3") == (
        struct.pack("!i", 2)
        + b"a\x00"
        + struct.pack("!HH", 1, (3 << 14) | 1)
        + b"b'c\x00"
        + struct.pack("!HHH", 2, 2, 3)
    )


def test_geometry_and_geography_are_sent_in_text_form_only():
    assert _text("geometry", "POINT(1 2)") == "POINT(1 2)"
    with pytest.raises(ValueError, match="text format only"):
        _binary("geography", "POINT(1 2)")


# --- the UI shows a column's subtype with its base -----------------------------------------------


@pytest.mark.asyncio
async def test_a_column_saved_with_its_base_and_subtype_keeps_both_and_a_wrong_one_is_refused(
    org_plane,
):
    hosts = _hosts(("ip", "text"), ("fee", "numeric(21,4)"))
    hosts.columns[0].subtype = "inet"
    hosts.columns[1].subtype = "money"
    async with org_plane.acquire() as conn:
        await table_repo.upsert(conn, hosts)
    assert await _stored(org_plane) == {"ip": ("text", "inet"), "fee": ("numeric(21,4)", "money")}
    wrong = _hosts(("ip", "integer"))
    wrong.columns[0].subtype = "inet"
    async with org_plane.acquire() as conn:
        with pytest.raises(table_repo.SubtypeRefused, match=r"hosts\.ip .*'inet'.*'integer'"):
            await table_repo.upsert(conn, wrong)


def test_the_column_types_the_ui_reads_carry_the_subtype():
    from provisa.api.admin.types import AvailableColumnType, ColumnInput, TableColumnType

    for gql_type in (AvailableColumnType, ColumnInput, TableColumnType):
        assert "subtype" in {f.name for f in gql_type.__strawberry_definition__.fields}


@pytest.mark.asyncio
async def test_a_suggested_column_of_a_subtype_is_offered_as_its_base_carrying_the_subtype(
    monkeypatch,
):
    from provisa.api.admin import schema_query
    from provisa.api.admin.types import AvailableColumnType

    async def _listed(*_args):
        return [
            AvailableColumnType(name="ip", data_type="inet", comment=None),
            AvailableColumnType(name="n", data_type="integer", comment=None),
        ]

    async def _remember(*_args):
        return None

    monkeypatch.setattr(schema_query, "resolve_available_columns_metadata", _listed)
    monkeypatch.setattr(schema_query, "_remember_selected_columns", _remember)
    monkeypatch.setattr(schema_query, "require_capability", lambda *_a: None)
    columns = await schema_query.Query().available_columns_metadata(None, "pg", "public", "t")
    assert [(c.name, c.data_type, c.subtype) for c in columns] == [
        ("ip", "text", "inet"),
        ("n", "integer", None),
    ]
