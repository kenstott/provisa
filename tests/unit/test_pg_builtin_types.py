# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6c1e72-8a4d-4b19-9d07-5e2b7c4a1f36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every PostgreSQL 17 built-in type is a Provisa type or subtype (REQ-2171, THE TYPES ARE A
SUPERSET OF POSTGRESQL'S), each read of one as PostgreSQL's canonical text.

``catalog_pg17.TYPES`` is generated from a PostgreSQL 17 server, so a built-in type this file does
not place fails it."""

# Requirements: REQ-2171

from __future__ import annotations

import datetime
import struct

import duckdb
import pytest
from graphql import GraphQLList, GraphQLNonNull, execute_sync, parse, print_schema

from buenavista.postgres import BVTYPE_TO_PGTYPE
from provisa.api.admin.introspect import _pg_column_typing
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.native_subtype import mark_subtype_reads
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.type_map import (
    FILTER_TYPE_MAP,
    SUBTYPE_SCALARS,
    BigInt,
    GraphQLFloat,
    GraphQLString,
    JSONScalar,
    Time,
    TimeFilter,
    column_type_to_graphql,
    nested_column_graphql_type,
)
from provisa.core.ir_types import (
    IR_TYPES,
    SUBTYPES,
    column_typing,
    landing_form,
    read_form,
    subtype_of,
    to_ir,
    to_sqlalchemy,
)
from provisa.federation.replica_address import ReplicaRoute, ReplicaRoutes, address_replicas
from provisa.federation.residency import resolve_landing_args
from provisa.pgwire.catalog_data import _PG_OID_ATTR_META, pg_type_rows
from provisa.pgwire.catalog_pg17 import TYPES
from provisa.pgwire.catalog_populate import _physical_to_pg_name, _physical_to_pg_oid
from provisa.pgwire.server import _sql_type_to_bvtype
from provisa.transpiler.native_json import source_projection
from provisa.transpiler.native_subtype import READ_MARKER, resolve_reads
from provisa.transpiler.transpile import transpile

_NAME, _FORMAT = 1, -1
_TYPTYPE, _CATEGORY, _BASE = 3, 4, 6


def _built_ins(*kinds: str) -> dict[int, tuple]:
    return {
        oid: t
        for oid, t in TYPES.items()
        if t[_TYPTYPE] in kinds and t[_CATEGORY] != "A" and t[0] == "pg_catalog"
    }


# int2vector and oidvector are filed under arrays, and are column types of their own.
_VECTORS = {22, 30}
_COLUMN_TYPES = {**_built_ins("b", "r", "m"), **{oid: TYPES[oid] for oid in _VECTORS}}
_PSEUDO_TYPES = _built_ins("p")
_DOMAINS = {oid: t for oid, t in TYPES.items() if t[_TYPTYPE] == "d"}
_ARRAYS = {oid: t for oid, t in TYPES.items() if t[_CATEGORY] == "A" and oid not in _VECTORS}

# A built-in type the IR holds itself: (its IR type). Every other built-in type is a subtype.
_IR_OWN: dict[str, str] = {
    "bool": "boolean",
    "bytea": "bytea",
    "int8": "bigint",
    "int2": "smallint",
    "int4": "integer",
    "text": "text",
    "json": "json",
    "float4": "float",
    "float8": "double",
    "bpchar": "text",
    "varchar": "text",
    "date": "date",
    "time": "time",
    "timestamp": "timestamp",
    "timestamptz": "timestamptz",
    "interval": "interval",
    "numeric": "numeric",
    "uuid": "uuid",
}


def _subtype_name(pg_type: tuple) -> str:
    return '"char"' if pg_type[_FORMAT] == '"char"' else pg_type[_NAME]


def test_the_column_types_are_the_ones_postgresql_17_has():
    assert len(_COLUMN_TYPES) == len(_IR_OWN) + len(SUBTYPES) - 2  # geometry, geography: PostGIS's


@pytest.mark.parametrize(
    ("oid", "pg_type"), sorted(_COLUMN_TYPES.items()), ids=lambda v: str(v)[:24]
)
def test_each_built_in_type_is_a_type_of_the_ir_or_a_subtype_at_its_oid(oid, pg_type):
    if pg_type[_NAME] in _IR_OWN:
        assert to_ir(pg_type[_FORMAT], "postgresql") == _IR_OWN[pg_type[_NAME]]
        assert subtype_of(pg_type[_FORMAT], "postgresql") is None
        return
    subtype = _subtype_name(pg_type)
    assert subtype_of(pg_type[_FORMAT], "postgresql") == subtype
    assert SUBTYPES[subtype].pg_oid == oid
    assert column_typing(pg_type[_FORMAT], "postgresql").data_type == subtype
    # Sent as, and listed as, its own type.
    assert BVTYPE_TO_PGTYPE[_sql_type_to_bvtype(subtype)][0] == oid
    assert _physical_to_pg_oid(subtype) == oid
    assert _physical_to_pg_name(subtype) == subtype
    assert oid in {row[0] for row in pg_type_rows()}
    assert _PG_OID_ATTR_META[oid] == (pg_type[2], pg_type[7], pg_type[8], pg_type[9])


@pytest.mark.parametrize(("oid", "domain"), sorted(_DOMAINS.items()), ids=lambda v: str(v)[:24])
def test_a_domain_is_its_base_as_postgresql_describes_it(oid, domain):
    del oid
    base = TYPES[domain[_BASE]]
    assert to_ir(domain[_FORMAT]) == to_ir(base[_FORMAT])
    assert subtype_of(domain[_FORMAT]) == subtype_of(base[_FORMAT])


@pytest.mark.parametrize(
    ("oid", "pseudo"), sorted(_PSEUDO_TYPES.items()), ids=lambda v: str(v)[:24]
)
def test_a_pseudo_type_holds_no_column_and_reads_as_text_if_a_source_names_one(oid, pseudo):
    del oid
    typing = column_typing(pseudo[_FORMAT], "postgresql")
    assert typing.data_type == ("json" if pseudo[_FORMAT] == "record[]" else "text")


@pytest.mark.parametrize(("oid", "array"), sorted(_ARRAYS.items()), ids=lambda v: str(v)[:24])
def test_an_array_of_any_type_is_json_keeping_its_native_type(oid, array):
    del oid
    typing = column_typing(array[_FORMAT], "postgresql")
    assert typing.data_type == "json"
    assert typing.native_type == array[_FORMAT].lower()


@pytest.mark.parametrize(
    ("data_type", "udt", "typtype", "expected"),
    [
        ("time with time zone", "timetz", "b", "timetz"),
        ("jsonb", "jsonb", "b", "jsonb"),
        ("json", "json", "b", "json"),
        ("pg_lsn", "pg_lsn", "b", "pg_lsn"),
        ('"char"', "char", "b", '"char"'),
        # A domain reports its base; an enum is its label's text.
        ("USER-DEFINED", "mood", "e", "text"),
    ],
)
def test_a_postgres_column_registers_with_its_identity(data_type, udt, typtype, expected):
    assert _pg_column_typing("postgresql", data_type, udt, udt, typtype).data_type == expected


def test_a_composite_stays_json_with_its_native_type():
    typing = _pg_column_typing("postgresql", "USER-DEFINED", "address", "address", "c")
    assert (typing.data_type, typing.native_type) == ("json", "address")


def test_jsonb_is_not_json_and_timetz_keeps_its_offset():
    assert subtype_of("json", "postgresql") is None and SUBTYPES["jsonb"].base == "json"
    assert (_physical_to_pg_oid("json"), _physical_to_pg_oid("jsonb")) == (114, 3802)
    assert (_physical_to_pg_name("json"), _physical_to_pg_name("jsonb")) == ("json", "jsonb")
    # Held as text, so every store keeps the offset a time column has no place for.
    assert to_ir("timetz") == to_ir("time(3) with time zone") == "text"
    assert column_typing("time(3) with time zone", "postgresql").data_type == "timetz"
    assert subtype_of("time with time zone", "trino") == "timetz"
    assert to_sqlalchemy("timetz") is to_sqlalchemy("text")


def test_a_source_type_mapping_to_nothing_is_read_as_text():
    assert column_typing("some_vendor_blob_of_thing", "oracle").data_type == "text"
    assert to_ir("some_vendor_blob_of_thing") == "text"


# --- the read form ------------------------------------------------------------------------------


@pytest.mark.parametrize("subtype", sorted(SUBTYPES))
@pytest.mark.parametrize(
    "reader",
    [
        "postgres",
        "duckdb",
        "trino",
        "mysql",
        "tsql",
        "oracle",
        "bigquery",
        "snowflake",
        "elasticsearch",
    ],
)
def test_every_reader_lands_every_subtype(subtype, reader):
    assert "{col}" in landing_form(subtype, reader)


@pytest.mark.parametrize(
    ("subtype", "reader", "form"),
    [
        ("pg_lsn", "postgres", "CAST({col} AS TEXT)"),
        ("pg_lsn", "duckdb", "CAST({col} AS VARCHAR)"),
        ("tsquery", "mysql", "CAST({col} AS CHAR)"),
        ("cidr", "tsql", "CAST({col} AS NVARCHAR(MAX))"),
        ("cidr", "clickhouse", "toString({col})"),
        ("macaddr", "bigquery", "CAST({col} AS STRING)"),
        ("geography", "duckdb", "CAST({col} AS VARCHAR)"),
        # What a reader with the subtype's own form uses is that form.
        ("geometry", "duckdb", "ST_AsText({col})"),
        # An adapter hands over what it holds.
        ("geography", "elasticsearch", "{col}"),
        # On a base other than text it is that base's value.
        ("money", "duckdb", "CAST({col} AS NUMERIC(21,4))"),
        ("oid", "duckdb", "CAST({col} AS BIGINT)"),
        ("jsonb", "postgres", "{col}"),
        ("jsonb", "duckdb", "{col}"),
    ],
)
def test_a_reader_with_no_form_of_its_own_reads_the_value_as_text(subtype, reader, form):
    assert landing_form(subtype, reader) == form


def test_a_read_as_a_value_is_the_form_it_lands_in_except_where_it_is_its_own_type():
    assert read_form("inet", "duckdb") == landing_form("inet", "duckdb")
    assert read_form("timetz", "trino") == landing_form("timetz", "trino")
    # PostgreSQL has every type and sends its canonical text itself.
    assert read_form("inet", "postgres") is None
    # Not held as text.
    assert read_form("money", "duckdb") is None and read_form("jsonb", "duckdb") is None


@pytest.mark.parametrize(
    ("value", "canonical"),
    [
        ("TIMETZ '12:34:56+05:30'", "12:34:56+05:30"),
        ("TIMETZ '12:34:56+05'", "12:34:56+05"),
        ("TIMETZ '12:34:56.5-08'", "12:34:56.5-08"),
    ],
)
def test_duckdb_reads_a_timetz_as_postgresqls_text(value, canonical):
    expression = landing_form("timetz", "duckdb").format(col=value)
    assert duckdb.sql(f"SELECT {expression}").fetchone() == (canonical,)


_TABLES = {("public", "hosts"): {"ip": "inet", "lsn": "pg_lsn"}}


def test_a_projected_subtyped_column_is_read_through_its_form_under_its_own_name():
    marked = mark_subtype_reads(
        'SELECT "h"."ip", "h"."lsn" AS "at", "h"."id" FROM "public"."hosts" AS "h"', _TABLES
    )
    assert marked.lower().count(READ_MARKER) == 2
    assert mark_subtype_reads(marked, _TABLES) == marked
    sql = transpile(marked, "duckdb")
    assert sql == (
        'SELECT CAST("h"."ip" AS TEXT) AS "ip", CAST("h"."lsn" AS TEXT) AS "at", "h"."id" '
        'FROM "public"."hosts" AS "h"'
    )


def test_a_timetz_orders_as_its_own_type_not_as_its_text():
    from provisa.compiler.native_subtype import mark_subtype_operations

    sql = 'SELECT "h"."id" FROM "public"."hosts" AS "h" ORDER BY "h"."opened"'
    marked = mark_subtype_operations(sql, {("public", "hosts"): {"opened": "timetz"}})
    assert 'CAST("h"."opened" AS TIMETZ)' in transpile(marked, "duckdb")
    assert 'CAST("h"."opened" AS TIMETZ)' in transpile(marked, "postgres")


def test_a_clause_of_the_select_is_not_a_read():
    sql = 'SELECT "h"."id" FROM "public"."hosts" AS "h" WHERE "h"."ip" IS NULL'
    assert mark_subtype_reads(sql, _TABLES) == sql


def test_a_read_on_postgresql_is_the_column_itself():
    marked = mark_subtype_reads('SELECT "h"."ip" FROM "public"."hosts" AS "h"', _TABLES)
    assert READ_MARKER not in transpile(marked, "postgres").lower()
    assert resolve_reads("SELECT 1", "postgres") == "SELECT 1"


def test_a_replica_already_holds_the_text():
    marked = mark_subtype_reads('SELECT "h"."ip" FROM "public"."hosts" AS "h"', _TABLES)
    routes = ReplicaRoutes(
        routes={
            (None, "public", "hosts"): ReplicaRoute(
                source_id="pg", table_name="hosts", target=("store", "replicas", "pg__hosts")
            )
        }
    )
    addressed = address_replicas(marked, routes)
    assert READ_MARKER not in addressed.lower() and "pg__hosts" in addressed


def test_a_landing_reads_a_column_of_a_type_with_no_form_as_text():
    table = _table(("id", "integer", None), ("lsn", "text", "pg_lsn"), ("tz", "text", "timetz"))
    assert source_projection(table.columns, "duckdb") == (
        '"id", CAST("lsn" AS VARCHAR) AS "lsn", CAST("tz" AS VARCHAR) AS "tz"'
    )
    args = resolve_landing_args(_source(), table, platform="duckdb")
    assert args.columns == [("id", "integer"), ("lsn", "text"), ("tz", "text")]
    assert args.not_landed == ()


def _table(*columns):
    from types import SimpleNamespace

    return SimpleNamespace(
        schema_name="net",
        table_name="hosts",
        watermark_column=None,
        primary_key=[],
        change_signal=None,
        probe_type=None,
        landing_filter=None,
        live=None,
        columns=[
            SimpleNamespace(name=n, data_type=t, subtype=s, is_primary_key=False, embedding=False)
            for n, t, s in columns
        ],
    )


def _source():
    from types import SimpleNamespace

    return SimpleNamespace(
        dialect="postgres", type="postgres", change_signal=None, live=None, cache_ttl=None
    )


# --- GraphQL ------------------------------------------------------------------------------------


def test_each_subtype_has_a_named_scalar_or_its_native_type():
    native = {
        "oid": BigInt,
        "money": GraphQLFloat,
        "jsonb": JSONScalar,
        "name": GraphQLString,
        '"char"': GraphQLString,
        "refcursor": GraphQLString,
        "timetz": Time,
    }
    assert set(SUBTYPE_SCALARS) == set(SUBTYPES) - set(native)
    assert len({scalar.name for scalar in SUBTYPE_SCALARS.values()}) == len(SUBTYPE_SCALARS)
    for subtype, scalar in SUBTYPE_SCALARS.items():
        assert column_type_to_graphql(subtype) is scalar
    for subtype, scalar in native.items():
        assert column_type_to_graphql(subtype) is scalar


@pytest.mark.parametrize(
    ("subtype", "name"),
    [
        ("inet", "IPAddress"),
        ("cidr", "CIDR"),
        ("macaddr", "MACAddress"),
        ("xml", "XML"),
        ("int4range", "Int4Range"),
        ("datemultirange", "DateMultiRange"),
        ("pg_lsn", "PgLsn"),
        ("tsquery", "TsQuery"),
        ("regclass", "Regclass"),
    ],
)
def test_a_subtypes_scalar_is_named(subtype, name):
    assert SUBTYPE_SCALARS[subtype].name == name


def test_time_and_timetz_are_the_time_scalar_and_numeric_stays_a_float():
    for spelling in ("time", "time(3)", "time without time zone", "timetz", "time with time zone"):
        assert column_type_to_graphql(spelling) is Time
    assert column_type_to_graphql("numeric(12,2)") is GraphQLFloat


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (datetime.time(12, 34, 56), "12:34:56"),
        (datetime.time(12, 34, 56, 500000), "12:34:56.5"),
        (
            datetime.time(12, 34, 56, tzinfo=datetime.timezone(datetime.timedelta(hours=5))),
            "12:34:56+05",
        ),
        (
            datetime.time(12, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30))),
            "12:00:00+05:30",
        ),
        ("12:34:56+05", "12:34:56+05"),
    ],
)
def test_the_time_scalar_serializes_postgresqls_text(value, text):
    assert Time.serialize(value) == text


@pytest.mark.parametrize("value", [1700000000, 1.5, True, {"a": 1}])
def test_a_subtypes_scalar_serializes_only_its_canonical_text(value):
    assert SUBTYPE_SCALARS["inet"].serialize("10.0.0.1/24") == "10.0.0.1/24"
    with pytest.raises(TypeError, match="IPAddress cannot represent"):
        SUBTYPE_SCALARS["inet"].serialize(value)


def test_every_scalar_a_column_can_be_has_a_filter():
    # schema_inputs leaves out a column whose scalar has none, so filtering would vanish silently.
    for scalar in (*SUBTYPE_SCALARS.values(), Time):
        assert scalar in FILTER_TYPE_MAP, scalar.name
        assert set(FILTER_TYPE_MAP[scalar].fields) == {
            "eq",
            "neq",
            "in",
            "is_null",
            "gt",
            "gte",
            "lt",
            "lte",
            "like",
            "ilike",
        }
    assert FILTER_TYPE_MAP[Time] is TimeFilter
    assert FILTER_TYPE_MAP[SUBTYPE_SCALARS["inet"]].name == "IPAddressFilter"
    assert FILTER_TYPE_MAP[SUBTYPE_SCALARS["inet"]].fields["eq"].type is SUBTYPE_SCALARS["inet"]


def test_a_filter_keeps_every_comparator_a_string_has():
    assert set(FILTER_TYPE_MAP[SUBTYPE_SCALARS["tsquery"]].fields) == set(
        FILTER_TYPE_MAP[GraphQLString].fields
    )


_COLUMNS = {
    "id": ColumnMetadata("id", "integer", False),
    "ip": ColumnMetadata("ip", "text", True, subtype="inet"),
    "at": ColumnMetadata("at", "text", True, subtype="pg_lsn"),
    "opened": ColumnMetadata("opened", "text", True, subtype="timetz"),
    "doc": ColumnMetadata("doc", "json", True, subtype="jsonb"),
    # A subtype declared on a varchar, the one character type a min applies to.
    "mac": ColumnMetadata("mac", "varchar", True, subtype="macaddr"),
}


def _schema_input() -> SchemaInput:
    return SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "pg",
                "domain_id": "net",
                "schema_name": "public",
                "table_name": "hosts",
                "enable_aggregates": True,
                "enable_group_by": True,
                "columns": [
                    {"column_name": c.column_name, "visible_to": ["admin"], "subtype": c.subtype}
                    for c in _COLUMNS.values()
                ],
                "write_ops": [],
                "write_returns_rows": True,
                "write_refused_forms": [],
            }
        ],
        relationships=[],
        column_types={1: list(_COLUMNS.values())},
        naming_rules=[],
        role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
        domains=[{"id": "net", "description": "Network"}],
    )


def test_a_subtyped_column_is_its_scalar_in_the_sdl_and_filters_by_it():
    sdl = print_schema(generate_schema(_schema_input()))
    assert "scalar IPAddress" in sdl and "scalar PgLsn" in sdl and "scalar Time" in sdl
    assert "ip: IPAddress" in sdl and "at: PgLsn" in sdl and "opened: Time" in sdl
    assert "doc: JSON" in sdl
    assert "ip: IPAddressFilter" in sdl and "at: PgLsnFilter" in sdl and "opened: TimeFilter" in sdl


def test_a_min_of_a_subtyped_column_is_its_scalar():
    sdl = print_schema(generate_schema(_schema_input()))
    block = sdl[sdl.index("MinFields {") :]
    assert "mac: MACAddress" in block[: block.index("}")]


def test_a_query_serializes_a_subtype_as_its_canonical_string():
    schema = generate_schema(_schema_input())
    field = next(iter(schema.query_type.fields))
    result = execute_sync(
        schema,
        parse(f"{{ {field} {{ ip opened }} }}"),
        root_value={field: [{"ip": "10.0.0.1/24", "opened": "12:34:56+05"}]},
    )
    assert result.errors is None
    assert result.data == {field: [{"ip": "10.0.0.1/24", "opened": "12:34:56+05"}]}


def test_the_compiled_read_of_a_subtyped_column_is_marked():
    ctx = build_context(_schema_input())
    assert ctx.read_columns == {
        ("public", "hosts"): {
            "ip": "inet",
            "at": "pg_lsn",
            "opened": "timetz",
            "mac": "macaddr",
        }
    }
    assert ("public", "hosts") in ctx.subtype_columns  # ip and opened order as their own type
    assert "at" not in ctx.subtype_columns[("public", "hosts")]


def test_an_array_of_a_subtype_is_a_list_of_its_scalar():
    declared = nested_column_graphql_type(
        {
            "native_type": "inet[]",
            "native_platform": "postgresql",
            "array_depth": 1,
            "element_type": "text",
        }
    )
    assert isinstance(declared, GraphQLList) and declared.of_type is SUBTYPE_SCALARS["inet"]
    assert not isinstance(declared.of_type, GraphQLNonNull)


# --- the wire -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("subtype", "text", "binary"),
    [
        ("tid", "(1,2)", struct.pack("!IH", 1, 2)),
        ("xid", "4000000000", struct.pack("!I", 4000000000)),
        ("cid", "7", struct.pack("!I", 7)),
        ("xid8", "18446744073709551615", struct.pack("!Q", 18446744073709551615)),
        ("jsonpath", "$.a", b"\x01$.a"),
        ("name", "pg_class", b"pg_class"),
        ('"char"', "r", b"r"),
        ("pg_lsn", "16/B374D848", struct.pack("!Q", 0x16B374D848)),
        ("pg_snapshot", "10:20:12,15", struct.pack("!iqqqq", 2, 10, 20, 12, 15)),
        ("int2vector", "1 2", None),
        ("txid_snapshot", "10:20:", struct.pack("!iqq", 0, 10, 20)),
    ],
)
def test_a_value_held_as_text_is_sent_in_its_types_text_and_binary_forms(subtype, text, binary):
    _oid, to_text, to_binary = BVTYPE_TO_PGTYPE[_sql_type_to_bvtype(subtype)]
    assert to_text(text) == text
    assert to_binary(text) == (binary or to_binary([1, 2]))


@pytest.mark.parametrize(
    ("text", "binary"),
    [
        ("12:34:56+05", struct.pack("!qi", 45296000000, -18000)),
        ("12:34:56+05:30", struct.pack("!qi", 45296000000, -19800)),
    ],
)
def test_a_timetz_read_as_text_is_sent_in_its_binary_form(text, binary):
    _oid, to_text, to_binary = BVTYPE_TO_PGTYPE[_sql_type_to_bvtype("timetz")]
    assert to_text(text) == text and to_binary(text) == binary


@pytest.mark.parametrize("subtype", ["tsquery", "aclitem", "regrole", "pg_mcv_list"])
def test_a_type_whose_text_does_not_give_its_binary_form_says_so(subtype):
    _oid, to_text, to_binary = BVTYPE_TO_PGTYPE[_sql_type_to_bvtype(subtype)]
    assert to_text("x") == "x"
    with pytest.raises(ValueError, match=f"{subtype} is sent in text format only"):
        to_binary("x")


def test_every_base_is_a_type_of_the_ir():
    assert {to_ir(sub.base) for sub in SUBTYPES.values()} <= IR_TYPES
