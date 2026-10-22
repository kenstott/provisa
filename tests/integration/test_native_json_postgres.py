# Copyright (c) 2026 Kenneth Stott
# Canary: 1d8f4a63-7e25-4b9c-a0d3-9f6b2e5c8a14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1965 on a real PostgreSQL: a natively array or composite column is registered from the
catalog's own description of it, and read as JSON by the expression PostgreSQL executes -- with
exact decimals, timestamps at their instant, hex binary, and a non-finite float refused."""

from __future__ import annotations

import json
import os
import uuid

import psycopg2
import pytest
import sqlglot.expressions as exp

from provisa.api.admin.introspect import _PG_COLUMN_TYPES_SQL, _pg_column_typing
from provisa.core.ir_types import to_physical
from provisa.transpiler.native_json import (
    NON_FINITE,
    NestedColumn,
    native_json_marker,
    source_projection,
)
from provisa.transpiler.transpile import transpile

pytestmark = [pytest.mark.integration]

# column, PostgreSQL type, a value, (depth, element type, native type), the JSON a read returns
_COLUMNS = [
    ("tags", "text[]", "ARRAY['a', NULL, 'b']", (1, "text", "text[]"), ["a", None, "b"]),
    ("names", "varchar(20)[]", "ARRAY['x']", (1, "text", "character varying(20)[]"), ["x"]),
    (
        "counts",
        "bigint[]",
        "ARRAY[9007199254740993]",
        (1, "bigint", "bigint[]"),
        [9007199254740993],
    ),
    ("flags", "boolean[]", "ARRAY[true, false]", (1, "boolean", "boolean[]"), [True, False]),
    (
        "amounts",
        "numeric(20,2)[]",
        "ARRAY[12345678901234567.89, 0.10]",
        (1, "numeric(20,2)", "numeric(20,2)[]"),
        ["12345678901234567.89", "0.10"],
    ),
    ("days", "date[]", "ARRAY[DATE '2026-01-02']", (1, "date", "date[]"), ["2026-01-02"]),
    (
        "seen",
        "timestamp[]",
        "ARRAY[TIMESTAMP '2026-10-09 12:34:56.123456', NULL]",
        (1, "timestamp", "timestamp without time zone[]"),
        ["2026-10-09T12:34:56.123456", None],
    ),
    (
        "seen_tz",
        "timestamptz[]",
        "ARRAY[TIMESTAMPTZ '2026-10-09 12:34:56.123456+02']",
        # REQ-2050: an instant's element is timestamptz, written in the timestamp form, in UTC.
        (1, "timestamptz", "timestamp with time zone[]"),
        ["2026-10-09T10:34:56.123456"],  # the same instant, in UTC
    ),
    ("blobs", "bytea[]", "ARRAY['\\x00ff'::bytea]", (1, "bytea", "bytea[]"), ["\\x00ff"]),
    (
        "ratios",
        "double precision[]",
        "ARRAY[1.5, NULL]",
        (1, "double", "double precision[]"),
        [1.5, None],
    ),
    (
        "ids",
        "uuid[]",
        "ARRAY['6f1c2d3e-0000-4000-8000-000000000001'::uuid]",
        (1, "uuid", "uuid[]"),
        ["6f1c2d3e-0000-4000-8000-000000000001"],
    ),
    # A declared second dimension is documentation to PostgreSQL: depth 1, and the value's own
    # dimensions come out as arrays of arrays.
    ("grid", "integer[][]", "ARRAY[[1, 2], [3, 4]]", (1, "integer", "integer[]"), [[1, 2], [3, 4]]),
]


@pytest.fixture(scope="module")
def pg(docker_postgres):
    con = psycopg2.connect(
        host=docker_postgres["host"],
        port=docker_postgres["port"],
        dbname=os.environ.get("PG_DATABASE", "provisa"),
        user=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
    )
    con.autocommit = True
    schema = f"json_cols_{uuid.uuid4().hex[:8]}"
    cur = con.cursor()
    cur.execute(f'CREATE SCHEMA "{schema}"')
    cur.execute(f'CREATE TYPE "{schema}".address AS (city text, zip text)')
    cur.execute(f"CREATE TYPE \"{schema}\".order_status AS ENUM ('open', 'shipped')")
    declared = ", ".join(f"{name} {pg_type}" for name, pg_type, *_ in _COLUMNS)
    cur.execute(
        f'CREATE TABLE "{schema}".orders (id integer, {declared}, '
        f'ship_to "{schema}".address, stops "{schema}".address[], '
        f'status "{schema}".order_status, doc jsonb)'
    )
    values = ", ".join(value for _name, _type, value, *_ in _COLUMNS)
    cur.execute(
        f"INSERT INTO \"{schema}\".orders VALUES (1, {values}, ROW('Oslo', '0150'), "
        f"ARRAY[ROW('Oslo', '0150')::\"{schema}\".address], 'open', '{{\"k\": 1}}')"
    )
    cur.execute(f'INSERT INTO "{schema}".orders (id) VALUES (2)')
    yield con, schema
    cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
    con.close()


def _registered(pg) -> dict:
    con, schema = pg
    cur = con.cursor()
    cur.execute(_PG_COLUMN_TYPES_SQL.replace("$1", "%s").replace("$2", "%s"), (schema, "orders"))
    return {row[0]: _pg_column_typing("postgresql", *row[1:5]) for row in cur.fetchall()}


def test_the_catalog_types_every_array_and_composite_column(pg):
    typings = _registered(pg)
    schema = pg[1]
    for name, _pg_type, _value, attributes, _expected in _COLUMNS:
        typing = typings[name]
        assert typing.data_type == "json", name
        assert (typing.array_depth, typing.element_type, typing.native_type) == attributes, name
    assert typings["id"].data_type == "integer"
    # A composite is a struct, an array of composites an array of structs: plain JSON.
    assert (typings["ship_to"].data_type, typings["ship_to"].array_depth) == ("json", 0)
    assert typings["ship_to"].native_type in ("address", f"{schema}.address")
    assert (typings["stops"].data_type, typings["stops"].array_depth) == ("json", 0)
    assert typings["stops"].native_type in ("address[]", f"{schema}.address[]")
    assert typings["status"].data_type == "text"  # an enum
    # JSON at its source: none of the three.
    assert (typings["doc"].data_type, typings["doc"].native_type) == ("json", None)


def _read(pg, column: str, where: str = "id = 1"):
    """The JSON text PostgreSQL hands over for ``column``, and the type of the value itself."""
    con, schema = pg
    typing = _registered(pg)[column]
    nested = NestedColumn(typing.native_type, typing.array_depth, typing.element_type)
    marker = native_json_marker(exp.column(column, "o", quoted=True), nested).sql(
        dialect="postgres"
    )
    sql = transpile(
        f"SELECT CAST({marker} AS TEXT), pg_typeof({marker})::text "
        f'FROM "{schema}"."orders" AS "o" WHERE {where}',
        "postgres",
    )
    cur = con.cursor()
    cur.execute(sql)
    return cur.fetchone()


@pytest.mark.parametrize(
    ("column", "expected"), [(name, expected) for name, *_, expected in _COLUMNS]
)
def test_postgres_hands_a_native_array_over_as_jsonb(pg, column, expected):
    text, value_type = _read(pg, column)
    assert value_type == "jsonb"
    # The exact JSON text, as jsonb prints it (", " and ": " separators).
    assert text == json.dumps(expected)
    assert _read(pg, column, "id = 2")[0] is None  # a NULL column stays NULL


def test_postgres_hands_a_composite_and_an_array_of_composites_over_as_jsonb(pg):
    # jsonb orders an object's keys by length, then bytewise.
    assert _read(pg, "ship_to")[0] == '{"zip": "0150", "city": "Oslo"}'
    assert _read(pg, "stops")[0] == '[{"zip": "0150", "city": "Oslo"}]'


@pytest.mark.parametrize("value", ["'NaN'", "'Infinity'", "'-Infinity'"])
def test_postgres_refuses_a_float_element_that_is_not_a_finite_number(pg, value):
    con, schema = pg
    cur = con.cursor()
    cur.execute(f'CREATE TABLE "{schema}".readings (v double precision[])')
    try:
        cur.execute(f'INSERT INTO "{schema}".readings VALUES (ARRAY[1.5, {value}::float8])')
        marker = native_json_marker(
            exp.column("v", "r", quoted=True),
            NestedColumn("double precision[]", 1, "double"),
        )
        sql = transpile(
            f'SELECT {marker.sql(dialect="postgres")} FROM "{schema}"."readings" AS "r"',
            "postgres",
        )
        with pytest.raises(psycopg2.Error, match=NON_FINITE):
            cur.execute(sql)
    finally:
        cur.execute(f'DROP TABLE "{schema}".readings')


def test_a_replica_lands_every_nested_column_as_jsonb_by_the_same_expressions(pg):
    """The statement copy's select list, run by PostgreSQL into a table whose JSON columns have
    the type IR json renders as on PostgreSQL."""
    con, schema = pg
    typings = _registered(pg)
    assert to_physical("json", "postgresql") == "JSONB"
    columns = [
        type(
            "C",
            (),
            {
                "name": name,
                "native_filter_type": None,
                "native_type": t.native_type,
                "array_depth": t.array_depth,
                "element_type": t.element_type,
            },
        )()
        for name, t in typings.items()
        if name not in ("status", "doc")
    ]
    ddl = ", ".join(f'"{c.name}" {"JSONB" if c.native_type else "integer"}' for c in columns)
    names = ", ".join(f'"{c.name}"' for c in columns)
    cur = con.cursor()
    cur.execute(f'CREATE TABLE "{schema}".orders_replica ({ddl})')
    try:
        cur.execute(
            f'INSERT INTO "{schema}".orders_replica ({names}) '
            f'SELECT {source_projection(columns, "postgres")} FROM "{schema}".orders'
        )
        cur.execute(f'SELECT amounts, seen_tz, ship_to FROM "{schema}".orders_replica WHERE id = 1')
        amounts, seen_tz, ship_to = cur.fetchone()
        assert amounts == ["12345678901234567.89", "0.10"]
        assert seen_tz == ["2026-10-09T10:34:56.123456"]
        assert ship_to == {"city": "Oslo", "zip": "0150"}
        cur.execute(f'SELECT amounts::text FROM "{schema}".orders_replica WHERE id = 1')
        assert json.loads(cur.fetchone()[0]) == ["12345678901234567.89", "0.10"]
    finally:
        cur.execute(f'DROP TABLE "{schema}".orders_replica')
