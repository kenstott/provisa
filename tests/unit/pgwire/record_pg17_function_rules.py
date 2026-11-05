# Copyright (c) 2026 Kenneth Stott
# Canary: c5e2a7d4-1f86-4b39-9a02-6d3b80e4f715
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Writes pg17_function_rule_answers.py: each statement of the generated rule of
provisa.transpiler.pg_forms_types, answered by a real PostgreSQL 17 (REQ-2171).

    docker run -d --name pg17 -e POSTGRES_PASSWORD=pg17 -p 127.0.0.1::5432 postgres:17.11
    .venv/bin/python -m tests.unit.pgwire.record_pg17_function_rules \\
        postgresql://postgres:pg17@127.0.0.1:<port>/postgres

A row's overloads and their types are pg_proc's. A row's statements are each overload's NULLs, a
NULL beside a value, each value against ``_PARTNERS`` values of the other argument, and each value
against itself."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import sys
from pathlib import Path
from pprint import pformat
from typing import Any

import psycopg
from psycopg import pq

from provisa.transpiler.pg_forms_types import ARRAY_OPERATORS, OPERATORS

_ANSWERS = Path(__file__).with_name("pg17_function_rule_answers.py")
_HEADER = '''# Copyright (c) 2026 Kenneth Stott
# Canary: 8a1d3f60-27c4-4f19-b5e8-3c7e90d2a614
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What PostgreSQL 17 (postgres:17.11, UTF8, en_US.utf8, UTC) answered for the generated rule of
provisa.transpiler.pg_forms_types: statement -> ("ok", type oid, rows) or ("err", SQLSTATE,
message). Real values, each statement over the extended protocol with text results. Written by
record_pg17_function_rules.py, never by hand."""

# Requirements: REQ-2167, REQ-2171
# fmt: off

from __future__ import annotations

from typing import Any

RECORDED: dict[str, tuple[Any, Any, Any]] = '''

_PARTNERS = 3


def _numbers(type_name: str, *values: str) -> list[str]:
    return [f"({v})::{type_name}" for v in values]


def _typed(type_name: str, *values: str) -> list[str]:
    return [f"'{v}'::{type_name}" for v in values]


_TIMESTAMPTZ = "timestamp with time zone"
_TIMESTAMP = "timestamp without time zone"
_FLOATS = ("0", "1.5", "-2.25", "0.1")

#: PostgreSQL's type -> the arguments a row of it is proved with: the bounds of the type and the
#: values its own format treats specially.
_VALUES: dict[str, list[str]] = {
    "smallint": _numbers("smallint", "0", "1", "-1", "7", "-3", "32767", "-32768", "100", "200"),
    "integer": _numbers(
        "integer", "0", "1", "-1", "7", "-3", "2147483647", "-2147483648", "33", "40"
    ),
    "bigint": _numbers(
        "bigint",
        "0",
        "1",
        "-1",
        "7",
        "-3",
        "9223372036854775807",
        "-9223372036854775808",
        "9007199254740993",
        "65",
    ),
    "real": [
        *_numbers("real", *_FLOATS, "1e38", "1e-38"),
        *_typed("real", "NaN", "Infinity", "-Infinity"),
    ],
    "double precision": [
        *_numbers(
            "double precision",
            *_FLOATS,
            "1e308",
            "1e-308",
            "1e15",
            "1e-5",
            "123456789012345680",
            "100",
        ),
        *_typed("double precision", "NaN", "Infinity"),
    ],
    "numeric": [f"{v}::numeric" for v in ("0", "1.5", "-2.25", "1234567.891", "100", "0.10")],
    "boolean": ["true::boolean", "false::boolean"],
    "bytea": _typed("bytea", "\\x", "\\x00ff", "\\x41", "\\x4142"),
    '"char"': _typed('"char"', "a", "B"),
    "oid": _numbers("oid", "0", "1", "4294967295"),
    "bit": [f"B'{v}'::bit(3)" for v in ("101", "110", "000")],
    "bit varying": [f"B'{v}'::bit varying" for v in ("101", "1010", "0")],
    "date": _typed("date", "2024-02-29", "1999-12-31", "2000-01-01", "1970-01-01"),
    "time without time zone": _typed(
        "time without time zone", "00:00:00", "12:30:01.5", "23:59:59.999999"
    ),
    "time with time zone": _typed(
        "time with time zone", "12:30:01+02", "10:30:01+00", "23:00:00-05"
    ),
    _TIMESTAMP: _typed(
        _TIMESTAMP,
        "2024-02-29 12:30:01.5",
        "1999-12-31 23:59:59",
        "2000-01-01 00:00:00",
        "2024-03-01 00:00:00",
    ),
    _TIMESTAMPTZ: _typed(
        _TIMESTAMPTZ,
        "2024-02-29 12:30:01.5+00",
        "1999-12-31 23:59:59+02",
        "2024-02-29 00:00:00+00",
    ),
    "interval": _typed(
        "interval",
        "0",
        "1 mon 2 days 03:04:05.5",
        "-1 year -2 mons",
        "25:00:00",
        "30 days",
        "1 mon",
        "1 year 1 mon -1 day",
        "-00:00:01",
    ),
    "uuid": _typed(
        "uuid",
        "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        "ffffffff-0000-0000-0000-000000000000",
        "00000000-0000-0000-0000-000000000001",
    ),
    "macaddr": _typed(
        "macaddr",
        "08:00:2b:01:02:03",
        "08-00-2b-01-02-04",
        "0800.2b01.0205",
        "ff:ff:ff:ff:ff:ff",
    ),
    "macaddr8": _typed(
        "macaddr8",
        "08:00:2b:01:02:03:04:05",
        "08-00-2b-01-02-03-04-06",
        "0800.2b01.0203.0407",
    ),
    "pg_lsn": _typed("pg_lsn", "16/B374D848", "0/0", "FFFFFFFF/FFFFFFFF", "16/b374d849"),
    "tid": _typed("tid", "(0,1)", "(1,2)", "(1,10)", "(4294967295,65535)"),
    "name": _typed("name", "a", "B", "", "abc"),
    "text": _typed("text", "a", "B", "", "abc", "a b", "é"),
    "character": _typed("character(3)", "a  ", "B", "abc", ""),
    "inet": _typed("inet", "10.1.2.3/8", "192.168.1.5", "10.1.0.0/16", "255.255.255.255", "::1"),
    "cidr": _typed("cidr", "10.0.0.0/8", "192.168.1.0/24", "10.1.0.0/16"),
    "cid": _typed("cid", "0", "5", "4294967295"),
    "xid": _typed("xid", "0", "5", "4294967295"),
    "anyarray": [*(f"ARRAY[{v}]" for v in ("1,2", "1,2,3", "1,NULL", "2")), "'{}'::integer[]"],
    "record": ["ROW(1,2)", "ROW(1,3)", "ROW(1,NULL::integer)", "ROW(2,1)", "ROW(NULL::integer,2)"],
}
#: The type a NULL of a pseudo-type is written in.
_NULL_OF = {"anyarray": "integer[]"}
#: date_subtract's third argument is a time zone.
_ZONES = _typed("text", "America/New_York", "UTC")


def _null(type_name: str) -> str:
    return f"NULL::{_NULL_OF.get(type_name, type_name)}"


def _call(name: str, *arguments: str) -> str:
    return f'SELECT pg_catalog."{name}"({", ".join(arguments)})'


def _values(name: str, types: tuple[str, ...]) -> list[list[str]]:
    return [
        _ZONES if name == "date_subtract" and index == 2 else _VALUES[type_name]
        for index, type_name in enumerate(types)
    ]


def _statements(name: str, types: tuple[str, ...]) -> list[str]:
    """One overload's statements."""
    values = _values(name, types)
    nulls = [_null(t) for t in types]
    statements = [_call(name, *nulls)]
    first = values[0]
    if len(types) == 1:
        return statements + [_call(name, value) for value in first]
    second = values[1]
    rest = [other[0] for other in values[2:]]
    for at, value in enumerate(first):
        partners = {second[(at + step * 2) % len(second)] for step in range(_PARTNERS)}
        statements.extend(_call(name, value, other, *rest) for other in sorted(partners))
    if types[0] == types[1]:
        statements.extend(_call(name, value, value, *rest) for value in first)
    statements.append(_call(name, first[0], nulls[1], *rest))
    statements.append(_call(name, nulls[0], second[0], *rest))
    return statements


def _overloads(connection: psycopg.Connection[Any]) -> list[tuple[str, tuple[str, ...]]]:
    """Each form's overloads as pg_proc has them, of the form's argument count."""
    arities = {form.name: len(form.params.split(", ")) for form in (*OPERATORS, *ARRAY_OPERATORS)}
    rows = connection.execute(
        "SELECT proname, (SELECT array_agg(format_type(t, NULL) ORDER BY o) "
        "FROM unnest(proargtypes::oid[]) WITH ORDINALITY AS a(t, o)) "
        "FROM pg_proc WHERE proname = ANY(%s) AND pronargs > 0 ORDER BY oid",
        [list(arities)],
    ).fetchall()
    return [(name, tuple(types)) for name, types in rows if len(types) == arities[name]]


def _answer(connection: psycopg.Connection[Any], sql: str) -> tuple[Any, Any, Any]:
    res = connection.pgconn.exec_params(sql.encode(), [], result_format=0)
    if res.status == pq.ExecStatus.FATAL_ERROR:
        sqlstate = res.error_field(pq.DiagnosticField.SQLSTATE)
        message = res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY)
        assert sqlstate is not None and message is not None
        return ("err", sqlstate.decode(), message.decode())
    assert res.status == pq.ExecStatus.TUPLES_OK
    rows = [None if (v := res.get_value(i, 0)) is None else v.decode() for i in range(res.ntuples)]
    return ("ok", res.ftype(0), rows)


def main(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as connection:
        version = connection.execute("SHOW server_version").fetchone()
        zone = connection.execute("SHOW TimeZone").fetchone()
        assert version is not None and version[0].startswith("17."), version
        assert zone is not None and zone[0] in ("UTC", "Etc/UTC"), zone
        statements = [
            sql for name, types in _overloads(connection) for sql in _statements(name, types)
        ]
        recorded = {sql: _answer(connection, sql) for sql in dict.fromkeys(statements)}
    _ANSWERS.write_text(_HEADER + pformat(recorded, width=100, sort_dicts=False) + "\n")
    print(f"{len(recorded)} statements -> {_ANSWERS}")


if __name__ == "__main__":
    main(sys.argv[1])
