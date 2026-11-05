# Copyright (c) 2026 Kenneth Stott
# Canary: 3b9e7c15-d246-4a80-8f13-e07a6c2d94f1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The statements that prove a range's and a multirange's emulation (REQ-2171), and how to record
PostgreSQL's answers to them.

Each statement reads a table ``s.t`` of one column ``v``: of the range type in PostgreSQL, of its
text form on an engine. ``python -m tests.unit.range_corpus <dsn>`` runs them on a real PostgreSQL
(docker postgres:17 in a throwaway container) and writes tests/unit/pg17_range_answers.py."""

# Requirements: REQ-2171

from __future__ import annotations

import pprint
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

#: Each type's values, as PostgreSQL's text. The numbers are chosen so that text order is wrong
#: ('[1,10)' sorts before '[9,20)' as text and the bounds 10 and 9 do not), and a sample holds
#: every shape of bound: exclusive, inclusive, unbounded, empty, a single value, and NULL.
SAMPLES: dict[str, list[str | None]] = {
    "int4range": ["[1,10)", "[9,20)", "[10,11)", "(,4)", "[4,)", "(,)", "empty", "[-3,0)", None],
    "int8range": [
        "[1,3000000000)",
        "[2999999999,5000000000)",
        "[3000000000,3000000001)",
        "(,4)",
        "[4,)",
        "(,)",
        "empty",
        None,
    ],
    "numrange": [
        "[1.5,2.50)",
        "(2.50,3]",
        "[2.50,2.50]",
        "[9,20)",
        "[10,11)",
        "(,2)",
        "[2,)",
        "(,)",
        "empty",
        None,
    ],
    "tsrange": [
        '["2020-01-01 00:00:00","2020-02-01 00:00:00")',
        '["2020-01-15 12:30:00.5","2020-03-01 00:00:00"]',
        '("2020-02-01 00:00:00","2020-02-02 00:00:00")',
        '(,"2020-01-10 00:00:00")',
        '["2020-01-10 00:00:00",)',
        "(,)",
        "empty",
        None,
    ],
    "tstzrange": [
        '["2020-01-01 00:00:00+00","2020-02-01 00:00:00+00")',
        '["2020-01-15 12:30:00.5+00","2020-03-01 00:00:00+00"]',
        '("2020-02-01 00:00:00+00","2020-02-02 00:00:00+00")',
        '(,"2020-01-10 00:00:00+00")',
        '["2020-01-10 00:00:00+00",)',
        "(,)",
        "empty",
        None,
    ],
    "daterange": [
        "[2020-01-01,2020-02-01)",
        "[2020-01-15,2020-03-02)",
        "[2020-02-01,2020-02-03)",
        "(,2020-01-10)",
        "[2020-01-10,)",
        "(,)",
        "empty",
        None,
    ],
}

#: Each type's element, as a statement writes one.
ELEMENTS: dict[str, list[str]] = {
    "int4range": ["5", "10", "-1"],
    "int8range": ["2999999999", "3000000000"],
    "numrange": ["2.5", "9.5"],
    "tsrange": ["'2020-01-10 00:00:00'::timestamp", "'2020-02-01 00:00:00'::timestamp"],
    "tstzrange": ["'2020-01-10 00:00:00+00'::timestamptz", "'2020-02-01 00:00:00+00'::timestamptz"],
    "daterange": ["'2020-01-10'::date", "'2020-02-01'::date"],
}

MULTIRANGES: dict[str, list[str | None]] = {
    "int4multirange": ["{[1,3),[5,10)}", "{[9,20)}", "{}", "{(,2),[30,)}", "{[3,5)}", None],
    "int8multirange": ["{[1,3),[5,3000000000)}", "{[9,20)}", "{}", "{(,2)}", None],
    "nummultirange": ["{[1.5,2.50),[9,20)}", "{[10,11)}", "{}", "{(,2),[30,)}", None],
    "tsmultirange": [
        '{["2020-01-01 00:00:00","2020-02-01 00:00:00"),["2020-03-01 00:00:00",)}',
        '{["2020-01-15 00:00:00","2020-01-16 00:00:00")}',
        "{}",
        None,
    ],
    "tstzmultirange": [
        '{["2020-01-01 00:00:00+00","2020-02-01 00:00:00+00")}',
        '{["2020-01-15 00:00:00+00","2020-01-16 00:00:00+00")}',
        "{}",
        None,
    ],
    "datemultirange": [
        "{[2020-01-01,2020-02-01),[2020-03-01,2020-04-01)}",
        "{[2020-01-15,2020-01-16)}",
        "{}",
        None,
    ],
}

MULTIRANGE_RANGE = {
    "int4multirange": "int4range",
    "int8multirange": "int8range",
    "nummultirange": "numrange",
    "tsmultirange": "tsrange",
    "tstzmultirange": "tstzrange",
    "datemultirange": "daterange",
}
RANGE_MULTIRANGE = {range_name: name for name, range_name in MULTIRANGE_RANGE.items()}
RANGE_OPERATORS = ("&&", "@>", "<@", "<<", ">>", "&<", "&>", "-|-")
BOOLEAN_OPERATORS = (
    "&&",
    "@>",
    "<@",
    "<<",
    ">>",
    "&<",
    "&>",
    "-|-",
    "=",
    "<>",
    "<",
    "<=",
    ">",
    ">=",
)
VALUE_OPERATORS = ("+", "*", "-")
PARTS = ("lower", "upper", "isempty", "lower_inc", "upper_inc", "lower_inf", "upper_inf")


def text_of(value: Any) -> str | None:
    """An engine's value in PostgreSQL's text form, to compare with a recorded answer."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, datetime):
        utc = value.astimezone(timezone.utc) if value.tzinfo else value
        written = utc.strftime("%Y-%m-%d %H:%M:%S")
        fraction = f".{utc.microsecond:06d}".rstrip("0") if utc.microsecond else ""
        return written + fraction + ("+00" if value.tzinfo else "")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    return str(value)


_PAIRS = "FROM s.t AS a CROSS JOIN s.t AS b ORDER BY a.i, b.i"


def predicate_statements() -> list[str]:
    """Each boolean operator over every pair of the table's rows, in one read."""
    return [f"SELECT a.i, b.i, a.v {op} b.v AS r {_PAIRS}" for op in BOOLEAN_OPERATORS]


def pair_statements() -> list[str]:
    """Statements over every pair of the table's rows, in one read: nothing here raises."""
    return [*predicate_statements(), f"SELECT a.i, b.i, range_merge(a.v, b.v)::text AS r {_PAIRS}"]


def constructed_statement(type_name: str) -> str:
    """A multirange built from two columns: a NULL member is PostgreSQL's error, so none."""
    multirange = RANGE_MULTIRANGE[type_name]
    return (
        f"SELECT a.i, b.i, {multirange}(a.v, b.v)::text AS r FROM s.t AS a CROSS JOIN s.t AS b "
        "WHERE a.v IS NOT NULL AND b.v IS NOT NULL ORDER BY a.i, b.i"
    )


def value_statement(op: str, i: int, j: int) -> str:
    """A value operator over one pair of rows, a read each: a pair may raise PostgreSQL's error."""
    return f"SELECT (a.v {op} b.v)::text AS r FROM s.t AS a, s.t AS b WHERE a.i = {i} AND b.i = {j}"


def part_statements(type_name: str) -> list[str]:
    parts = [f"SELECT i, {part}(v){cast} AS r FROM s.t ORDER BY i" for part, cast in _parts()]
    parts.append("SELECT i, v::text AS r FROM s.t ORDER BY i")
    parts.append("SELECT i FROM s.t ORDER BY v, i")
    parts.append("SELECT i FROM s.t ORDER BY v DESC, i")
    parts.append("SELECT i, multirange(v)::text AS r FROM s.t ORDER BY i")
    parts.append("SELECT range_agg(v)::text AS r FROM s.t")
    parts.append("SELECT range_intersect_agg(v)::text AS r FROM s.t WHERE i < 2")
    parts.append(constructed_statement(type_name))
    first, second = SAMPLES[type_name][:2]
    parts.append(f"SELECT i FROM s.t WHERE v IN ('{first}', '{second}') ORDER BY i")
    parts.append(f"SELECT i FROM s.t WHERE v BETWEEN '{first}' AND '{second}' ORDER BY i")
    return (
        parts
        + [f"SELECT i, v @> {element} AS r FROM s.t ORDER BY i" for element in ELEMENTS[type_name]]
        + [f"SELECT i, {element} <@ v AS r FROM s.t ORDER BY i" for element in ELEMENTS[type_name]]
    )


def literal_statements(type_name: str) -> list[str]:
    """Each operator with a literal beside the column."""
    out: list[str] = []
    for sample in [s for s in SAMPLES[type_name] if s is not None][:3]:
        written = f"'{sample}'::{type_name}"
        out.extend(
            f"SELECT i, v {op} {written} AS r FROM s.t ORDER BY i" for op in BOOLEAN_OPERATORS
        )
    return out


def _parts() -> list[tuple[str, str]]:
    # A numeric or timestamp read as text, so the two engines' own value types compare.
    return [(p, "::text" if p in ("lower", "upper") else "") for p in PARTS]


def constructor_statements() -> list[str]:
    """A constructor over columns: table ``s.b`` of two bounds per type, some NULL."""
    return [
        f"SELECT i, {name}({lo}, {hi}, {flags})::text AS r FROM s.b ORDER BY i"
        for name, lo, hi in (
            ("int4range", "i4a", "i4b"),
            ("int8range", "i8a", "i8b"),
            ("numrange", "na", "nb"),
            ("tsrange", "tsa", "tsb"),
            ("tstzrange", "tza", "tzb"),
            ("daterange", "da", "db"),
        )
        for flags in ("'[)'", "'[]'", "'(]'", "'()'")
    ]


BOUND_COLUMNS = {
    "i": ("integer", ["0", "1", "2", "3", "4"]),
    "i4a": ("integer", ["1", "1", "3", "NULL", "5"]),
    "i4b": ("integer", ["10", "1", "3", "4", "NULL"]),
    "i8a": ("bigint", ["1", "1", "3000000000", "NULL", "5"]),
    "i8b": ("bigint", ["3000000000", "1", "3000000000", "4", "NULL"]),
    "na": ("numeric(10,2)", ["1.50", "1.50", "2.00", "NULL", "9.00"]),
    "nb": ("numeric(10,2)", ["2.50", "1.50", "2.00", "4.00", "NULL"]),
    "tsa": (
        "timestamp",
        ["'2020-01-01'", "'2020-01-01'", "'2020-01-02 12:30:00.5'", "NULL", "'2020-02-01'"],
    ),
    "tsb": (
        "timestamp",
        ["'2020-02-01'", "'2020-01-01'", "'2020-01-02 12:30:00.5'", "'2020-02-01'", "NULL"],
    ),
    "tza": (
        "timestamptz",
        [
            "'2020-01-01 00:00:00+00'",
            "'2020-01-01 00:00:00+00'",
            "'2020-01-02 12:30:00.5+00'",
            "NULL",
            "'2020-02-01 00:00:00+00'",
        ],
    ),
    "tzb": (
        "timestamptz",
        [
            "'2020-02-01 00:00:00+00'",
            "'2020-01-01 00:00:00+00'",
            "'2020-01-02 12:30:00.5+00'",
            "'2020-02-01 00:00:00+00'",
            "NULL",
        ],
    ),
    "da": ("date", ["'2020-01-01'", "'2020-01-01'", "'2020-01-02'", "NULL", "'2020-02-01'"]),
    "db": ("date", ["'2020-02-01'", "'2020-01-01'", "'2020-01-02'", "'2020-02-01'", "NULL"]),
}


def _run(conn, statement: str) -> list | str:  # noqa: ANN001 -- a psycopg connection
    """Each row's last column, the statement's answer (the rows' order is the statement's), or
    ERROR and the SQLSTATE."""
    res = conn.pgconn.exec_(statement.encode())
    if res.error_field(ord("C")) is not None:
        return "ERROR " + res.error_field(ord("C")).decode()
    last = res.nfields - 1
    return [
        None if res.get_value(row, last) is None else res.get_value(row, last).decode()
        for row in range(res.ntuples)
    ]


def _table(conn, type_name: str, samples: list[str | None]) -> None:  # noqa: ANN001
    conn.execute("DROP TABLE IF EXISTS s.t")
    conn.execute(f"CREATE TABLE s.t (i integer, v {type_name})")
    for i, sample in enumerate(samples):
        conn.execute("INSERT INTO s.t VALUES (%s, %s::" + type_name + ")", (i, sample))


def _bounds_table(conn) -> None:  # noqa: ANN001
    columns = ", ".join(f"{name} {kind}" for name, (kind, _) in BOUND_COLUMNS.items())
    conn.execute(f"CREATE TABLE s.b ({columns})")
    for row in zip(*(values for _, values in BOUND_COLUMNS.values())):
        conn.execute(f"INSERT INTO s.b VALUES ({', '.join(row)})")


def record(dsn: str) -> None:
    import psycopg

    conn = psycopg.connect(dsn, autocommit=True)
    conn.execute("SET TimeZone = 'UTC'")
    conn.execute("DROP SCHEMA IF EXISTS s CASCADE")
    conn.execute("CREATE SCHEMA s")
    answers: dict[str, dict[str, list | str]] = {}
    for type_name, samples in SAMPLES.items():
        _table(conn, type_name, samples)
        statements = pair_statements() + part_statements(type_name)
        answers[type_name] = {s: _run(conn, s) for s in statements + literal_statements(type_name)}
        count = range(len(samples))
        for op in VALUE_OPERATORS:
            # One row or one error for a pair; a pair with a NULL is one NULL row.
            answers[type_name][f"value {op}"] = [
                _run(conn, value_statement(op, i, j)) for i in count for j in count
            ]
    for type_name, samples in MULTIRANGES.items():
        _table(conn, type_name, samples)
        answers[type_name] = {s: _run(conn, s) for s in multirange_statements(type_name)}
    _bounds_table(conn)
    answers["constructors"] = {s: _run(conn, s) for s in constructor_statements()}
    out = Path(__file__).with_name("pg17_range_answers.py")
    head = (
        "# Copyright (c) 2026 Kenneth Stott\n"
        "#\n"
        "# This source code is licensed under the Business Source License 1.1\n"
        "# found in the LICENSE file in the root directory of this source tree.\n"
        "#\n"
        "# NOTICE: Use of this software for training artificial intelligence or\n"
        "# machine learning models is strictly prohibited without explicit written\n"
        "# permission from the copyright holder.\n\n"
        '"""What PostgreSQL 17 answered to each statement of tests/unit/range_corpus.py, by type:\n'
        'the rows\' text, or ERROR and the SQLSTATE. Recorded, not derived."""\n\n'
        "# Requirements: REQ-2171\n# fmt: off\n\n"
    )
    out.write_text(
        head + "RECORDED: dict = " + pprint.pformat(answers, width=120, compact=True) + "\n"
    )


def multirange_statements(type_name: str) -> list[str]:
    pairs = "FROM s.t AS a CROSS JOIN s.t AS b ORDER BY a.i, b.i"
    out = [f"SELECT a.i, b.i, a.v {op} b.v AS r {pairs}" for op in BOOLEAN_OPERATORS]
    out += [f"SELECT a.i, b.i, (a.v {op} b.v)::text AS r {pairs}" for op in VALUE_OPERATORS]
    out += [f"SELECT i, {part}(v){cast} AS r FROM s.t ORDER BY i" for part, cast in _parts()]
    out.append("SELECT i, range_merge(v)::text AS r FROM s.t ORDER BY i")
    out.append("SELECT i, v::text AS r FROM s.t ORDER BY i")
    out.append("SELECT i FROM s.t ORDER BY v, i")
    out.append("SELECT i, unnest(v)::text AS r FROM s.t ORDER BY i")
    out.append("SELECT range_agg(v)::text AS r FROM s.t")
    out.append("SELECT range_intersect_agg(v)::text AS r FROM s.t WHERE i < 2")
    range_name = MULTIRANGE_RANGE[type_name]
    for sample in [s for s in SAMPLES[range_name] if s is not None][:2]:
        written = f"'{sample}'::{range_name}"
        out += [f"SELECT i, v {op} {written} AS r FROM s.t ORDER BY i" for op in RANGE_OPERATORS]
        out += [f"SELECT i, {written} {op} v AS r FROM s.t ORDER BY i" for op in RANGE_OPERATORS]
    out += [f"SELECT i, v @> {e} AS r FROM s.t ORDER BY i" for e in ELEMENTS[range_name]]
    return out


if __name__ == "__main__":
    record(sys.argv[1])
