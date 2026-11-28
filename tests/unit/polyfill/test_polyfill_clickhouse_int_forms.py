# Copyright (c) 2026 Kenneth Stott
# Canary: 3b9e5f27-8d41-4c6a-a0f2-71c58e94d2b6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The integer arithmetic forms at their edges on the embedded engine (chdb) (REQ-2236; REQ-2171).

chdb's analyzer folds every branch of a CASE of constants, the unchosen one too: a branch that
divides by the zero a guard refuses, or narrows the value a range check refuses, raised
ClickHouse's own error (CH153 division by zero, CH407 convert overflow) before PostgreSQL's. Each
branch of these forms is total, so PostgreSQL's 22012 or 22003 is what is raised, and a value in
range is answered. Also: inetmi_int8 of the IPv6 zero address raises PostgreSQL's 22003, and
pg_lsn_larger of the greatest pg_lsn answers it.

The statements are the function support measure's own edge calls; the answers are PostgreSQL 17's."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

from typing import Any

import duckdb
import pytest

from provisa.core.pg_error import PostgresError, from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import RowRefused
from provisa.transpiler.transpile import transpile

_MIN = "'-9223372036854775808'::bigint"
_MAX = "'9223372036854775807'::bigint"


def _select(call: str) -> str:
    name, arguments = call.split("(", 1)
    return f'SELECT pg_catalog."{name}"({arguments} AS v'


def _rows(runtime: ClickHouseFederationRuntime, sql: str) -> list[tuple[Any, ...]]:
    return list(runtime.run_sync(transpile(sql, "clickhouse", generated=False)).iter_rows())


def _raised(runtime: ClickHouseFederationRuntime, sql: str) -> PostgresError:
    with pytest.raises(Exception) as raised:
        _rows(runtime, sql)
    if isinstance(raised.value, RowRefused):
        return PostgresError(raised.value.sqlstate, str(raised.value))
    error = from_engine(raised.value)
    assert error is not None, str(raised.value)[:300]
    return error


_ZERO = "22012", "division by zero"
_BIGINT = "22003", "bigint out of range"


@pytest.mark.parametrize(
    ("call", "error"),
    [
        ("int2div(3::smallint, '0'::smallint)", _ZERO),
        ("int2mod(3::smallint, '0'::smallint)", _ZERO),
        ("int4div(3::integer, '0'::integer)", _ZERO),
        ("int4mod(3::integer, '0'::integer)", _ZERO),
        ("int8mod(3::bigint, '0'::bigint)", _ZERO),
        ("int8div(3::bigint, '0'::bigint)", _ZERO),
        ("int24div(3::smallint, '0'::integer)", _ZERO),
        ("int28div(3::smallint, '0'::bigint)", _ZERO),
        ("int42div(3::integer, '0'::smallint)", _ZERO),
        ("int48div(3::integer, '0'::bigint)", _ZERO),
        (f"int28mi(3::smallint, {_MIN})", _BIGINT),
        (f"int28mul(3::smallint, {_MIN})", _BIGINT),
        (f"int28pl(3::smallint, {_MAX})", _BIGINT),
        (f"int48mi(3::integer, {_MIN})", _BIGINT),
        (f"int48mul(3::integer, {_MIN})", _BIGINT),
        (f"int48pl(3::integer, {_MAX})", _BIGINT),
        (f"int82mi({_MIN}, 3::smallint)", _BIGINT),
        (f"int82mul({_MIN}, 3::smallint)", _BIGINT),
        (f"int82pl({_MAX}, 3::smallint)", _BIGINT),
        (f"int84mi({_MIN}, 3::integer)", _BIGINT),
        (f"int84mul({_MIN}, 3::integer)", _BIGINT),
        (f"int84pl({_MAX}, 3::integer)", _BIGINT),
        (f"int8mi({_MIN}, 3::bigint)", _BIGINT),
        (f"int8mul({_MIN}, 3::bigint)", _BIGINT),
        (f"int8pl({_MAX}, 3::bigint)", _BIGINT),
        (f"int8div({_MIN}, (-1)::bigint)", _BIGINT),
        (f"int8abs({_MIN})", _BIGINT),
        (f"int8um({_MIN})", _BIGINT),
        (f"int8inc({_MAX})", _BIGINT),
        (f"int8dec({_MIN})", _BIGINT),
        ("int4pl(2147483647, 1)", ("22003", "integer out of range")),
        ("int2mul(32767::smallint, 2::smallint)", ("22003", "smallint out of range")),
        ("int4div((-2147483648)::integer, -1)", ("22003", "integer out of range")),
        ("inetmi_int8('::/0'::inet, 3::bigint)", ("22003", "result is out of range")),
    ],
)
def test_an_integer_forms_error_is_postgresqls(runtime, call, error):
    raised = _raised(runtime, _select(call))
    assert (raised.sqlstate, str(raised)) == error


@pytest.mark.parametrize(
    ("call", "answer"),
    [
        (f"int82div({_MIN}, 3::smallint)", -3074457345618258602),
        (f"int84div({_MIN}, 3::integer)", -3074457345618258602),
        (f"int8div({_MIN}, 3::bigint)", -3074457345618258602),
        (f"int8div({_MAX}, (-1)::bigint)", -9223372036854775807),
        (f"int8mod({_MIN}, (-1)::bigint)", 0),
        ("int4mod((-2147483648)::integer, -1)", 0),
        ("int4div(7, -2)", -3),
        ("int4mod(-7, 2)", -1),
        ("int2div(7::smallint, 2::smallint)", 3),
        (f"int8pl({_MAX}, (-3)::bigint)", 9223372036854775804),
        (f"int8mi({_MIN}, (-3)::bigint)", -9223372036854775805),
        (f"int8um({_MAX})", -9223372036854775807),
        (f"int8abs({_MIN} + 1)", 9223372036854775807),
        (f"int8inc({_MIN})", -9223372036854775807),
        (f"int8dec({_MAX})", 9223372036854775806),
        ("int2pl(32767::smallint, (-1)::smallint)", 32766),
        ("int4mul(-1073741824, 2)", -2147483648),
        ("int8div(3::bigint, NULL::bigint)", None),
        ("int4mod(NULL::integer, 0)", None),
        ("pg_lsn_larger('FFFFFFFF/FFFFFFFF'::pg_lsn, '0/16B3748'::pg_lsn)", "FFFFFFFF/FFFFFFFF"),
        ("pg_lsn_smaller('FFFFFFFF/FFFFFFFF'::pg_lsn, '0/16B3748'::pg_lsn)", "0/16B3748"),
    ],
)
def test_an_integer_form_in_range_answers_postgresqls(runtime, call, answer):
    (row,) = _rows(runtime, _select(call))
    assert row[0] == answer


@pytest.mark.parametrize(
    ("call", "answer"),
    [
        (f"int8div({_MIN}, 3::bigint)", -3074457345618258602),
        (f"int84div({_MIN}, 3::integer)", -3074457345618258602),
        (f"int8div({_MAX}, (-1)::bigint)", -9223372036854775807),
        ("pg_lsn_larger('FFFFFFFF/FFFFFFFF'::pg_lsn, '0/16B3748'::pg_lsn)", "FFFFFFFF/FFFFFFFF"),
        ("inetmi_int8('10.0.0.5'::inet, 3::bigint)", "10.0.0.2"),
    ],
)
def test_the_same_forms_answer_postgresqls_on_duckdb(call, answer):
    """DuckDB's int8div divides by ``//`` (its ``/`` is a double's): the divisor the forms share."""
    connection = duckdb.connect()
    (row,) = connection.execute(transpile(_select(call), "duckdb", generated=False)).fetchall()
    assert row[0] == answer


@pytest.mark.parametrize(
    "call",
    [
        "inetmi_int8('::/0'::inet, 3::bigint)",
        "inetmi_int8('0::0'::inet, 1::bigint)",
        "int8pl_inet((-3)::bigint, '::'::inet)",
        "inetpl('::/0'::inet, (-1)::bigint)",
    ],
)
def test_the_ipv6_zero_address_moved_below_zero_is_postgresqls_underflow_on_duckdb(call):
    connection = duckdb.connect()
    with pytest.raises(duckdb.Error) as raised:
        connection.execute(transpile(_select(call), "duckdb", generated=False))
    error = from_engine(raised.value)
    assert error is not None
    assert (error.sqlstate, str(error)) == ("22003", "result is out of range")
