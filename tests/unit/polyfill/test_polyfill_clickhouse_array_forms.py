# Copyright (c) 2026 Kenneth Stott
# Canary: 3d8f1c27-6b4e-4a9d-b0e5-91c7a2f4d618
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's forms of the defects of the first measured Conformance (functions) run on
ClickHouse after the hand-edited counts went (run 45453337906; REQ-2236, polyfill grid).

Each statement is the measure's own call of an overload (function_support.calls and edge_calls),
translated for ClickHouse and run on the embedded engine (chdb, ClickHouse 26), its answer or its
error compared with PostgreSQL's (pixeltable_pgserver's PostgreSQL 16.11, which answers these
alike with 17):

- array_cat, which had no form;
- a comparison of an array of two dimensions with one of one (array_eq .. array_ge, btarraycmp):
  each array's key, its elements then its dimensions, as array.c's array_cmp orders arrays;
  array_larger and array_smaller of arrays of different dimensions refused by name, stated;
- PostgreSQL's own refusals of an array of two dimensions (array_position, array_positions,
  width_bucket's thresholds, array_append); array_cat of such arrays as array.c answers it;
- hash_array of an array of two dimensions: of its elements, as array.c hashes them;
- width_bucket of an infinite float8 operand (answered), a NaN and an infinite bound (2201G);
- json_array_length and jsonb_array_length of no array: PostgreSQL's 22023 and message;
- isfinite of an interval past int32 months: 22008; of a timestamp outside DateTime64's bound (a
  year BC, past 9999, infinity): timestamp_in's 0A000, now stated; of an instant literal
  DateTime64 holds none of (a year BC, past 9999, infinity): PostgreSQL's answer
  (:func:`test_an_edge_instant_answers_postgresql`), and timestamptz_larger and
  timestamptz_smaller of one: the other operand where the call picks it, PostgreSQL's answer;
  0A000, stated, where the answer is the edge itself (REQ-2236, amended 2026-11-24).

The run's 24.3-only errors (cardinality, array_larger's supertype, array_position of a start,
array_sample and trim_array's arraySlice of a Nullable count, array_agg of a NULL) do not occur on
ClickHouse 26: :func:`test_answered_on_clickhouse_26` holds them there."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import datetime
from typing import Any

import pytest

from provisa.core.pg_error import PostgresError, from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused, RowRefused
from provisa.transpiler.polyfill import GRID
from provisa.transpiler.transpile import transpile


def _rows(runtime: ClickHouseFederationRuntime, sql: str) -> list[tuple[Any, ...]]:
    return list(runtime.run_sync(transpile(sql, "clickhouse", generated=False)).iter_rows())


def _one(runtime: ClickHouseFederationRuntime, sql: str) -> Any:
    (row,) = _rows(runtime, sql)
    return row[0]


def _raised(runtime: ClickHouseFederationRuntime, sql: str) -> PostgresError:
    with pytest.raises(Exception) as raised:
        _rows(runtime, sql)
    if isinstance(raised.value, (RowRefused, FunctionRefused)):
        return PostgresError(raised.value.sqlstate, str(raised.value))
    error = from_engine(raised.value)
    assert error is not None, str(raised.value)[:300]
    return error


def _stated_on_clickhouse(sqlstate: str, message: str) -> bool:
    """Whether a limitation of a grid row on ClickHouse states the error: its refusal, or a raise
    its text names (scripts/conformance/function_edges.stated_errors)."""
    for feature in GRID.features:
        for limitation in feature.read.limitations.get("clickhouse", ()):
            refusal = limitation.refusal
            if refusal is not None and (refusal.sqlstate, refusal.message) == (sqlstate, message):
                return True
            if f"raises {sqlstate} ({message})" in limitation.text:
                return True
            if f"raise {sqlstate} ({message})" in limitation.text:
                return True
    return False


_TWO = "'{{1,2},{3,4}}'::integer[]"
_ONE = "'{1,2,3}'::integer[]"
_TSTZ = "'2024-06-15 12:30:45+00'::timestamp with time zone"


def _call(function: str, *args: str) -> str:
    return f'SELECT pg_catalog."{function}"({", ".join(args)}) AS v'


ANSWERED = [
    (_call("array_cat", _ONE, _ONE), [1, 2, 3, 1, 2, 3]),
    (_call("array_cat", "'{1,NULL}'::integer[]", "'{}'::integer[]"), [1, None]),
    (_call("array_cat", "'{1,2}'::integer[]", "'{3}'::bigint[]"), [1, 2, 3]),
    (_call("array_cat", _TWO, "'{{5,6}}'::integer[]"), [[1, 2], [3, 4], [5, 6]]),
    (_call("array_cat", _TWO, "'{5,6}'::integer[]"), [[1, 2], [3, 4], [5, 6]]),
    (_call("array_cat", "'{5,6}'::integer[]", _TWO), [[5, 6], [1, 2], [3, 4]]),
    (_call("array_cat", "'{}'::integer[]", _TWO), [[1, 2], [3, 4]]),
    (_call("array_eq", _TWO, _ONE), False),
    (_call("array_eq", _ONE, _TWO), False),
    (_call("array_eq", _TWO, _TWO), True),
    (_call("array_eq", "'{}'::integer[]", "'{{1}}'::integer[]"), False),
    (_call("array_ne", _TWO, _ONE), True),
    (_call("array_ne", _ONE, _TWO), True),
    (_call("array_lt", _TWO, _ONE), False),
    (_call("array_lt", _ONE, _TWO), True),
    (_call("array_lt", _TWO, "'{{1,2,3,4}}'::integer[]"), False),
    (_call("array_le", _TWO, _ONE), False),
    (_call("array_le", _ONE, _TWO), True),
    (_call("array_gt", _TWO, _ONE), True),
    (_call("array_gt", _ONE, _TWO), False),
    (_call("array_ge", _TWO, _ONE), True),
    (_call("array_ge", _ONE, _TWO), False),
    (_call("btarraycmp", _TWO, _ONE), 1),
    (_call("btarraycmp", _ONE, _TWO), -1),
    (_call("btarraycmp", _TWO, "'{{1,2,3,4}}'::integer[]"), 1),
    (_call("btarraycmp", "'{{1,NULL},{3,4}}'::integer[]", "'{1,2,3,4}'::integer[]"), 1),
    (_call("array_larger", _TWO, "'{{1,2,3,4}}'::integer[]"), [[1, 2], [3, 4]]),
    (_call("array_smaller", _TWO, "'{{1,2,3,4}}'::integer[]"), [[1, 2, 3, 4]]),
    (_call("hash_array", _TWO), 1786625125),
    (_call("hash_array", "'{{{1},{2}},{{3},{4}}}'::integer[]"), 1786625125),
    (_call("hash_array", "'{{a,b},{c,NULL}}'::text[]"), -306943274),
    (_call("hash_array_extended", _TWO, "3::bigint"), -8612223870323964240),
    (
        _call("width_bucket", *(f"{v}::double precision" for v in ("'-Infinity'", "0", "10")), "5"),
        0,
    ),
    (
        _call("width_bucket", *(f"{v}::double precision" for v in ("'Infinity'", "0", "10")), "5"),
        6,
    ),
    (
        _call("width_bucket", *(f"{v}::double precision" for v in ("'Infinity'", "10", "0")), "5"),
        0,
    ),
    (_call("width_bucket", *(f"{v}::double precision" for v in ("5.5", "0", "10")), "5"), 3),
    (_call("json_array_length", "'[1,2,3]'::json"), 3),
    (_call("json_array_length", "NULL::json"), None),
    # An edge timestamp is held (polyfill.held_instants, REQ-2236), so isfinite answers it.
    (_call("isfinite", "'4713-01-01 00:00:00 BC'::timestamp without time zone"), True),
    (_call("isfinite", "'294276-12-31 23:59:59.999999'::timestamp without time zone"), True),
    (_call("isfinite", "'infinity'::timestamp without time zone"), False),
]


@pytest.mark.parametrize(("sql", "want"), ANSWERED)
def test_answered_as_postgresql_answers(runtime, sql, want):
    assert _one(runtime, sql) == want


RAISED_AS_POSTGRESQL = [
    (_call("array_position", _TWO, "3::integer"), "0A000",
     "searching for elements in multidimensional arrays is not supported"),
    (_call("array_positions", _TWO, "3::integer"), "0A000",
     "searching for elements in multidimensional arrays is not supported"),
    (_call("width_bucket", "3::integer", _TWO), "2202E",
     "thresholds must be one-dimensional array"),
    (_call("array_append", _TWO, "3::integer"), "22000",
     "argument must be empty or one-dimensional array"),
    (_call("array_cat", _TWO, _ONE), "2202E", "cannot concatenate incompatible arrays"),
    (_call("width_bucket", *(f"{v}::double precision" for v in ("'NaN'", "0", "10")), "5"),
     "2201G", "operand, lower bound, and upper bound cannot be NaN"),
    (_call("width_bucket", *(f"{v}::double precision" for v in ("1", "'-Infinity'", "10")), "5"),
     "2201G", "lower and upper bounds must be finite"),
    (_call("width_bucket", *(f"{v}::double precision" for v in ("1", "0", "10")), "0"),
     "2201G", "count must be greater than zero"),
    (_call("json_array_length", "'{}'::json"), "22023", "cannot get array length of a non-array"),
    (_call("json_array_length", "'\"日本語😀\"'::json"), "22023",
     "cannot get array length of a scalar"),
    (_call("json_array_length", "'12345678901234567890123456789'::json"), "22023",
     "cannot get array length of a scalar"),
    (_call("json_array_length", "'null'::json"), "22023", "cannot get array length of a scalar"),
    (_call("jsonb_array_length", "'{}'::jsonb"), "22023",
     "cannot get array length of a non-array"),
    (_call("jsonb_array_length", "'\"日本語😀\"'::jsonb"), "22023",
     "cannot get array length of a scalar"),
    (_call("isfinite", "'178956971 years'::interval"), "22008", "interval out of range"),
    (_call("interval_um", "'178956971 years'::interval"), "22008", "interval out of range"),
]  # fmt: skip


@pytest.mark.parametrize(("sql", "sqlstate", "message"), RAISED_AS_POSTGRESQL)
def test_raised_as_postgresql_raises(runtime, sql, sqlstate, message):
    error = _raised(runtime, sql)
    assert error.sqlstate == sqlstate and message in str(error), str(error)


_BC = "'4713-01-01 00:00:00+00 BC'::timestamp with time zone"
_FAR = "'294276-12-31 23:59:59.999999+00'::timestamp with time zone"
_INFINITE = "'infinity'::timestamp with time zone"
_MINUS_INFINITE = "'-infinity'::timestamp with time zone"
_BEFORE = "'1800-01-01 00:00:00+00'::timestamp with time zone"
_AFTER = "'2300-01-01 00:00:00+00'::timestamp with time zone"
_NULL_TSTZ = "NULL::timestamp with time zone"


def _outside(function: str) -> str:
    """The raise of a call whose answer is outside DateTime64's bound (instants.out_of_bound)."""
    return (
        f"{function}() of a value outside 1900-01-01 00:00:00 .. 2299-12-31 23:59:59.999999 UTC "
        "has no exact form on clickhouse"
    )


WITHIN_BOUND = [
    (_call("array_larger", _TWO, _ONE), "0A000",
     "array_larger() of arrays of different dimensions has no form on clickhouse"),
    (_call("array_larger", _ONE, _TWO), "0A000",
     "array_larger() of arrays of different dimensions has no form on clickhouse"),
    (_call("array_smaller", _TWO, _ONE), "0A000",
     "array_smaller() of arrays of different dimensions has no form on clickhouse"),
    (_call("array_smaller", _ONE, _TWO), "0A000",
     "array_smaller() of arrays of different dimensions has no form on clickhouse"),
    (f"SELECT cardinality(pg_catalog.\"array_cat\"({_TWO}, '{{{{5,6}}}}'::integer[])) AS v",
     "0A000", "array_cat() of an array of more than one dimension read by another call has no "
     "form on clickhouse"),
    # The answer is the edge itself, which DateTime64 holds none of.
    (_call("timestamptz_larger", _FAR, _TSTZ), "0A000", _outside("timestamptz_larger")),
    (_call("timestamptz_larger", _TSTZ, _FAR), "0A000", _outside("timestamptz_larger")),
    (_call("timestamptz_larger", _TSTZ, _INFINITE), "0A000", _outside("timestamptz_larger")),
    (_call("timestamptz_larger", _INFINITE, _TSTZ), "0A000", _outside("timestamptz_larger")),
    (_call("timestamptz_larger", _BC, _MINUS_INFINITE), "0A000", _outside("timestamptz_larger")),
    (_call("timestamptz_larger", _BC, _FAR), "0A000", _outside("timestamptz_larger")),
    (_call("timestamptz_smaller", _BC, _TSTZ), "0A000", _outside("timestamptz_smaller")),
    (_call("timestamptz_smaller", _TSTZ, _BC), "0A000", _outside("timestamptz_smaller")),
    (_call("timestamptz_smaller", _MINUS_INFINITE, _TSTZ), "0A000", _outside("timestamptz_smaller")),
    (_call("timestamptz_smaller", _TSTZ, _MINUS_INFINITE), "0A000", _outside("timestamptz_smaller")),
    (_call("timestamptz_smaller", _FAR, _INFINITE), "0A000", _outside("timestamptz_smaller")),
    (_call("timestamptz_smaller", _BC, _FAR), "0A000", _outside("timestamptz_smaller")),
]  # fmt: skip


@pytest.mark.parametrize(("sql", "sqlstate", "message"), WITHIN_BOUND)
def test_raised_within_a_stated_bound(runtime, sql, sqlstate, message):
    """An error a limitation of a grid row on ClickHouse states, its SQLSTATE and its message:
    within the bound the measure holds the row to (function_support._held_to_its_bound)."""
    error = _raised(runtime, sql)
    assert error.sqlstate == sqlstate and message in str(error), str(error)
    assert _stated_on_clickhouse(sqlstate, message)


_AT = datetime.datetime(2024, 6, 15, 12, 30, 45, tzinfo=datetime.timezone.utc)

EDGE_ANSWERS = [
    # The edge is on the side the call does not pick: the other operand, PostgreSQL's answer.
    (_call("timestamptz_larger", _BC, _TSTZ), _AT),
    (_call("timestamptz_larger", _TSTZ, _BC), _AT),
    (_call("timestamptz_larger", _MINUS_INFINITE, _TSTZ), _AT),
    (_call("timestamptz_larger", _TSTZ, _MINUS_INFINITE), _AT),
    (_call("timestamptz_larger", _BEFORE, _TSTZ), _AT),
    (_call("timestamptz_smaller", _FAR, _TSTZ), _AT),
    (_call("timestamptz_smaller", _TSTZ, _FAR), _AT),
    (_call("timestamptz_smaller", _INFINITE, _TSTZ), _AT),
    (_call("timestamptz_smaller", _TSTZ, _INFINITE), _AT),
    (_call("timestamptz_smaller", _TSTZ, _AFTER), _AT),
    # STRICT: a NULL beside an edge answers NULL.
    (_call("timestamptz_larger", _NULL_TSTZ, _BC), None),
    (_call("timestamptz_smaller", _INFINITE, _NULL_TSTZ), None),
    # A value the engine computes beside an edge: that value.
    (
        f'SELECT pg_catalog."timestamptz_larger"({_BC}, v) AS v '
        f"FROM (VALUES ({_TSTZ})) AS t(v)",
        _AT,
    ),
    # isfinite of a value DateTime64 holds none of: PostgreSQL's answer, the value never written.
    (_call("isfinite", _BC), True),
    (_call("isfinite", _FAR), True),
    (_call("isfinite", _INFINITE), False),
    (_call("isfinite", _MINUS_INFINITE), False),
    (_call("isfinite", _BEFORE), True),
]  # fmt: skip


@pytest.mark.parametrize(("sql", "want"), EDGE_ANSWERS)
def test_an_edge_instant_answers_postgresql(runtime, sql, want):
    """REQ-2236, amended 2026-11-24: a form that fails only for edge values answers them with
    CASE forms of PostgreSQL's answer; refused by name before."""
    assert _one(runtime, sql) == want


ON_CLICKHOUSE_26 = [
    (_call("cardinality", _ONE), 3),
    (_call("array_larger", _ONE, _ONE), [1, 2, 3]),
    (_call("array_smaller", _ONE, _ONE), [1, 2, 3]),
    (_call("array_position", _ONE, "3::integer", "3::integer"), 3),
    (_call("trim_array", _ONE, "3::integer"), []),
    ('SELECT pg_catalog."array_agg"(NULL::integer) AS v', [None]),
]


@pytest.mark.parametrize(("sql", "want"), ON_CLICKHOUSE_26)
def test_answered_on_clickhouse_26(runtime, sql, want):
    """The run's errors on clickhouse-server 24.3 alone: ClickHouse 26 answers each."""
    assert _one(runtime, sql) == want


def test_array_sample_on_clickhouse_26(runtime):
    """array_sample's arraySlice of a Nullable count, 24.3's error: a sample of the array."""
    answer = _one(runtime, _call("array_sample", _ONE, "2::integer"))
    assert len(answer) == 2 and set(answer) <= {1, 2, 3}
