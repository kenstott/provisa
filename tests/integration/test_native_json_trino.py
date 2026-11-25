# Copyright (c) 2026 Kenneth Stott
# Canary: a6c3e915-4f72-4d0b-8e1a-2b7d9c5f0e38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1965 on a real Trino: a native array, row or map is handed over as JSON by the expression
Trino executes -- with exact decimals, timestamps at their instant, hex binary, and a non-finite
float refused."""

from __future__ import annotations

import json

import pytest
import sqlglot.expressions as exp
import trino

from provisa.core.ir_types import column_typing
from provisa.transpiler.native_json import NON_FINITE, NestedColumn, native_json_marker
from provisa.transpiler.transpile import transpile_to_trino

pytestmark = [pytest.mark.integration]

# Trino type as its information_schema spells it, a value, the JSON a read returns
_CASES = [
    ("array(varchar)", "ARRAY['a', NULL, 'b']", ["a", None, "b"]),
    ("array(array(integer))", "ARRAY[ARRAY[1, 2], ARRAY[3]]", [[1, 2], [3]]),
    ("array(bigint)", "ARRAY[BIGINT '9007199254740993']", [9007199254740993]),
    ("array(boolean)", "ARRAY[true, false]", [True, False]),
    (
        "array(decimal(20,2))",
        "ARRAY[DECIMAL '12345678901234567.89', DECIMAL '0.10']",
        ["12345678901234567.89", "0.10"],
    ),
    (
        "array(array(decimal(5,2)))",
        "ARRAY[ARRAY[DECIMAL '1.50'], ARRAY[DECIMAL '2.25', NULL]]",
        [["1.50"], ["2.25", None]],
    ),
    ("array(date)", "ARRAY[DATE '2026-01-02']", ["2026-01-02"]),
    (
        "array(timestamp(6))",
        "ARRAY[TIMESTAMP '2026-10-09 12:34:56.123456', NULL]",
        ["2026-10-09T12:34:56.123456", None],
    ),
    (
        "array(timestamp(6) with time zone)",
        "ARRAY[TIMESTAMP '2026-10-09 12:34:56.123456 +02:00']",
        ["2026-10-09T10:34:56.123456"],  # the same instant, in UTC
    ),
    ("array(varbinary)", "ARRAY[X'00FF']", ["\\x00ff"]),
    ("array(double)", "ARRAY[1.5E0, NULL]", [1.5, None]),
    (
        "array(uuid)",
        "ARRAY[UUID '6f1c2d3e-0000-4000-8000-000000000001']",
        ["6f1c2d3e-0000-4000-8000-000000000001"],
    ),
    # Depth 0: nothing inside is typed; the value is Trino's own cast to JSON.
    (
        "row(a integer, b array(varchar))",
        "CAST(ROW(1, ARRAY['x']) AS ROW(a integer, b array(varchar)))",
        {"a": 1, "b": ["x"]},
    ),
    ("map(varchar, integer)", "MAP(ARRAY['k'], ARRAY[1])", {"k": 1}),
    ("array(row(a integer))", "ARRAY[CAST(ROW(1) AS ROW(a integer))]", [{"a": 1}]),
]


def _read_sql(trino_type: str, value: str) -> str:
    typing = column_typing(trino_type, "trino")
    assert typing.data_type == "json" and typing.native_type
    nested = NestedColumn(typing.native_type, typing.array_depth, typing.element_type)
    marker = native_json_marker(exp.column("v", "t", quoted=True), nested)
    return transpile_to_trino(
        f'SELECT {marker.sql(dialect="postgres")} AS "v" FROM src AS "t"',
        # Not VALUES: Trino spreads a row-typed expression in a VALUES row into one column per field.
        generated=False,
    ).replace("FROM src", f"FROM (SELECT CAST({value} AS {trino_type}) UNION ALL SELECT NULL) ")


@pytest.mark.parametrize(("trino_type", "value", "expected"), _CASES)
def test_trino_hands_a_native_nested_value_over_as_json(trino_conn, trino_type, value, expected):
    cur = trino_conn.cursor()
    cur.execute(_read_sql(trino_type, value).replace('AS "t"', 'AS "t"("v")'))
    # A UNION ALL keeps no row order: the value's row and the NULL's, in whichever order.
    read = sorted((row[0] for row in cur.fetchall()), key=lambda v: v is None)
    # The exact JSON text Trino writes: nothing for Provisa to convert.
    assert read[0] == json.dumps(expected, separators=(",", ":"))
    assert read[1] is None  # a NULL column stays NULL


@pytest.mark.parametrize("value", ["nan()", "infinity()", "-infinity()"])
def test_trino_refuses_a_float_element_that_is_not_a_finite_number(trino_conn, value):
    cur = trino_conn.cursor()
    sql = _read_sql("array(double)", f"ARRAY[1.5E0, {value}]").replace('AS "t"', 'AS "t"("v")')
    with pytest.raises(trino.exceptions.TrinoUserError, match=NON_FINITE):
        cur.execute(sql)
        cur.fetchall()
