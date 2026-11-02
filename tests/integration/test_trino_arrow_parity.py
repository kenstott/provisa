# Copyright (c) 2026 Kenneth Stott
# Canary: c62c6994-4db8-4557-9d72-46984a802045
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2194: a Trino read through the Zaychik Flight SQL proxy answers exactly as the Trino client
did -- the reader it replaces -- against a real coordinator and the proxy image this repository
builds (zaychik/Dockerfile with zaychik/trino-statement-protocol.patch).

Per type: the same rows (floats within 1e-14 relative, NaN equal to NaN), the same column types,
NULLs included. Then: the statement's session reaches Trino, Trino's errors arrive by name, and
parameters bind as the Trino client binds them.
"""

from __future__ import annotations

import math
import os
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import pytest
import trino

from provisa.executor.errors import FederationError
from provisa.executor.trino_flight import (
    TrinoContext,
    create_flight_connection,
    execute_trino_flight,
    execute_trino_flight_sync,
)

pytestmark = [pytest.mark.requires_trino, pytest.mark.integration]

_CONTEXT = TrinoContext(source="provisa/itest", catalog="system", schema="runtime")


@pytest.fixture(scope="module")
def trino_conn():
    conn = trino.dbapi.connect(
        host="localhost",
        port=int(os.environ["TRINO_PORT"]),
        user="provisa",
        source="provisa/itest",
        catalog="system",
        schema="runtime",
        http_scheme="http",
    )
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def flight_conn():
    conn = create_flight_connection(host="localhost", port=int(os.environ["ZAYCHIK_PORT"]))
    yield conn
    conn.close()


def _via_trino_client(conn, sql: str, params: list | None = None) -> tuple[list, list[str]]:
    cur = conn.cursor()
    cur.execute(sql, params) if params else cur.execute(sql)
    rows = cur.fetchall()
    types = [str(d[1]) for d in cur.description]
    # The trino-client read carried an instant at UTC (REQ-2050); so does the Flight read.
    zoned = [i for i, t in enumerate(types) if t.startswith("timestamp") and "with time zone" in t]
    out = []
    for row in rows:
        row = list(row)
        for i in zoned:
            if isinstance(row[i], datetime) and row[i].tzinfo is not None:
                row[i] = row[i].astimezone(timezone.utc)
        out.append(tuple(row))
    return out, types


def _same(ours: Any, theirs: Any) -> bool:
    if isinstance(theirs, float) and isinstance(ours, float):
        if math.isnan(theirs):
            return math.isnan(ours)
        if math.isinf(theirs):
            return ours == theirs
        return ours == theirs or abs(ours - theirs) <= 1e-14 * max(abs(ours), abs(theirs))
    if isinstance(theirs, dict):
        return ours.keys() == theirs.keys() and all(_same(ours[k], theirs[k]) for k in theirs)
    if isinstance(theirs, (list, tuple)):
        return len(ours) == len(theirs) and all(_same(a, b) for a, b in zip(ours, theirs))
    return type(ours) is type(theirs) and ours == theirs


#: One expression per Trino type the Flight read must answer as the Trino client does.
_CORPUS = [
    "true",
    "TINYINT '-7'",
    "SMALLINT '300'",
    "70000",
    "BIGINT '1099511627776'",
    "REAL '0.1'",
    "REAL '3.4028235e38'",
    "CAST(nan() AS real)",
    "DOUBLE '0.3333333333333333'",
    "1e300",
    "nan()",
    "infinity()",
    "-infinity()",
    "DECIMAL '1.50'",
    "DECIMAL '-12345678901234567890.123456789'",
    "'héllo'",
    "CAST('ab' AS char(3))",
    "CAST('x' AS varchar(5))",
    "X'0102ff'",
    "DATE '2024-02-29'",
    "DATE '1000-01-01'",
    "TIME '01:02:03'",
    "TIME '01:02:03.123456'",
    "TIME '23:59:59.9999999'",
    "TIMESTAMP '2024-01-02 03:04:05'",
    "TIMESTAMP '2024-01-02 03:04:05.123'",
    "TIMESTAMP '2024-01-02 03:04:05.123456'",
    "TIMESTAMP '2024-01-02 03:04:05.123456789'",
    "TIMESTAMP '1969-12-31 23:59:59.999999'",
    "TIMESTAMP '2024-01-02 03:04:05.123 +02:00'",
    "TIMESTAMP '2024-01-02 03:04:05.123456 UTC'",
    "TIMESTAMP '2024-11-03 01:30:00.000000 America/New_York'",
    "TIME '03:04:05.123 +02:00'",
    "JSON '{\"a\":[1,2]}'",
    "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'",
    "IPADDRESS '10.0.0.1'",
    "IPADDRESS '2001:db8::1'",
    "INTERVAL '1 02:03:04.005' DAY TO SECOND",
    "INTERVAL '-3' DAY",
    "INTERVAL '1-2' YEAR TO MONTH",
    "ARRAY[1, NULL, 3]",
    "ARRAY['a', 'b']",
    "ARRAY[TIMESTAMP '2024-01-02 03:04:05.123456789']",
    "MAP(ARRAY['a', 'b'], ARRAY[0.5e0, nan()])",
    "MAP(ARRAY[1, 2], ARRAY[DATE '2024-01-02', NULL])",
    "CAST(ROW(7, X'010203') AS ROW(a bigint, b varbinary))",
    "CAST(ROW(1, ROW('x', DECIMAL '2.5')) AS ROW(i integer, r ROW(s varchar, d decimal(3,1))))",
    "ROW(1, 'unnamed')",
    "NULL",
]


@pytest.mark.parametrize("expr", _CORPUS)
def test_each_type_answers_as_the_trino_client(trino_conn, flight_conn, expr):
    sql = f"SELECT v FROM (VALUES ({expr}), (NULL)) AS t(v)"
    theirs, their_types = _via_trino_client(trino_conn, sql)
    ours = execute_trino_flight(flight_conn, sql, context=_CONTEXT)
    assert ours.column_types == their_types
    assert _same(ours.rows, theirs), (ours.rows, theirs)


def test_a_multi_batch_result_answers_as_the_trino_client(trino_conn, flight_conn):
    sql = (
        "SELECT n, n * 0.5e0 AS half, CAST(n AS varchar) AS s, "
        "from_unixtime_nanos(n * 1000001001) AS ts, DECIMAL '0.01' * n AS d "
        "FROM UNNEST(sequence(1, 25000)) AS t(n) ORDER BY n"
    )
    theirs, their_types = _via_trino_client(trino_conn, sql)
    ours = execute_trino_flight_sync(flight_conn, sql, context=_CONTEXT)
    assert ours.column_types == their_types
    assert _same(ours.rows, theirs)


# -- the session -------------------------------------------------------------------------------


def test_session_properties_reach_trino(flight_conn):
    context = TrinoContext(
        source="provisa/itest",
        catalog="system",
        schema="runtime",
        properties={"query_max_run_time": "3h", "join_distribution_type": "BROADCAST"},
    )
    result = execute_trino_flight(flight_conn, "SHOW SESSION", context=context)
    values = {row[0]: row[1] for row in result.rows}
    assert values["query_max_run_time"] == "3h"
    assert values["join_distribution_type"] == "BROADCAST"
    # A statement without them is a session of its own: nothing carried over.
    plain = execute_trino_flight(flight_conn, "SHOW SESSION", context=_CONTEXT)
    defaults = {row[0]: (row[1], row[2]) for row in plain.rows}
    assert defaults["query_max_run_time"][0] == defaults["query_max_run_time"][1]


def test_catalog_schema_and_source_reach_trino(trino_conn, flight_conn):
    marker = f"marker-{uuid.uuid4().hex}"
    context = TrinoContext(source="provisa/itest-org", catalog="system", schema="metadata")
    (current,) = execute_trino_flight(
        flight_conn, f"SELECT current_catalog, current_schema, '{marker}'", context=context
    ).rows
    assert current == ("system", "metadata", marker)
    seen, _ = _via_trino_client(
        trino_conn,
        'SELECT source, "user" FROM system.runtime.queries '
        f"WHERE query LIKE '%{marker}%' AND query NOT LIKE '%system.runtime.queries%'",
    )
    assert seen == [("provisa/itest-org", "provisa")]


def test_the_query_timeout_is_enforced(flight_conn):
    context = TrinoContext(source="provisa/itest", properties={"query_max_execution_time": "1s"})
    with pytest.raises(FederationError) as raised:
        execute_trino_flight(
            flight_conn,
            "SELECT count(*) FROM UNNEST(sequence(1, 1000000)) a(x) "
            "CROSS JOIN UNNEST(sequence(1, 1000000)) b(y)",
            context=context,
        )
    assert raised.value.error_name == "EXCEEDED_TIME_LIMIT"


# -- errors -------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM no_such_catalog.s.t",
        "SELECT x FROMM t",
        "SELECT 1 / 0",
        "SELECT CAST('x' AS integer)",
        "SELECT * FROM system.runtime.no_such_table",
    ],
)
def test_errors_arrive_by_trinos_name(trino_conn, flight_conn, sql):
    with pytest.raises(trino.exceptions.TrinoQueryError) as theirs:
        _via_trino_client(trino_conn, sql)
    with pytest.raises(FederationError) as ours:
        execute_trino_flight(flight_conn, sql, context=_CONTEXT)
    assert ours.value.error_name == theirs.value.error_name
    assert ours.value.error_type == theirs.value.error_type
    assert ours.value.message == theirs.value.message
    assert ours.value.query_id


# -- parameters ---------------------------------------------------------------------------------


_PARAMS = [
    7,
    1.5,
    "o'brien",
    None,
    True,
    b"\x01\xff",
    date(2024, 2, 29),
    datetime(2024, 1, 2, 3, 4, 5, 123456),
    datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=timezone.utc),
    Decimal("12.340"),
    [1, 2],
]


@pytest.mark.parametrize("param", _PARAMS, ids=repr)
def test_parameters_bind_as_the_trino_client_binds_them(trino_conn, flight_conn, param):
    theirs, their_types = _via_trino_client(trino_conn, "SELECT ?, ? IS NULL", [param, param])
    ours = execute_trino_flight(flight_conn, "SELECT $1, $1 IS NULL", [param], context=_CONTEXT)
    assert ours.column_types == their_types
    assert _same(ours.rows, theirs), (ours.rows, theirs)
