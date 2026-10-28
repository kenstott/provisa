# Copyright (c) 2026 Kenneth Stott
# Canary: 855ec605-73b2-49a4-be75-59c0554802f2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Flight SQL bulk ingest (REQ-2175), through the ADBC Flight SQL driver against a server on a
loopback port: rows ingested into a table the model declares are written as the caller through
the one pipeline, a batch of INSERT ... VALUES at a time, and answered their count. A table the
model does not declare is not created, and one it declares is not replaced: nothing is defined
through a query protocol (REQ-1926). Before, CommandStatementIngest was not served. The pipeline
is stubbed here to show what it is asked."""

# Requirements: REQ-2175, REQ-1926

from __future__ import annotations

import types

import adbc_driver_flightsql.dbapi as flightsql
import adbc_driver_manager
import pyarrow as pa
import pytest

from provisa.api.flight import flightsql_catalog
from provisa.api.flight.server import ProvisaFlightServer
from provisa.executor.result import QueryResult


class _State:
    auth_config = None
    auth_middleware_active = False
    multitenancy = False
    org_id = "default"
    settings_overrides: dict = {"derived_roles": []}


def _catalog(state, role):  # noqa: ARG001  # role_catalog's signature
    """A role served sales.orders (id, note)."""
    return flightsql_catalog.RoleCatalog(
        "provisa",
        [flightsql_catalog._Table("provisa", "sales", "orders", 16385)],
        {16385: [("id", "integer"), ("note", "text")]},
        [],
        {},
    )


@pytest.fixture
def served(monkeypatch):
    """A ProvisaFlightServer on a loopback port whose pipeline governs and runs each statement it
    is asked (recorded), answered one affected row per ``VALUES`` row."""
    ran: list[tuple[str, str]] = []

    def _as_caller(self, request, body):  # noqa: ARG001  # the server's signature
        request.setdefault("role", "anonymous")
        return body()

    async def _govern(sql, role_id, state, **kwargs):  # noqa: ARG001  # the pipeline's signature
        return types.SimpleNamespace(sql=sql, role=role_id)

    async def _execute(plan, state):  # noqa: ARG001  # the pipeline's signature
        ran.append((plan.sql, plan.role))
        rows = plan.sql.partition("VALUES")[2].count("),") + 1
        return QueryResult(rows=[], column_names=[], rowcount=rows)

    monkeypatch.setattr(ProvisaFlightServer, "_as_ticket_caller", _as_caller)
    monkeypatch.setattr(flightsql_catalog, "role_catalog", _catalog)
    monkeypatch.setattr("provisa.pgwire._pipeline.govern_batch_final_plan_with_fn", _govern)
    monkeypatch.setattr("provisa.pgwire._pipeline.require_governed_plan", lambda plan: None)
    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _execute)
    server = ProvisaFlightServer(_State(), location="grpc://127.0.0.1:0")
    try:
        yield types.SimpleNamespace(uri=f"grpc://127.0.0.1:{server.port}", ran=ran)
    finally:
        server.shutdown()


def _connect(uri: str):
    return flightsql.connect(
        uri, db_kwargs={"adbc.flight.sql.rpc.call_header.x-provisa-role": "seller"}
    )


_ROWS = pa.table({"id": pa.array([1, 2, 3], pa.int32()), "note": ["a", None, "it's"]})


@pytest.mark.parametrize("mode", ["append", "create_append"])
def test_rows_ingested_into_a_declared_table_are_written_as_the_caller(served, mode):
    with _connect(served.uri) as conn, conn.cursor() as cur:
        assert cur.adbc_ingest("orders", _ROWS, mode=mode) == 3
    assert served.ran == [
        (
            "INSERT INTO \"orders\" (\"id\", \"note\") VALUES (1, 'a'), (2, NULL), (3, 'it''s')",
            "seller",
        )
    ]


def test_a_large_ingest_is_written_a_batch_at_a_time(served):
    rows = pa.table({"id": pa.array(range(1200), pa.int32())})
    with _connect(served.uri) as conn, conn.cursor() as cur:
        assert cur.adbc_ingest("orders", rows, mode="append") == 1200
    assert len(served.ran) == 3  # 500, 500, 200


def test_a_table_in_the_schema_named_is_written_there(served):
    with _connect(served.uri) as conn, conn.cursor() as cur:
        cur.adbc_ingest("orders", _ROWS.select(["id"]), mode="append", db_schema_name="sales")
    ((sql, _),) = served.ran
    assert sql.startswith('INSERT INTO "sales"."orders" ("id") VALUES')


@pytest.mark.parametrize(
    ("mode", "status", "message"),
    [
        ("create", adbc_driver_manager.AdbcStatusCode.INVALID_ARGUMENT, "already exists"),
        ("replace", adbc_driver_manager.AdbcStatusCode.NOT_IMPLEMENTED, "defined in the model"),
    ],
)
def test_a_declared_table_is_not_created_again_or_replaced(served, mode, status, message):
    with (
        _connect(served.uri) as conn,
        conn.cursor() as cur,
        pytest.raises(adbc_driver_manager.Error, match=message) as refused,
    ):
        cur.adbc_ingest("orders", _ROWS, mode=mode)
    assert refused.value.status_code == status
    assert served.ran == []


@pytest.mark.parametrize(
    ("mode", "status", "message"),
    [
        ("append", adbc_driver_manager.AdbcStatusCode.NOT_FOUND, "table 'invoices' does not exist"),
        ("create", adbc_driver_manager.AdbcStatusCode.NOT_IMPLEMENTED, "defined in the model"),
        (
            "create_append",
            adbc_driver_manager.AdbcStatusCode.NOT_IMPLEMENTED,
            "defined in the model",
        ),
    ],
)
def test_a_table_the_model_does_not_declare_is_not_found_or_created(served, mode, status, message):
    with (
        _connect(served.uri) as conn,
        conn.cursor() as cur,
        pytest.raises(adbc_driver_manager.Error, match=message) as refused,
    ):
        cur.adbc_ingest("invoices", _ROWS, mode=mode)
    assert refused.value.status_code == status
    assert served.ran == []


def test_a_column_the_table_does_not_have_is_refused_by_name(served):
    with (
        _connect(served.uri) as conn,
        conn.cursor() as cur,
        pytest.raises(adbc_driver_manager.Error, match="orders has no column 'colour'"),
    ):
        cur.adbc_ingest("orders", pa.table({"colour": ["red"]}), mode="append")
    assert served.ran == []


def test_an_append_to_an_undeclared_table_says_it_does_not_exist(served):
    """In PostgreSQL's words, which a client reads a missing table by."""
    with (
        _connect(served.uri) as conn,
        conn.cursor() as cur,
        pytest.raises(adbc_driver_manager.Error, match="table 'invoices' does not exist"),
    ):
        cur.adbc_ingest("invoices", _ROWS, mode="append")


def test_zoned_and_nanosecond_timestamps_are_written_as_their_instants():
    """A zoned value is written as its instant in UTC -- 9999-12-31 23:59 UTC is a year past
    9999 in Asia/Tokyo -- and a nanosecond value at the microsecond PostgreSQL holds, a null as
    NULL."""
    import datetime as dt

    from provisa.api.flight import flightsql as fsql

    instant = dt.datetime(9999, 12, 31, 23, 59, tzinfo=dt.UTC)
    rows = pa.table(
        {
            "at": pa.array([instant], pa.timestamp("us", tz="Asia/Tokyo")),
            "ns": pa.array([1_700_000_000_123_456_789], pa.timestamp("ns")),
            "gone": pa.array([None], pa.timestamp("ns")),
            "old": pa.array([-9_223_372_036_854_775_000], pa.timestamp("ns", tz="UTC")),
        }
    )
    (statement,) = fsql.ingest_statements("t", None, rows)
    assert "CAST('9999-12-31T23:59:00+00:00' AS TIMESTAMPTZ)" in statement
    assert "CAST('2023-11-14T22:13:20.123456' AS TIMESTAMP)" in statement
    assert ", NULL," in statement and "NaT" not in statement
    # Truncated toward the epoch, as Arrow casts: the earliest nanosecond instant still reads.
    assert "CAST('1677-09-21T00:12:43.145225+00:00' AS TIMESTAMPTZ)" in statement
