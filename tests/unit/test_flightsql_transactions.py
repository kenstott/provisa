# Copyright (c) 2026 Kenneth Stott
# Canary: 4ac8689e-b45a-4b31-b5ac-fe34519254ce
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Flight SQL's transaction actions, acknowledged and holding no transaction (REQ-883, amended
2026-10-27: transaction control is a no-op on every surface): through the ADBC Flight SQL driver
against a server on a loopback port, a client turns autocommit off, runs a statement, rolls back
and commits, as it would against a transactional server. Before, the server claimed no
transaction support and the driver refused to turn autocommit off."""

# Requirements: REQ-883

from __future__ import annotations

import types

import adbc_driver_flightsql.dbapi as flightsql
import pyarrow as pa
import pyarrow.flight as flight
import pytest

from provisa.api.flight import flightsql as fsql
from provisa.api.flight.server import ProvisaFlightServer


class _State:
    auth_config = None
    auth_middleware_active = False
    multitenancy = False
    org_id = "default"
    settings_overrides: dict = {"derived_roles": []}


@pytest.fixture
def served(monkeypatch):
    """A ProvisaFlightServer on a loopback port whose pipeline answers each statement it is asked
    (recorded) with one row."""
    asked: list[str] = []

    def _as_caller(self, request, body):  # noqa: ARG001  # the server's signature
        request.setdefault("role", "anonymous")
        return body()

    def _pipeline(self, request, ticket):  # noqa: ARG001  # the native ticket's signature
        asked.append(request["query"])
        return flight.RecordBatchStream(pa.table({"n": [1]}))

    async def _describe(sql, role_id, *, as_of=None):  # noqa: ARG001  # pgwire's signature
        return types.SimpleNamespace(shape=[("n", "integer")], governed=None)

    monkeypatch.setattr(ProvisaFlightServer, "_as_ticket_caller", _as_caller)
    monkeypatch.setattr(ProvisaFlightServer, "_do_get_on_loop", _pipeline)
    monkeypatch.setattr("provisa.pgwire._pipeline.describe_pgwire_statement", _describe)
    server = ProvisaFlightServer(_State(), location="grpc://127.0.0.1:0")
    try:
        yield types.SimpleNamespace(uri=f"grpc://127.0.0.1:{server.port}", asked=asked)
    finally:
        server.shutdown()


def test_a_client_toggles_autocommit_and_ends_transactions_that_hold_nothing(served):
    with flightsql.connect(
        served.uri,
        db_kwargs={"adbc.flight.sql.rpc.call_header.x-provisa-role": "seller"},
        autocommit=True,
    ) as conn:
        assert conn.adbc_connection.get_option("adbc.connection.autocommit") == "true"
        conn.adbc_connection.set_options(**{"adbc.connection.autocommit": False})
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchall() == [(1,)]
        conn.adbc_connection.rollback()
        conn.adbc_connection.commit()
        conn.adbc_connection.set_options(**{"adbc.connection.autocommit": True})
    assert served.asked == ["SELECT 1"]


def test_sql_info_says_transactions_are_taken():
    (row,) = fsql.sql_info_table(
        fsql.pb.CommandGetSqlInfo(info=[fsql.pb.FLIGHT_SQL_SERVER_TRANSACTION])
    ).to_pylist()
    assert row == {
        "info_name": fsql.pb.FLIGHT_SQL_SERVER_TRANSACTION,
        "value": fsql.pb.SQL_SUPPORTED_TRANSACTION_TRANSACTION,
    }


def test_ending_a_transaction_the_server_never_began_is_refused(served):
    client = flight.connect(served.uri)
    try:
        request = fsql.pb.ActionEndTransactionRequest(
            transaction_id=b"elsewhere",
            action=fsql.pb.ActionEndTransactionRequest.END_TRANSACTION_COMMIT,
        )
        with pytest.raises(flight.FlightServerError, match="no transaction 'elsewhere'"):
            list(client.do_action(flight.Action("EndTransaction", fsql.pack(request))))
    finally:
        client.close()
