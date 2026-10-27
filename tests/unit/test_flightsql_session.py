# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Flight SQL's session options (REQ-2175): the current catalog and schema a client reads, through
the ADBC Flight SQL driver against a server on a loopback port, as pgwire answers them --
current_database() and current_schema(). Before, reading either failed "EOF ...
GetSessionOptions"."""

# Requirements: REQ-2175

from __future__ import annotations

import types

import adbc_driver_flightsql.dbapi as flightsql
import pytest

from provisa.api.flight.server import ProvisaFlightServer
from provisa.pgwire.catalog import _handle_scalar


class _State:
    auth_config = None
    auth_middleware_active = False
    multitenancy = False
    org_id = "default"
    settings_overrides: dict = {"derived_roles": []}


@pytest.fixture
def served(monkeypatch):
    """A ProvisaFlightServer on a loopback port, each call recording the role it ran as."""
    roles: list[str] = []

    def _as_caller(self, request, body):  # noqa: ARG001  # the server's signature
        roles.append(str(request.get("role")))
        return body()

    monkeypatch.setattr(ProvisaFlightServer, "_as_ticket_caller", _as_caller)
    server = ProvisaFlightServer(_State(), location="grpc://127.0.0.1:0")
    try:
        yield types.SimpleNamespace(uri=f"grpc://127.0.0.1:{server.port}", roles=roles)
    finally:
        server.shutdown()


def _pgwire(function: str) -> str:
    answer = _handle_scalar(f"SELECT {function}()", "seller", _State())
    assert answer is not None
    (row,) = answer.rows
    return row[0]


def test_the_current_catalog_and_schema_are_pgwires(served):
    with flightsql.connect(
        served.uri, db_kwargs={"adbc.flight.sql.rpc.call_header.x-provisa-role": "seller"}
    ) as conn:
        assert conn.adbc_current_catalog == _pgwire("current_database") == "provisa"
        assert conn.adbc_current_db_schema == _pgwire("current_schema") == "public"
    assert set(served.roles) == {"seller"}
