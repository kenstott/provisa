# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A read refused by name (core.read_refusal) is answered on Arrow Flight as the family's one
answer: gRPC UNAVAILABLE carrying the refusal's message. Raised as anything but a Flight error,
pyarrow sent it as an untyped "Unknown error" (REQ-1961: a numeric registered narrower than its
values was refused naming the column, and the client could not tell it was a refusal)."""

# Requirements: REQ-1961

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from provisa.api.flight import stream_slots
from provisa.api.flight.server import ProvisaFlightServer
from provisa.core import settings_registry
from provisa.federation.runtime_support import ColumnTypeMismatch


class _State:
    org_id = "default"
    roles = {"analyst": {}}
    rate_limiter = None
    flight_global_cap = 2
    security_high = False


@pytest.fixture(autouse=True)
def _fresh_slots(monkeypatch):
    monkeypatch.setattr(stream_slots, "_slots", None)
    monkeypatch.setattr(settings_registry, "_config", {})


def test_a_refused_read_is_answered_unavailable_naming_the_column():
    def _execute(_request):
        raise ColumnTypeMismatch("amount", pa.decimal128(18, 1), pa.decimal128(38, 2))

    server = ProvisaFlightServer.__new__(ProvisaFlightServer)
    server._state = _State()
    server._execute_query = _execute  # type: ignore[method-assign]
    ticket = flight.Ticket(json.dumps({"query": "SELECT 1", "role": "analyst"}).encode())
    with pytest.raises(flight.FlightUnavailableError) as refused:
        server.do_get(None, ticket)
    assert "column 'amount' is registered as decimal128(18, 1)" in str(refused.value)
