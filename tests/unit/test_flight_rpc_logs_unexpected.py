# Copyright (c) 2026 Kenneth Stott
# Canary: aecedda9-f2e3-454d-b673-05f798b911fa
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Flight RPC that fails with anything but a Flight error is a server defect: the server logs it
with its traceback before pyarrow answers the client. Before, the traceback went only to the
client (as the ADBC Flight SQL driver's "Detail: Python exception") and server.log had nothing
(REQ-2167: the Flight SQL conformance run's 'NoneType' object is not iterable)."""

# Requirements: REQ-2167

from __future__ import annotations

import logging

import pyarrow.flight as flight
import pytest

from provisa.api.flight.server import ProvisaFlightServer


def _server() -> ProvisaFlightServer:
    server = ProvisaFlightServer.__new__(ProvisaFlightServer)
    server._as_catalog_role = lambda _context, body: body(None)  # type: ignore[method-assign]
    return server


def test_an_unexpected_failure_is_logged_with_its_traceback(caplog):
    def _fails(_descriptor, _role):
        raise TypeError("'NoneType' object is not iterable")

    server = _server()
    server._flight_info = _fails  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR, logger="provisa.api.flight.server"):
        with pytest.raises(TypeError):
            server.get_flight_info(None, flight.FlightDescriptor.for_command(b"cmd"))
    (record,) = [r for r in caplog.records if r.name == "provisa.api.flight.server"]
    assert "get_flight_info" in record.getMessage()
    assert record.exc_info is not None and record.exc_info[0] is TypeError


def test_a_flight_error_is_the_answer_and_not_logged(caplog):
    def _not_found(_descriptor, _role):
        raise flight.FlightServerError("Table not found: d.t")

    server = _server()
    server._flight_info = _not_found  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR, logger="provisa.api.flight.server"):
        with pytest.raises(flight.FlightServerError):
            server.get_flight_info(None, flight.FlightDescriptor.for_path("d", "t"))
    assert [r for r in caplog.records if r.name == "provisa.api.flight.server"] == []
