# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1163: an Arrow Flight ticket reads bitemporal materialized views as of a time.

Its ``as_of`` option is the ``X-Provisa-As-Of`` header of the HTTP transports: one ISO 8601 time,
validated to a safe SQL timestamp literal, and handed to the one pipeline, which reads every
bitemporal view the statement names as of it."""

# Requirements: REQ-1163

from __future__ import annotations

import asyncio
import inspect
from types import SimpleNamespace

import pytest

pa = pytest.importorskip("pyarrow")
flight = pytest.importorskip("pyarrow.flight")

from provisa.api.flight import server as flight_server  # noqa: E402
from provisa.api.flight.server import ProvisaFlightServer, _ticket_as_of  # noqa: E402
from provisa.pgwire import _pipeline  # noqa: E402

_LITERAL = "TIMESTAMP '2026-10-16 02:00:00.000000'"


def test_a_ticket_names_one_time_as_an_iso_8601_string():
    assert _ticket_as_of({"query": "SELECT 1", "as_of": "2026-10-16T02:00:00"}) == _LITERAL
    assert _ticket_as_of({"query": "SELECT 1"}) is None


@pytest.mark.parametrize("as_of", ["yesterday", 1792200000, ["2026-10-16"]])
def test_an_as_of_that_cannot_be_read_is_refused_by_name(as_of):
    with pytest.raises(flight.FlightServerError, match="as_of"):
        _ticket_as_of({"query": "SELECT 1", "as_of": as_of})


class _Stop(PermissionError):
    """Raised by the stand-in pipeline once it has seen what it was asked."""


def _server() -> ProvisaFlightServer:
    srv = ProvisaFlightServer.__new__(ProvisaFlightServer)
    srv._state = SimpleNamespace(roles={})  # pyright: ignore[reportAttributeAccessIssue]
    srv._run_on_loop = lambda coro, *, timeout=None: asyncio.run(coro)  # type: ignore[method-assign]
    return srv


def _asked(monkeypatch, name: str) -> dict:
    seen: dict = {}

    async def _pipeline_stand_in(*args, **kwargs):
        seen.update(kwargs)
        raise _Stop("seen")

    monkeypatch.setattr(_pipeline, name, _pipeline_stand_in)
    return seen


def test_a_sql_ticket_hands_its_as_of_to_the_pipeline(monkeypatch):
    seen = _asked(monkeypatch, "govern_batch_final_plan_with_fn")
    with pytest.raises(flight.FlightServerError):
        _server()._do_get_sql_governed(
            {
                "query": "SELECT * FROM sales.view_daily",
                "role": "finance",
                "as_of": "2026-10-16T02:00",
            }
        )
    assert seen["as_of"] == _LITERAL


def test_a_graphql_ticket_hands_its_as_of_to_the_pipeline(monkeypatch):
    seen = _asked(monkeypatch, "_govern_and_route_compiled")
    srv = _server()
    compiled = SimpleNamespace(sql="SELECT 1", params=[])
    monkeypatch.setattr(
        srv, "_compile_query", lambda _ticket: (None, None, None, None, compiled, None, None)
    )
    with pytest.raises(flight.FlightServerError):
        srv._do_get_graphql(
            {"query": "{ daily { id } }", "role": "finance", "as_of": "2026-10-16T02:00"}
        )
    assert seen["as_of"] == _LITERAL


def test_a_cypher_ticket_hands_its_as_of_to_the_pipeline():
    """The Cypher path's translation needs a whole graph context to reach the pipeline; the call
    it makes is read instead."""
    source = inspect.getsource(flight_server.ProvisaFlightServer._do_get_cypher)
    assert "as_of=_ticket_as_of(request)" in source
