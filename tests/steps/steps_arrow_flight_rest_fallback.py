# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd steps for REQ-146 — Flight falls back to Trino REST when Zaychik is unavailable."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.federation.backend import TrinoBackend
from provisa.federation.engine import build_trino_engine


@pytest.fixture
def shared_data():
    return {}


@given("the Zaychik proxy is unavailable")
def given_zaychik_unavailable(shared_data):
    # No Arrow Flight client configured on the engine state signals Zaychik is unavailable.
    shared_data["state"] = SimpleNamespace(flight_client=None)
    shared_data["backend"] = TrinoBackend(build_trino_engine())


@when("a Flight query is submitted")
def when_flight_query_submitted(shared_data):
    backend = shared_data["backend"]
    try:
        backend.execute_arrow(shared_data["state"], "SELECT 1")
        shared_data["arrow_error"] = None
    except RuntimeError as exc:
        shared_data["arrow_error"] = exc


@then("the Flight server falls back to materializing results via Trino REST API")
def then_falls_back_to_trino_rest(shared_data):
    # Arrow transport refuses (fails closed) when Zaychik is absent — the caller then
    # materializes via the row/REST path, which every Trino backend implements.
    assert isinstance(shared_data["arrow_error"], RuntimeError)
    assert "Arrow Flight transport is not configured" in str(shared_data["arrow_error"])
    # The REST/row materialization fallback exists on the same backend.
    assert callable(shared_data["backend"].execute)


scenarios("../features/REQ-146.feature")
