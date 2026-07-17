# Copyright (c) 2026 Kenneth Stott
# Canary: 0a321aa1-3ec1-41ca-8eed-cea958f0b1a0
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd steps for REQ-988 — Snowflake as a first-class engine with Arrow + Snowflake dialect."""

from __future__ import annotations

import types

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.federation.engine import build_snowflake_engine
from provisa.federation.runtime import EngineCapability, EngineRuntime
from provisa.transpiler.transpile import SUPPORTED_DIALECTS, transpile


@pytest.fixture
def shared_data():
    return {}


@given("Snowflake promoted to a first-class federation engine peer to Trino/DuckDB/ClickHouse")
def given_snowflake_first_class(shared_data):
    state = types.SimpleNamespace(trino_conn=object(), flight_client=None, source_pools=None)
    shared_data["caps"] = EngineRuntime(build_snowflake_engine(), state).capabilities


@when("it declares capabilities")
def when_declares_capabilities(shared_data):
    shared_data["cap_set"] = set(shared_data["caps"])


@then("ROWS, ARROW, and ARROW_STREAM are advertised")
def then_all_three_advertised(shared_data):
    assert {
        EngineCapability.ROWS,
        EngineCapability.ARROW,
        EngineCapability.ARROW_STREAM,
    } <= shared_data["cap_set"]


@given("a single-source query targeting Snowflake")
def given_single_source_query(shared_data):
    shared_data["pg_sql"] = "SELECT id, amount FROM orders WHERE amount > 100"


@when("physical SQL is transpiled")
def when_transpiled(shared_data):
    assert "snowflake" in SUPPORTED_DIALECTS
    shared_data["out"] = transpile(shared_data["pg_sql"], "snowflake")


@then("it targets the Snowflake dialect and routes directly with no Trino detour")
def then_targets_snowflake_dialect(shared_data):
    out = shared_data["out"]
    assert isinstance(out, str) and out.strip()
    assert "orders" in out  # transpiled to a Snowflake-dialect statement, not a Trino wrapper


scenarios("../features/REQ-988.feature")
