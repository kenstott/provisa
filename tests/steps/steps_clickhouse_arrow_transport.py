# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd steps for REQ-986 — ClickHouse exposes Arrow-native transport (table + stream)."""

from __future__ import annotations

import types

import pyarrow as pa
import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.federation.engine import build_clickhouse_engine
from provisa.federation.runtime import EngineCapability, EngineRuntime


@pytest.fixture
def shared_data():
    return {}


@given("the ClickHouse federation engine advertising ARROW and ARROW_STREAM")
def given_clickhouse_advertises_arrow(shared_data):
    state = types.SimpleNamespace(trino_conn=object(), flight_client=None, source_pools=None)
    caps = EngineRuntime(build_clickhouse_engine(), state).capabilities
    assert {EngineCapability.ARROW, EngineCapability.ARROW_STREAM} <= set(caps)


@when("a query executes through the Provisa Arrow Flight server via run_arrow")
def when_run_arrow(shared_data):
    chdb = pytest.importorskip("chdb")  # noqa: F841
    from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime

    rt = ClickHouseFederationRuntime.embedded()
    shared_data["rt"] = rt
    shared_data["table"] = rt.run_arrow("SELECT number AS n FROM numbers(3)")


@then("columnar data is returned as an Arrow table without row materialization")
def then_arrow_table_returned(shared_data):
    table = shared_data["table"]
    assert isinstance(table, pa.Table)
    assert table.num_rows == 3
    assert table.column_names == ["n"]


@when("the same query executes via run_arrow_stream")
def when_run_arrow_stream(shared_data):
    rt = shared_data["rt"]
    schema, batches = rt.run_arrow_stream("SELECT number AS n FROM numbers(5)")
    shared_data["schema"] = schema
    shared_data["batches"] = list(batches)
    rt.close()


@then("it yields Arrow record batches lazily")
def then_record_batches_yielded(shared_data):
    assert isinstance(shared_data["schema"], pa.Schema)
    assert all(isinstance(b, pa.RecordBatch) for b in shared_data["batches"])
    assert sum(b.num_rows for b in shared_data["batches"]) == 5


scenarios("../features/REQ-986.feature")
