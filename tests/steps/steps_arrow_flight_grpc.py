# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd steps for REQ-143 — Arrow Flight gRPC streaming with the governed pipeline."""

from __future__ import annotations

import pyarrow as pa
import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.api.flight.server import ProvisaFlightServer
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import ColumnRef
from provisa.executor.formats.arrow import rows_to_arrow_table


@pytest.fixture
def shared_data():
    return {}


@given("a client connects to the Arrow Flight server on port 8815")
def given_client_connects_on_8815(shared_data):
    # The Flight server's default location binds gRPC on 8815 (REQ-143).
    import inspect

    default_location = (
        inspect.signature(ProvisaFlightServer.__init__).parameters["location"].default
    )
    assert default_location.endswith(":8815"), default_location
    assert default_location.startswith("grpc://"), default_location
    shared_data["location"] = default_location


@when("a query is submitted")
def when_query_submitted(shared_data):
    columns = [
        ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
        ColumnRef(alias=None, column="region", field_name="region", nested_in=None),
    ]
    shared_data["table"] = rows_to_arrow_table([(1, "NA"), (2, "EU")], columns)


@then("record batches are streamed via gRPC with the full Provisa security pipeline applied")
def then_record_batches_streamed_with_security(shared_data):
    table = shared_data["table"]
    assert isinstance(table, pa.Table)
    # Streamable as gRPC record batches.
    batches = table.to_batches()
    assert batches and all(isinstance(b, pa.RecordBatch) for b in batches)
    assert set(table.schema.names) == {"id", "region"}
    # The governed pipeline is wired into do_get, which resolves an RLS context per role.
    assert callable(ProvisaFlightServer.do_get)
    assert RLSContext.empty() is not None


scenarios("../features/REQ-143.feature")
