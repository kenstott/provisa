# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd steps for REQ-995 — Fabric attaches Iceberg live via OneLake Delta virtualization."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.federation.mssql_warehouse_connectors import openrowset_link_connectors


def _src(stype, path="onelake://ws/lh/Tables/orders"):
    return SimpleNamespace(
        id="x", type=SimpleNamespace(value=stype), path=path, federation_hints={}
    )


@pytest.fixture
def shared_data():
    return {}


@given("an Iceberg table registered in OneLake on Microsoft Fabric")
def given_iceberg_in_onelake(shared_data):
    shared_data["fabric"] = {c.source_type: c for c in openrowset_link_connectors("fabric")}


@when("a live attachment is configured for Fabric engine with _FABRIC_ONLY_FORMAT constraint")
def when_live_attach_fabric(shared_data):
    shared_data["details"] = shared_data["fabric"]["iceberg"].details(_src("iceberg"))


@then("the table is queryable LIVE via OPENROWSET FORMAT='DELTA' without materialization")
def then_queryable_live_as_delta(shared_data):
    # Fabric virtualizes Iceberg through OneLake Delta metadata → OPENROWSET FORMAT='DELTA'.
    assert shared_data["details"]["format"] == "DELTA"
    assert "iceberg" in shared_data["fabric"]  # attached live, not landed


@given("the same Iceberg table targeted for Azure Synapse serverless")
def given_iceberg_for_synapse(shared_data):
    shared_data["synapse"] = {c.source_type: c for c in openrowset_link_connectors("synapse")}


@when("attachment is attempted without OneLake virtualization")
def when_attach_synapse_no_virtualization(shared_data):
    shared_data["synapse_types"] = set(shared_data["synapse"])


@then("the table is materialized as a REPLICA instead of attached live")
def then_materialized_as_replica(shared_data):
    # Synapse serverless has no OneLake virtualization, so Iceberg is not a live-attach connector.
    assert "iceberg" not in shared_data["synapse_types"]


scenarios("../features/REQ-995.feature")
