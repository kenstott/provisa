# Copyright (c) 2026 Kenneth Stott
# Canary: 51733c9d-2876-4015-b627-96c660302cb6
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd steps for REQ-948 — connector driver availability is provider-tagged per connector."""

from __future__ import annotations

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.federation.connector_base import DriverProvider
from provisa.federation.connector_duckdb import MysqlFdwConnector, OracleFdwConnector


@pytest.fixture
def shared_data():
    return {}


@given('a connector runtime_deps entry tagged provider="bundled"')
def given_bundled_dep(shared_data):
    shared_data["conn"] = MysqlFdwConnector()


@when("the source list is rendered")
def when_source_list_rendered(shared_data):
    conn = shared_data["conn"]
    shared_data["bundled"] = all(d.provider is DriverProvider.BUNDLED for d in conn.runtime_deps)
    shared_data["operator_deps"] = conn.operator_deps


@then("the source is enabled because Provisa ships and relocates its driver")
def then_source_enabled(shared_data):
    assert shared_data["bundled"] is True
    # No operator-provided deps ⇒ never shown disabled.
    assert shared_data["operator_deps"] == ()


@given('a runtime_deps entry tagged provider="operator" whose driver is not installed')
def given_operator_dep(shared_data):
    shared_data["conn"] = OracleFdwConnector()


@when("probe() reports it unavailable")
def when_probe_unavailable(shared_data):
    shared_data["operator_deps"] = shared_data["conn"].operator_deps


@then("the source appears in the dropdown but disabled with its operator remediation")
def then_source_disabled_with_remediation(shared_data):
    deps = shared_data["operator_deps"]
    # A non-empty operator_deps set is what renders the source offered-but-disabled with remediation.
    assert len(deps) >= 1
    assert all(d.provider is DriverProvider.OPERATOR for d in deps)
    assert all(d.lib for d in deps)  # remediation text names the missing library


scenarios("../features/REQ-948.feature")
