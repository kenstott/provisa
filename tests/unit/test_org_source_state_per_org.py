# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What an org's build records about its sources and tables is its own (REQ-1266).

Each collection here is keyed by a source id, a table name or a table id, and two orgs may use
the same one: a demo's sandboxes are copies of its template, ids and all. What one org's build
records under a key is what that org reads under it, whichever org was built last, and an org
that recorded nothing under a key finds nothing there."""

# Requirements: REQ-1266

from __future__ import annotations

import pytest

from provisa.api.org_runtime import OrgRuntime
from provisa.core.request_context import reset_current_org, set_current_org

pytestmark = pytest.mark.unbound

# collection -> a key two orgs may both use
_KEYED = {
    "table_approval_hooks": 7,
    "source_approval_hooks": "sales-pg",
    "graphql_remote_sources": "graphql-demo",
    "openapi_specs": "petstore-api",
    "grpc_remote_sources": "grpc-demo",
    "websocket_sources": "ticker",
    "rss_sources": "news",
    "cdc_sources": "sales-pg",
    "ingest_engines": "events",
    "ingest_tables": "events",
    "kafka_windows": "clicks",
    "kafka_bootstrap": "clicks",
    "kafka_table_configs": "clickstream",
    "kafka_table_physical": "clickstream",
    "pg_enum_types": "order_status",
    "table_watermarks": "orders",
}
_SETS = {"pg_notify_tables": "orders"}


@pytest.fixture
def state(monkeypatch):
    from provisa.api.app import AppState

    state = AppState()
    state.org_id = "root"
    for org in ("acme", "globex"):
        state.org_registry.set(org, OrgRuntime(org_id=org))
    monkeypatch.setattr("provisa.api.app.state", state)
    return state


def _in(org: str, fn):
    token = set_current_org(org)
    try:
        return fn()
    finally:
        reset_current_org(token)


@pytest.mark.parametrize(("name", "key"), sorted(_KEYED.items()))
def test_each_org_reads_what_it_recorded(state, name, key):
    _in("acme", lambda: getattr(state, name).__setitem__(key, f"acme's {name}"))
    _in("globex", lambda: getattr(state, name).__setitem__(key, f"globex's {name}"))
    assert _in("acme", lambda: getattr(state, name).get(key)) == f"acme's {name}"
    assert _in("globex", lambda: getattr(state, name).get(key)) == f"globex's {name}"


@pytest.mark.parametrize(("name", "key"), sorted(_KEYED.items()))
def test_an_org_finds_nothing_another_recorded(state, name, key):
    _in("globex", lambda: getattr(state, name).__setitem__(key, f"globex's {name}"))
    assert _in("acme", lambda: key in getattr(state, name)) is False


@pytest.mark.parametrize(("name", "member"), sorted(_SETS.items()))
def test_a_member_one_org_recorded_is_not_another_orgs(state, name, member):
    _in("globex", lambda: getattr(state, name).add(member))
    assert _in("acme", lambda: member in getattr(state, name)) is False
    assert _in("globex", lambda: member in getattr(state, name)) is True
