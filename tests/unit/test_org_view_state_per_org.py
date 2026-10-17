# Copyright (c) 2026 Kenneth Stott
# Canary: 2eb132cb-81cb-4ba5-bed3-e00efc384693
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each org's views and table settings are its own (REQ-1266).

Two orgs may each have a view of the same name, and each numbers its own tables. What one org's
build records -- a view's SQL for inline expansion, a bitemporal view's read, a table's response
cache TTL -- is what that org's reads use, whichever org was built last."""

# Requirements: REQ-1266, REQ-135, REQ-1163

from __future__ import annotations

import pytest

from provisa.api.org_runtime import OrgRuntime
from provisa.compiler.view_expand import expand_view_refs
from provisa.core.request_context import reset_current_org, set_current_org

pytestmark = pytest.mark.unbound


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


def _build_views(state, org: str) -> None:
    """What an org's build records for its view ``summary`` (app_rebuild)."""

    def build():
        state.view_sql_map["summary"] = f'SELECT * FROM "{org}_db"."public"."orders"'
        state.bitemporal_view_reads["summary"] = (f'"mat"."org_{org}_mv_cache"."mv_summary"', None)

    _in(org, build)


def test_a_view_is_expanded_from_its_own_orgs_definition(state):
    _build_views(state, "acme")
    _build_views(state, "globex")
    sql = 'SELECT * FROM "views"."sales"."summary"'
    acme = _in("acme", lambda: expand_view_refs(sql, state.view_sql_map))
    globex = _in("globex", lambda: expand_view_refs(sql, state.view_sql_map))
    assert '"acme_db"' in acme and '"globex_db"' not in acme
    assert '"globex_db"' in globex and '"acme_db"' not in globex


def test_a_view_another_org_built_is_not_expanded(state):
    _build_views(state, "globex")
    assert _in("acme", lambda: dict(state.view_sql_map)) == {}


def test_a_bitemporal_view_read_is_its_own_orgs(state):
    _build_views(state, "acme")
    _build_views(state, "globex")
    acme = _in("acme", lambda: state.bitemporal_view_reads["summary"][0])
    assert acme == '"mat"."org_acme_mv_cache"."mv_summary"'


def test_a_tables_cache_ttl_is_its_own_orgs(state):
    """Table ids are numbered per org: table 7 of one org is not table 7 of another."""
    _in("acme", lambda: setattr(state, "table_cache", {7: 60}))
    _in("globex", lambda: setattr(state, "table_cache", {7: None}))
    assert _in("acme", lambda: state.table_cache.get(7)) == 60
    assert _in("globex", lambda: state.table_cache.get(7)) is None
