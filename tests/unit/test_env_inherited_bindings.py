# Copyright (c) 2026 Kenneth Stott
# Canary: 2ed81a1c-8193-4bf2-8c12-5777893fa5cd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A branch's unbound sources point where its base's bindings do (REQ-1491, REQ-1529, REQ-1538):
the nearest bound environment up its branched_from chain, by reference."""

# Requirements: REQ-1491, REQ-1529, REQ-1538

from __future__ import annotations

import pytest

from provisa.core import env_bindings
from provisa.core.env_classes import BINDING_COLUMNS
from provisa.core.environments import org_schema


def _row(sid: str, *, bound: bool, host: str = "", port: int = 0, **kw) -> dict:
    row = {c: None for c in BINDING_COLUMNS["sources"]}
    row.update(id=sid, type="postgresql", bound=bound, host=host, port=port, description=sid)
    row.update(kw)
    return row


def _lineage(monkeypatch, parents: dict[str, str | None], bound: dict[str, dict[str, dict]]):
    """parents: each environment's branched_from; bound: each environment's bound rows."""
    asked: list[str] = []

    async def parent(admin_db, org_id, env):
        return parents[env]

    async def bound_rows(conn, schema, ids):
        env = next(e for e in bound if org_schema("acme", e) == schema)
        asked.append(env)
        return {sid: r for sid, r in bound[env].items() if sid in ids}

    monkeypatch.setattr(env_bindings, "_parent", parent)
    monkeypatch.setattr(env_bindings, "_bound_rows", bound_rows)
    return asked


async def test_a_branch_reads_the_nearest_ancestors_binding(monkeypatch):
    prod = _row("pg", bound=True, host="prod-db", port=5432, password_ref="${secret:PG}")
    stage = _row("pg", bound=True, host="stage-db", port=6543)
    _lineage(
        monkeypatch,
        {"dev": "stage", "stage": "prod", "prod": None},
        {"stage": {"pg": stage}, "prod": {"pg": prod}},
    )
    rows = {"pg": _row("pg", bound=False, description="the branch's own")}
    out = await env_bindings.inherited_sources(None, None, "acme", "dev", rows)
    assert (out["pg"]["host"], out["pg"]["port"]) == ("stage-db", 6543)
    # Only where it points is inherited: the row stays the branch's, and stays unbound.
    assert out["pg"]["description"] == "the branch's own"
    assert out["pg"]["bound"] is False
    assert rows["pg"]["host"] == ""  # nothing is written into the branch's rows


async def test_a_source_unbound_in_the_parent_is_read_further_up(monkeypatch):
    prod = _row("pg", bound=True, host="prod-db", port=5432)
    asked = _lineage(
        monkeypatch,
        {"dev": "stage", "stage": "prod", "prod": None},
        {"stage": {}, "prod": {"pg": prod}},
    )
    out = await env_bindings.inherited_sources(
        None, None, "acme", "dev", {"pg": _row("pg", bound=False)}
    )
    assert out["pg"]["host"] == "prod-db"
    assert asked == ["stage", "prod"]


async def test_a_bound_source_keeps_its_own_binding(monkeypatch):
    asked = _lineage(monkeypatch, {"dev": "prod", "prod": None}, {"prod": {}})
    own = _row("pg", bound=True, host="dev-db", port=1)
    out = await env_bindings.inherited_sources(None, None, "acme", "dev", {"pg": own})
    assert out["pg"] is own
    assert asked == []


async def test_an_environment_created_without_inherit_connections_inherits_nothing(monkeypatch):
    """REQ-1538: inherit_connections is opt-in; created without it, an environment records no
    branched_from (provisa.api.admin.environments_router), so it is a base and its sources stay
    unbound."""
    asked = _lineage(monkeypatch, {"qa": None}, {})
    rows = {"pg": _row("pg", bound=False)}
    out = await env_bindings.inherited_sources(None, None, "acme", "qa", rows)
    assert out == rows and out["pg"]["host"] == ""
    assert asked == []


async def test_a_source_no_ancestor_bound_stays_unbound(monkeypatch):
    _lineage(monkeypatch, {"dev": "prod", "prod": None}, {"prod": {}})
    out = await env_bindings.inherited_sources(
        None, None, "acme", "dev", {"pg": _row("pg", bound=False)}
    )
    assert out["pg"]["host"] == "" and out["pg"]["port"] == 0


async def test_prod_inherits_from_nobody(monkeypatch):
    _lineage(monkeypatch, {}, {})
    rows = {"pg": _row("pg", bound=False)}
    assert await env_bindings.inherited_sources(None, None, "acme", "prod", rows) is rows


async def test_a_chain_returning_to_an_environment_is_refused(monkeypatch):
    _lineage(monkeypatch, {"dev": "stage", "stage": "dev"}, {"stage": {}, "dev": {}})
    with pytest.raises(RuntimeError, match="dev -> stage -> dev"):
        await env_bindings.inherited_sources(
            None, None, "acme", "dev", {"pg": _row("pg", bound=False)}
        )
