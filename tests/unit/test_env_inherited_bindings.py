# Copyright (c) 2026 Kenneth Stott
# Canary: 2ed81a1c-8193-4bf2-8c12-5777893fa5cd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An environment's inherited sources point where its parent's bindings do (REQ-1491, REQ-1529,
REQ-1538, REQ-1942): through the first environment up its parent chain that binds the source as
its own, by reference; a source the chain reaches unbound is reached through no connection."""

# Requirements: REQ-1491, REQ-1529, REQ-1538, REQ-1942

from __future__ import annotations

import pytest

from provisa.core import env_bindings
from provisa.core.env_classes import BINDING_COLUMNS
from provisa.core.environments import org_schema


def _row(sid: str, binding: str, host: str = "", port: int = 0, **kw) -> dict:
    row = {c: None for c in BINDING_COLUMNS["sources"]}
    row.update(id=sid, type="postgresql", binding=binding, host=host, port=port, description=sid)
    row.update(kw)
    return row


def _lineage(monkeypatch, parents: dict[str, str | None], held: dict[str, dict[str, dict]]):
    """``parents``: each environment's parent; ``held``: each environment's sources rows."""
    asked: list[str] = []

    async def parent(admin_db, org_id, env):
        return parents[env]

    async def rows_of(conn, schema, ids):
        env = next(e for e in held if org_schema("acme", e) == schema)
        asked.append(env)
        return {sid: r for sid, r in held[env].items() if sid in ids}

    monkeypatch.setattr(env_bindings, "_parent", parent)
    monkeypatch.setattr(env_bindings, "_rows_of", rows_of)
    return asked


async def _resolve(env: str, rows: dict) -> dict:
    return await env_bindings.inherited_sources(None, None, "acme", env, rows)


async def test_an_inherited_source_reads_the_nearest_own_binding(monkeypatch):
    prod = _row("pg", "own", host="prod-db", port=5432, password_ref="${secret:PG}")
    stage = _row("pg", "own", host="stage-db", port=6543)
    _lineage(
        monkeypatch,
        {"dev": "stage", "stage": "prod", "prod": None},
        {"stage": {"pg": stage}, "prod": {"pg": prod}},
    )
    rows = {"pg": _row("pg", "inherited", description="the environment's own")}
    out = await _resolve("dev", rows)
    assert (out["pg"]["host"], out["pg"]["port"]) == ("stage-db", 6543)
    # Only where it points is inherited: the row stays the environment's, and stays inherited.
    assert out["pg"]["description"] == "the environment's own"
    assert out["pg"]["binding"] == "inherited"
    assert rows["pg"]["host"] == ""  # nothing is written into the environment's rows


async def test_a_source_the_parent_inherits_is_read_further_up(monkeypatch):
    prod = _row("pg", "own", host="prod-db", port=5432)
    asked = _lineage(
        monkeypatch,
        {"dev": "stage", "stage": "prod", "prod": None},
        {"stage": {"pg": _row("pg", "inherited")}, "prod": {"pg": prod}},
    )
    out = await _resolve("dev", {"pg": _row("pg", "inherited")})
    assert out["pg"]["host"] == "prod-db"
    assert asked == ["stage", "prod"]


async def test_a_source_the_parent_leaves_unbound_is_unbound_here(monkeypatch):
    prod = _row("pg", "own", host="prod-db", port=5432)
    asked = _lineage(
        monkeypatch,
        {"dev": "stage", "stage": "prod", "prod": None},
        {"stage": {"pg": _row("pg", "unbound")}, "prod": {"pg": prod}},
    )
    out = await _resolve("dev", {"pg": _row("pg", "inherited")})
    # The chain stops where it is unbound: prod's binding is not reached past it.
    assert (out["pg"]["binding"], out["pg"]["host"]) == ("unbound", "")
    assert asked == ["stage"]


async def test_an_own_source_keeps_its_own_binding(monkeypatch):
    asked = _lineage(monkeypatch, {"dev": "prod", "prod": None}, {"prod": {}})
    own = _row("pg", "own", host="dev-db", port=1)
    out = await _resolve("dev", {"pg": own})
    assert out["pg"] is own
    assert asked == []


async def test_an_unbound_source_inherits_nothing(monkeypatch):
    """REQ-1942: a source an environment leaves unbound -- every source of one created Unbound --
    reads through no connection, whatever its parent binds."""
    asked = _lineage(
        monkeypatch, {"qa": "prod", "prod": None}, {"prod": {"pg": _row("pg", "own", "prod-db")}}
    )
    rows = {"pg": _row("pg", "unbound")}
    out = await _resolve("qa", rows)
    assert out == rows and out["pg"]["host"] == ""
    assert asked == []


async def test_a_source_no_environment_up_the_chain_binds_is_unbound(monkeypatch):
    _lineage(monkeypatch, {"dev": "prod", "prod": None}, {"prod": {}})
    out = await _resolve("dev", {"pg": _row("pg", "inherited")})
    assert (out["pg"]["binding"], out["pg"]["host"], out["pg"]["port"]) == ("unbound", "", 0)


async def test_prod_inherits_from_nobody(monkeypatch):
    _lineage(monkeypatch, {}, {})
    rows = {"pg": _row("pg", "inherited")}
    assert await _resolve("prod", rows) is rows


async def test_a_chain_returning_to_an_environment_is_refused(monkeypatch):
    _lineage(
        monkeypatch,
        {"dev": "stage", "stage": "dev"},
        {"stage": {"pg": _row("pg", "inherited")}, "dev": {"pg": _row("pg", "inherited")}},
    )
    with pytest.raises(RuntimeError, match="dev -> stage -> dev"):
        await _resolve("dev", {"pg": _row("pg", "inherited")})
