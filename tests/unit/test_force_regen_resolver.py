# Copyright (c) 2026 Kenneth Stott
# Canary: adfc2b51-e873-42b7-8a6c-9ec905b6b36a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""force_regen admin mutation (REQ-968): scope derivation, refusal, and the posted event."""

import pytest

import provisa.api.admin.schema_mutation as sm
import provisa.api.admin._refresh_summary as refresh_summary
import provisa.events.injector as injector

from provisa.api.admin.schema_mutation import Mutation
from provisa.core.models import Source
from provisa.federation import strategy as strategy_mod
from provisa.federation.strategy import Strategy


class _Result:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _Conn:
    def __init__(self, row):
        self._row = row

    async def execute_core(self, _stmt):
        return _Result(self._row)


class _Pool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        conn = self._conn

        class _Ctx:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *_exc):
                return False

        return _Ctx()


class _Registry:
    def __init__(self, views):
        self._views = views

    def get(self, key):
        return self._views.get(key)


class _Config:
    def __init__(self, sources):
        self.sources = sources


class _State:
    def __init__(self, views, sources):
        self.mv_registry = _Registry(views)
        self.config = _Config(sources)


@pytest.fixture
def posted(monkeypatch):
    """Capture what the resolver posts instead of touching a real queue."""
    calls = []

    async def _force_regen(_conn, *, scope, node, reason, **kw):
        calls.append({"scope": scope, "node": node, "reason": reason, **kw})
        return 4242

    monkeypatch.setattr(injector, "force_regen", _force_regen)
    return calls


@pytest.fixture
def wire(monkeypatch):
    """Point the resolver at a fake table row, config and engine."""

    def _wire(*, row, views=None, strategy=Strategy.MATERIALIZED, engine=object()):
        import provisa.api.app as app_mod

        source = Source(id="wh", type="postgresql", host="h", database="d")
        monkeypatch.setattr(sm, "_get_pool", lambda: _awaited(_Pool(_Conn(row))))
        monkeypatch.setattr(app_mod, "state", _State(views or {}, [source]), raising=False)
        monkeypatch.setattr(refresh_summary, "_resolve_engine", lambda: engine)
        monkeypatch.setattr(strategy_mod, "federate", lambda _s, _e: strategy)

    return _wire


def _awaited(value):
    async def _coro():
        return value

    return _coro()


async def test_a_landed_source_table_regens_at_source_scope(wire, posted):
    """Re-landing the source is what an operator means by "run this table now" — the cascade to its
    dependents is the event loop's, not a second thing to ask for."""
    wire(row=("sales", "orders", "wh"))
    res = await Mutation().force_regen(table_id=7, reason="bad overnight load")
    assert res.success is True
    assert posted == [{"scope": "source", "node": "sales.orders", "reason": "bad overnight load"}]
    assert res.params["event"] == 4242


async def test_a_derived_view_regens_at_node_scope(wire, posted):
    """A view's rows come from its own SQL, so recomputing it must not re-land every input it reads."""
    wire(row=("marts", "revenue", "wh"), views={"view-revenue": object()})
    res = await Mutation().force_regen(table_id=9, reason="changed the SQL")
    assert res.success is True
    assert posted[0]["scope"] == "node"
    assert posted[0]["node"] == "marts.revenue"


async def test_a_live_federated_table_is_refused(wire, posted):
    """Nothing lands it, so a forced event would sit in the queue with no processor to claim it."""
    wire(row=("sales", "orders", "wh"), strategy=Strategy.VIRTUAL)
    res = await Mutation().force_regen(table_id=7, reason="just because")
    assert res.success is False
    assert res.code == "schema.table_not_landed"
    assert posted == []


async def test_a_regen_with_no_reason_is_refused(wire, posted, monkeypatch):
    """REQ-968: the why-tag is the audit record; the injector refuses without one and the resolver
    reports that refusal rather than swallowing it."""
    monkeypatch.undo()  # keep the real injector so its own guard is what answers
    wire(row=("sales", "orders", "wh"))
    res = await Mutation().force_regen(table_id=7, reason="   ")
    assert res.success is False
    assert res.code == "schema.regen_refused"


async def test_an_unknown_table_is_reported_not_posted(wire, posted):
    wire(row=None)
    res = await Mutation().force_regen(table_id=404, reason="x")
    assert res.success is False
    assert res.code == "schema.table_not_found"
    assert posted == []
