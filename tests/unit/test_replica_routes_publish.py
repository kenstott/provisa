# Copyright (c) 2026 Kenneth Stott
# Canary: 972a9ada-c39a-4086-a127-80124d453301
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Publishing the replica read map on a native engine (REQ-1912, REQ-1695, REQ-1882).

The map is published by a schema rebuild, outside any statement. It must not fail on a source
whose credential is an org vault reference (``${secret:...}``), and it must not hold the worker's
event loop while anything slow happens: every request on that worker would wait behind it."""

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace


from provisa.core.models import Source, SourceType
from provisa.federation import replica_routing
from provisa.federation.engine import build_engine


class _Runtime:
    """A native runtime whose source attach and store attach report what they were given and
    can be made to wait, as a slow or unreachable host makes a real one wait."""

    def __init__(self, release: threading.Event | None = None) -> None:
        self.release = release
        self.dialed: list[str | None] = []
        self.blocked: list[str] = []

    def _wait(self, what: str) -> None:
        if self.release is not None and not self.release.wait(2):
            self.blocked.append(what)

    def attach_source(self, source) -> None:
        self.dialed.append(source.password)
        self._wait("source")

    def ensure_materialize_attached(self) -> str:
        self._wait("store")
        return "mat_store"

    def mv_store_schema(self, org_id: str) -> str:
        return f"org_{org_id}_mv_cache"


def _source() -> Source:
    return Source(
        id="pg",
        type=SourceType.postgresql,
        host="h",
        port=5432,
        database="d",
        username="u",
        password="${secret:PG_PASSWORD}",
        cache_ttl=60,
    )


def _state(monkeypatch, runtime: _Runtime):
    engine = build_engine("duckdb")
    engine.backend._runtime = runtime
    source = _source()
    reg = {
        "id": 1,
        "source_id": "pg",
        "schema_name": "public",
        "table_name": "orders",
        "replicate": 0,
        "load_protected": None,
        "columns": [{"column_name": "id", "native_filter_type": None}],
    }
    key = ("pg", "public", "orders")
    registry = replica_routing._Registry(
        [reg], {"pg": source}, serving=frozenset({key}), promoted=frozenset()
    )

    async def _registry(_state):
        return registry

    async def _sources(_state, _conn=None):
        return [source]

    async def _decrypted(_db, _org, _owner):
        return {"PG_PASSWORD": "pw"}

    monkeypatch.setattr(replica_routing, "_registry", _registry)
    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    monkeypatch.setattr("provisa.core.secrets_store._decrypted", _decrypted)
    table = SimpleNamespace(source_id="pg", schema_name="public", table_name="orders")
    return SimpleNamespace(
        org_id="default",
        active_org_id="default",
        admin_db=None,
        config=SimpleNamespace(sources=[source], tables=[table]),
        runtime_sources={},
        tables=[],
        model_db=None,
        tenant_db=None,
        source_catalogs={"pg": "pg"},
        federation_engine=SimpleNamespace(engine=engine),
    )


async def test_routes_publish_with_no_org_bound_for_a_source_whose_credential_is_in_the_vault(
    monkeypatch,
):
    """A rebuild publishes the map outside any statement. A source whose password is
    ``${secret:...}`` must not fail it with "no organization is bound" (30c1246b)."""
    from provisa.core.secrets_store import bound_org_id

    assert bound_org_id() is None
    runtime = _Runtime()
    routes = await replica_routing.replica_routes(_state(monkeypatch, runtime))
    (route,) = set(routes.routes.values())
    assert route.target[0] == "mat_store"
    assert runtime.dialed == []  # where a replica is read needs no source dialed


async def test_a_routes_publish_never_holds_the_event_loop_while_it_waits_on_a_host(monkeypatch):
    """REQ-1882: a publish waiting on a slow source or store must not stall a concurrent request
    on the same worker. The request here is what releases the wait: on a held loop it never
    runs, and the wait runs out."""
    release = threading.Event()
    runtime = _Runtime(release)
    state = _state(monkeypatch, runtime)

    async def request() -> None:
        await asyncio.sleep(0)
        release.set()

    publish = asyncio.create_task(replica_routing.replica_routes(state))
    await asyncio.gather(publish, request())
    assert runtime.blocked == []
    assert runtime.dialed == []  # where a replica is read needs no source dialed
