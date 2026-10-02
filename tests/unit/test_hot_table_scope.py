# Copyright (c) 2026 Kenneth Stott
# Canary: 418690f2-6cfb-487c-83ce-0b2d33ce0847
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A hot table's rows belong to one org, one environment and one model (REQ-230, REQ-595, REQ-1914).

The hot tier keeps the rows of small lookup tables in the process and in Redis and substitutes
them into queries. One manager serves the whole process and one Redis serves every process, and
both were keyed by the bare table name: two orgs — or prod and a branch, or two sources — with a
table of the same name read each other's rows, and a table pointed at another relation kept
serving the old one's.

The registry is now kept per org and environment, holds only rows loaded under the model the
runtime currently has, and the Redis key carries that scope and the table's catalog, schema and
name.
"""

# Requirements: REQ-230, REQ-232, REQ-595, REQ-1914, REQ-1529

from __future__ import annotations

import pytest

from provisa.cache import hot_tables
from provisa.cache.hot_tables import HOT_PREFIX, HotTableCandidate, HotTableManager

ACME_ROWS = [{"id": 1, "name": "acme customer"}]
GLOBEX_ROWS = [{"id": 1, "name": "globex customer"}]


class _Acting:
    """Where the request is acting, and the model that runtime loaded."""

    def __init__(self) -> None:
        self.place = "acme"
        self.stamp: int | None = 7

    def __call__(self) -> tuple[str, int | None]:
        return self.place, self.stamp


@pytest.fixture
def acting(monkeypatch) -> _Acting:
    where = _Acting()
    monkeypatch.setattr(hot_tables, "_scope_parts", where)
    return where


@pytest.fixture
async def manager(acting):
    mgr = HotTableManager(redis_url=None, auto_threshold=100, max_rows=1000)
    await mgr._connect()
    await mgr._redis.flushall()
    yield mgr
    await mgr._redis.flushall()
    await mgr.close()


async def _load(mgr: HotTableManager, rows, *, catalog: str = "pg", schema: str = "public") -> None:
    await mgr._store_rows("customers", rows, "id", catalog, schema)


async def _keys(mgr: HotTableManager) -> list[str]:
    return sorted(await mgr._redis.keys(HOT_PREFIX + "*"))


# --- two orgs ------------------------------------------------------------------------------------


async def test_two_orgs_with_a_same_named_table_never_read_each_others_rows(manager, acting):
    await _load(manager, ACME_ROWS)
    assert manager.is_hot("customers") and await manager.get_rows("customers") == ACME_ROWS

    acting.place = "globex"
    assert not manager.is_hot("customers")
    assert manager.get_entry("customers") is None
    assert await manager.get_rows("customers") == []
    assert "customers" not in manager.managed_tables()
    assert manager.snapshot() == []

    await _load(manager, GLOBEX_ROWS)
    assert await manager.get_rows("customers") == GLOBEX_ROWS
    acting.place = "acme"
    assert await manager.get_rows("customers") == ACME_ROWS
    assert manager.get_entry("customers").rows == ACME_ROWS


async def test_another_worker_of_another_org_does_not_read_the_blob(manager, acting):
    """Two processes share one Redis. The blob one org's worker wrote is not at the key another
    org's worker reads."""
    await _load(manager, ACME_ROWS)
    other = HotTableManager(redis_url=None, auto_threshold=100, max_rows=1000)
    acting.place = "globex"
    await other._store_rows("customers", GLOBEX_ROWS, "id", "pg", "public")
    assert await other.get_rows("customers") == GLOBEX_ROWS
    acting.place = "acme"
    assert await manager.get_rows("customers") == ACME_ROWS
    assert len(await _keys(manager)) == 2
    await other.close()


async def test_no_blob_is_keyed_by_the_bare_table_name(manager, acting):
    await _load(manager, ACME_ROWS)
    (key,) = await _keys(manager)
    assert key != HOT_PREFIX + "customers:blob"
    assert key == HOT_PREFIX + "acme:m7:pg.public.customers:blob"


# --- an environment ------------------------------------------------------------------------------


async def test_a_branch_does_not_read_prods_rows(manager, acting):
    await _load(manager, ACME_ROWS)
    acting.place = "acme_env_staging"
    assert not manager.is_hot("customers") and await manager.get_rows("customers") == []


# --- the model -----------------------------------------------------------------------------------


async def test_a_table_pointed_elsewhere_does_not_serve_the_old_rows(manager, acting):
    """The model changes (the table now reads another relation) and the runtime reloads at the
    next stamp: what was loaded under the previous model is not hot any more."""
    manager.register_candidate(HotTableCandidate("customers", "id", "pg", "public"))
    await _load(manager, ACME_ROWS)

    acting.stamp = 8
    assert not manager.is_hot("customers")
    assert manager.get_entry("customers") is None
    assert await manager.get_rows("customers") == []
    # It is still a candidate, so the next small read of it makes it hot again with the rows
    # that read returned.
    assert "customers" in manager.managed_tables()
    await manager.maybe_promote("customers", [(1, "current row")], ["id", "name"])
    assert await manager.get_rows("customers") == [{"id": 1, "name": "current row"}]


async def test_invalidating_a_table_removes_its_blob_and_only_in_the_acting_org(manager, acting):
    await _load(manager, ACME_ROWS)
    acting.place = "globex"
    await _load(manager, GLOBEX_ROWS)
    await manager.invalidate("customers")
    assert not manager.is_hot("customers")
    assert await _keys(manager) == [HOT_PREFIX + "acme:m7:pg.public.customers:blob"]
    acting.place = "acme"
    assert await manager.get_rows("customers") == ACME_ROWS


# --- two sources in one model --------------------------------------------------------------------


async def test_a_name_two_relations_claim_serves_neither(manager, acting):
    """The hot tier is addressed by table name. When two relations of one model carry the same
    name, the name cannot say whose rows to substitute, so it is not hot at all."""
    await _load(manager, ACME_ROWS, catalog="pg", schema="public")
    await _load(manager, GLOBEX_ROWS, catalog="warehouse", schema="public")
    assert not manager.is_hot("customers")
    assert manager.get_entry("customers") is None
    assert await manager.get_rows("customers") == []
    # Loading either again does not bring the name back.
    await _load(manager, ACME_ROWS, catalog="pg", schema="public")
    assert not manager.is_hot("customers")


# --- the scope is the runtime's ------------------------------------------------------------------


def test_the_scope_is_read_from_the_acting_runtime():
    import provisa.api.app as appmod
    from provisa.core.request_context import current_org

    runtime = appmod.state._active_runtime()
    held = runtime.model_stamp
    runtime.model_stamp = 4321
    token = current_org.set("acme")
    try:
        assert hot_tables._scope_parts() == ("acme", 4321)
    finally:
        current_org.reset(token)
        runtime.model_stamp = held
