# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7d2e91-6c4f-4a08-b5e2-8f1a9c0d7e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-source live-read permits (REQ-1909): acquire/release, leases, the deadline-bounded wait,
ordered multi-source acquisition, stream release, and which sources a plan reads live."""

# Requirements: REQ-1909

from __future__ import annotations

import threading
import time
import uuid
from types import SimpleNamespace

import pytest

from provisa.core import request_deadline
from provisa.federation import live_concurrency as lc
from provisa.transpiler.router import Route


@pytest.fixture
def store() -> lc.LivePermitStore:
    return lc.LivePermitStore(None)


@pytest.fixture
def org() -> str:
    # The embedded store shares one FakeServer per process: a unique org keeps tests independent.
    return f"org-{uuid.uuid4().hex[:8]}"


def test_permits_up_to_the_cap_then_none(store, org):
    key = store.key(org, "src")
    t1 = store.try_acquire(key, 2)
    t2 = store.try_acquire(key, 2)
    assert t1 and t2
    assert store.try_acquire(key, 2) is None
    assert store.holders(key) == 2
    store.release(key, t1)
    assert store.try_acquire(key, 2) is not None


def test_an_unrenewed_lease_lapses_and_frees_its_permit(store, org, monkeypatch):
    monkeypatch.setattr(lc, "LEASE_S", 0.3)
    key = store.key(org, "src")
    assert store.try_acquire(key, 1) is not None
    assert store.try_acquire(key, 1) is None
    time.sleep(0.45)  # the holder "crashed": no renewal
    assert store.try_acquire(key, 1) is not None


def test_renewal_keeps_a_live_lease(store, org, monkeypatch):
    monkeypatch.setattr(lc, "LEASE_S", 0.4)
    key = store.key(org, "src")
    assert store.try_acquire(key, 1) is not None
    for _ in range(3):
        time.sleep(0.25)
        store.renew_all()
    assert store.try_acquire(key, 1) is None


def test_a_full_source_fails_at_the_request_deadline_naming_the_source_and_cap(store, org):
    key = store.key(org, "orders_pg")
    assert store.try_acquire(key, 2) and store.try_acquire(key, 2)
    t0 = time.monotonic()
    with request_deadline.within(0.4):
        with pytest.raises(lc.LiveConcurrencyExceeded, match=r"'orders_pg'.*cap of 2"):
            lc.acquire(store, org, [("orders_pg", 2)])
    assert 0.35 <= time.monotonic() - t0 < 1.5


def test_a_waiter_runs_as_soon_as_a_permit_is_released(store, org):
    key = store.key(org, "src")
    token = store.try_acquire(key, 1)
    assert token is not None
    threading.Timer(0.2, store.release, args=(key, token)).start()
    t0 = time.monotonic()
    with request_deadline.within(5.0):
        permits = lc.acquire(store, org, [("src", 1)])
    assert permits.count == 1
    assert 0.15 <= time.monotonic() - t0 < 2.0
    permits.release()
    assert store.holders(key) == 0


def test_opposite_orders_never_deadlock(store, org):
    """Two queries naming the same two cap-1 sources in opposite orders both complete."""
    done: list[str] = []
    errors: list[BaseException] = []

    def run(name: str, pairs: list[tuple[str, int]]) -> None:
        try:
            with request_deadline.within(10.0):
                for _ in range(10):
                    with lc.acquire(store, org, pairs):
                        time.sleep(0.01)
            done.append(name)
        except BaseException as e:  # surfaced below
            errors.append(e)

    a = threading.Thread(target=run, args=("a", [("x", 1), ("y", 1)]))
    b = threading.Thread(target=run, args=("b", [("y", 1), ("x", 1)]))
    a.start(), b.start()
    a.join(20), b.join(20)
    assert not errors
    assert sorted(done) == ["a", "b"]


def test_partial_acquisition_is_released_when_a_later_source_times_out(store, org):
    key_b = store.key(org, "b")
    assert store.try_acquire(key_b, 1) is not None
    with request_deadline.within(0.3):
        with pytest.raises(lc.LiveConcurrencyExceeded):
            lc.acquire(store, org, [("a", 1), ("b", 1)])
    assert store.holders(store.key(org, "a")) == 0


def test_a_stream_releases_when_closed_early_or_failed(store, org):
    key = store.key(org, "src")
    permits = lc.acquire(store, org, [("src", 1)])
    rows = permits.guard(iter(range(100)))
    assert next(rows) == 0
    rows.close()  # client disconnected mid-stream
    assert store.holders(key) == 0

    permits = lc.acquire(store, org, [("src", 1)])

    def boom():
        yield 1
        raise RuntimeError("engine died")

    with pytest.raises(RuntimeError):
        list(permits.guard(boom()))
    assert store.holders(key) == 0


def _src(sid: str, typ: str, *, prefer=False, protected=False, cap=None):
    return SimpleNamespace(
        id=sid,
        type=SimpleNamespace(value=typ),
        prefer_materialized=prefer,
        load_protected=protected,
        max_live_concurrency=cap,
    )


def _state(attaching: set[str]):
    connectors = {t: SimpleNamespace(reads_in_place=True, mechanism="ATTACH_X") for t in attaching}
    return SimpleNamespace(federation_engine=SimpleNamespace(connectors=connectors))


def test_live_sources_direct_route_is_its_one_source():
    plan = SimpleNamespace(route=Route.DIRECT, source_id="pg", sources=frozenset({"pg"}))
    assert lc._live_source_ids(_state(set()), plan, {"pg": _src("pg", "postgresql")}) == ["pg"]


def test_live_sources_engine_route_counts_only_attached_sources_not_on_their_replica():
    by_id = {
        "pg": _src("pg", "postgresql"),
        "mongo": _src("mongo", "mongodb", prefer=True),
        "ch": _src("ch", "clickhouse", protected=True),
        "api": _src("api", "openapi"),
    }
    plan = SimpleNamespace(route=Route.ENGINE, source_id="", sources=frozenset(by_id))
    live = lc._live_source_ids(_state({"postgresql", "mongodb", "clickhouse"}), plan, by_id)
    assert live == ["pg"]


def test_live_sources_api_route_is_its_upstream():
    plan = SimpleNamespace(route=Route.API, source_id="petstore", sources=frozenset({"petstore"}))
    by_id = {"petstore": _src("petstore", "openapi")}
    assert lc._live_source_ids(_state(set()), plan, by_id) == ["petstore"]


def test_live_sources_cache_route_reads_nothing_live():
    plan = SimpleNamespace(route=Route.CACHE, source_id="", sources=frozenset({"pg"}))
    assert lc._live_source_ids(_state({"postgresql"}), plan, {"pg": _src("pg", "postgresql")}) == []


def test_a_plan_with_no_capped_live_source_takes_no_permit_and_touches_no_store():
    plan = SimpleNamespace(live_caps=(), live_caps_org=None)
    permits = lc.acquire_plan_permits(SimpleNamespace(), plan)  # no live_permit_store needed
    assert permits.count == 0
    permits.release()


def test_a_plan_acquires_the_caps_bound_when_it_was_minted(store, org):
    plan = SimpleNamespace(live_caps=(("src", 1),), live_caps_org=org)
    state = SimpleNamespace(live_permit_store=store)
    with lc.acquire_plan_permits(state, plan) as permits:
        assert permits.count == 1
        assert store.holders(store.key(org, "src")) == 1
    assert store.holders(store.key(org, "src")) == 0
