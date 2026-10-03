# Copyright (c) 2026 Kenneth Stott
# Canary: 2d6e9f4a-8b3c-4a1e-9f7d-3c5b8a2e6d10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1877: the in-memory, TTL-evicted compiled-query-outcome cache.

Covers the cache class itself (`CompiledQueryCache`) and the key builder
(`compiled_query_cache_key`) in isolation: hit/miss, TTL expiry, key differentiation by
role/person/schema generation/relationship-guard-bypass, and shape-hash literal-independence
(the addendum's core correctness requirement — a raw-text key would miss on every bind-parameter
repeat, since pgwire inlines `$1`/`$2` before the compile stage ever sees the SQL).

Pipeline wiring (`_govern_and_route_planned` in provisa/pgwire/_pipeline.py) is covered
end-to-end by the existing pipeline regression suites (test_high_security_relationship_guard.py,
test_govern_and_route_nf_args.py, test_raw_sql_column_governance.py, etc.), which all still pass
with the cache wired in — this file does not re-run the whole pipeline.
"""

from __future__ import annotations

import time

import pytest

from provisa.compiler.compiled_query_cache import (
    CompiledOutcome,
    CompiledQueryCache,
    compiled_query_cache_key,
    sql_shape_digest,
)


def test_miss_then_hit():
    cache = CompiledQueryCache(ttl_seconds=60)
    key = compiled_query_cache_key(
        "SELECT * FROM sales.orders WHERE id = 1", "analyst", "u1", "boot-1", 1, False
    )
    assert cache.get(key) is None
    cache.put(key, CompiledOutcome())
    hit = cache.get(key)
    assert hit is not None
    assert hit.validated is True


def test_ttl_expiry():
    cache = CompiledQueryCache(ttl_seconds=0.01)
    key = compiled_query_cache_key("SELECT 1", "analyst", "u1", "boot-1", 1, False)
    cache.put(key, CompiledOutcome())
    assert cache.get(key) is not None
    time.sleep(0.03)
    assert cache.get(key) is None
    # An expired entry is evicted on read, not just masked.
    assert len(cache) == 0


@pytest.mark.parametrize(
    "kwargs_a,kwargs_b",
    [
        ({"role_id": "analyst"}, {"role_id": "modeler"}),
        ({"person_id": "u1"}, {"person_id": "u2"}),
        ({"person_id": "u1"}, {"person_id": None}),
        ({"schema_boot_id": "boot-1"}, {"schema_boot_id": "boot-2"}),
        ({"schema_version": 1}, {"schema_version": 2}),
        ({"bypass_relationship_guard": False}, {"bypass_relationship_guard": True}),
        # REQ-1620: a set of held roles acts as its meta-role, which is another role id
        ({"role_id": "analyst"}, {"role_id": "meta:analyst+sales_reader"}),
    ],
)
def test_key_differentiates_by_component(kwargs_a, kwargs_b):
    base = dict(
        sql_text="SELECT * FROM sales.orders",
        role_id="analyst",
        person_id="u1",
        schema_boot_id="boot-1",
        schema_version=1,
        bypass_relationship_guard=False,
    )
    key_a = compiled_query_cache_key(**{**base, **kwargs_a})
    key_b = compiled_query_cache_key(**{**base, **kwargs_b})
    assert key_a != key_b


def test_schema_version_bump_invalidates_before_ttl():
    """A schema mutation bumps state.schema_version; a plan cached under the old generation must
    not be served for the new one, even well inside the TTL window (REQ-1877's invalidation hook,
    same generation pair provisa.compiler.prepared already uses)."""
    cache = CompiledQueryCache(ttl_seconds=3600)
    old_key = compiled_query_cache_key("SELECT 1", "analyst", "u1", "boot-1", 1, False)
    new_key = compiled_query_cache_key("SELECT 1", "analyst", "u1", "boot-1", 2, False)
    cache.put(old_key, CompiledOutcome())
    assert cache.get(old_key) is not None
    # The schema rebuild bumped schema_version; the pipeline now looks up under the new
    # generation's key and correctly misses, even though the TTL has not elapsed.
    assert cache.get(new_key) is None


def test_schema_boot_id_change_invalidates():
    """A process restart (new schema_boot_id) must not serve a plan cached under the old boot."""
    cache = CompiledQueryCache(ttl_seconds=3600)
    old_key = compiled_query_cache_key("SELECT 1", "analyst", "u1", "boot-A", 1, False)
    new_key = compiled_query_cache_key("SELECT 1", "analyst", "u1", "boot-B", 1, False)
    cache.put(old_key, CompiledOutcome())
    assert cache.get(new_key) is None
    assert cache.get(old_key) is not None


def test_clear_drops_every_entry():
    cache = CompiledQueryCache(ttl_seconds=60)
    key = compiled_query_cache_key("SELECT 1", "analyst", "u1", "boot-1", 1, False)
    cache.put(key, CompiledOutcome())
    assert len(cache) == 1
    cache.clear()
    assert len(cache) == 0
    assert cache.get(key) is None


def test_shape_digest_is_literal_independent():
    """The addendum's core correctness requirement: two SQL texts differing only in a literal
    value hash identically, so a repeated shape with a different bind-parameter value still hits
    the cache (pgwire inlines $1/$2 into literal SQL text before the compile stage ever runs —
    provisa/pgwire/server.py's _substitute_params — so a raw-text key would defeat the whole
    point for bind-parameter-using clients)."""
    a = sql_shape_digest("SELECT * FROM sales.orders WHERE id = 1")
    b = sql_shape_digest("SELECT * FROM sales.orders WHERE id = 999999")
    assert a == b


def test_shape_digest_differs_by_structure():
    a = sql_shape_digest("SELECT * FROM sales.orders WHERE id = 1")
    b = sql_shape_digest("SELECT * FROM sales.customers WHERE id = 1")
    assert a != b


def test_key_uses_shape_not_raw_text():
    key_a = compiled_query_cache_key(
        "SELECT * FROM sales.orders WHERE id = 1", "analyst", "u1", "boot-1", 1, False
    )
    key_b = compiled_query_cache_key(
        "SELECT * FROM sales.orders WHERE id = 42", "analyst", "u1", "boot-1", 1, False
    )
    assert key_a == key_b


def test_default_ttl_from_env(monkeypatch):
    monkeypatch.setenv("PROVISA_COMPILED_QUERY_CACHE_TTL_SECONDS", "5")
    import importlib

    import provisa.compiler.compiled_query_cache as mod

    importlib.reload(mod)
    try:
        cache = mod.CompiledQueryCache()
        assert cache._ttl == 5
    finally:
        monkeypatch.delenv("PROVISA_COMPILED_QUERY_CACHE_TTL_SECONDS", raising=False)
        importlib.reload(mod)


def test_max_entries_bound_evicts_rather_than_grows_unbounded():
    cache = CompiledQueryCache(ttl_seconds=3600)
    from provisa.compiler import compiled_query_cache as mod

    original_max = mod._MAX_ENTRIES
    mod._MAX_ENTRIES = 4
    try:
        for i in range(10):
            key = compiled_query_cache_key(f"SELECT {i}", "analyst", "u1", "boot-1", 1, False)
            cache.put(key, CompiledOutcome())
        assert len(cache) <= mod._MAX_ENTRIES
    finally:
        mod._MAX_ENTRIES = original_max
