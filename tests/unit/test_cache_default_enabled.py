# REQ-985: caching defaults to ENABLED. A CacheStore always exists — RedisCacheStore(None)
# falls back to embedded fakeredis when no Redis URL is configured; only an explicit
# `cache.enabled: false` selects the NoopCacheStore.
import pytest

from provisa.cache.store import CachedResult, NoopCacheStore, RedisCacheStore


def _select_store(cache_config: dict) -> object:
    # Mirrors the production decision in provisa/api/app.py: default enabled=True, and an
    # enabled store is RedisCacheStore(url_or_None) — never absent. Explicit false => Noop.
    if cache_config.get("enabled", True):
        return RedisCacheStore(cache_config.get("redis_url"))
    return NoopCacheStore()


def test_omitted_enabled_defaults_to_redis_store():
    assert isinstance(_select_store({}), RedisCacheStore)


def test_explicit_false_selects_noop_store():
    assert isinstance(_select_store({"enabled": False}), NoopCacheStore)


def test_explicit_true_selects_redis_store():
    assert isinstance(_select_store({"enabled": True}), RedisCacheStore)


@pytest.mark.asyncio
async def test_redis_store_without_url_roundtrips_via_embedded_fakeredis():
    store = RedisCacheStore(None)  # no Redis URL → embedded fakeredis, never a "no cache" state
    await store.set("k", b"payload", ttl=60)
    got = await store.get("k")
    assert isinstance(got, CachedResult)
    assert got.data == b"payload"
    await store.close()


@pytest.mark.asyncio
async def test_noop_store_never_caches():
    store = NoopCacheStore()
    await store.set("k", b"payload", ttl=60)
    assert await store.get("k") is None
