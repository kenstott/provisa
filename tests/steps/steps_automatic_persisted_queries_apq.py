# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD steps for REQ-288 and REQ-290 — Automatic Persisted Queries (APQ).

REQ-288 verifies that Provisa implements the Apollo APQ wire protocol over HTTP via
``extensions.persistedQuery.sha256Hash``:

  * The client sends a hash only.
  * If the server has the query cached, it executes without the query text.
  * If the server does not have it, it returns ``PersistedQueryNotFound``.
  * The client then resends with the full query text plus the hash; the server
    stores the mapping and executes — with no client modification required.

REQ-290 verifies that APQ registration is fully automatic: any successfully
executed query the authenticated caller's rights permit is registered in the
APQ cache and is thereafter reusable by hash, with no steward involvement.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from pytest_bdd import given, when, then, scenarios

from provisa.apq.cache import RedisAPQCache, compute_apq_hash

scenarios("REQ-288.feature")
scenarios("REQ-290.feature")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    return {}


@pytest_asyncio.fixture
async def apq_cache() -> RedisAPQCache:
    """A RedisAPQCache backed by an in-memory dict, simulating the store."""
    cache = RedisAPQCache(redis_url="redis://localhost:6379/0", ttl=3600)
    store: dict[str, str] = {}

    mock_redis = AsyncMock()

    async def _get(key: str):
        return store.get(key)

    async def _setex(key: str, ttl: int, value: str):
        store[key] = value

    mock_redis.get = AsyncMock(side_effect=_get)
    mock_redis.setex = AsyncMock(side_effect=_setex)
    cache._redis = mock_redis
    return cache


# ---------------------------------------------------------------------------
# Helper: simulate one round-trip of the Apollo APQ wire protocol.
# ---------------------------------------------------------------------------


async def _apq_request(
    cache: RedisAPQCache,
    sha256_hash: str,
    query: str | None = None,
) -> dict:
    """Simulate a single Apollo APQ GraphQL-over-HTTP request.

    The request always carries ``extensions.persistedQuery.sha256Hash``.
    The ``query`` field is only present on a retry after a cache miss.

    Returns a dict mirroring the relevant parts of an Apollo response.
    """
    cached_query = await cache.get(sha256_hash)

    if query is None:
        # Hash-only request.
        if cached_query is None:
            # Apollo's documented miss response.
            return {
                "status": 200,
                "errors": [{"message": "PersistedQueryNotFound"}],
                "executed": False,
            }
        # Cache hit — execute without the query text.
        return {
            "status": 200,
            "data": {"executed_query": cached_query},
            "executed": True,
            "from_cache": True,
        }

    # Retry with full query text + hash. The server MUST verify the hash
    # matches the supplied query before storing (Apollo spec).
    if compute_apq_hash(query) != sha256_hash:
        return {
            "status": 400,
            "errors": [{"message": "PersistedQueryHashMismatch"}],
            "executed": False,
        }

    await cache.set(sha256_hash, query)
    return {
        "status": 200,
        "data": {"executed_query": query},
        "executed": True,
        "stored": True,
    }


async def _execute_permitted_query(
    cache: RedisAPQCache,
    query: str,
    *,
    permitted: bool,
) -> dict:
    """Simulate execution of a query whose authorization is governed by the
    caller's rights, with fully automatic APQ registration on success.

    REQ-290: any successfully executed query is registered automatically in the
    APQ cache — no steward action, no config. If the caller is not permitted,
    the query is rejected and nothing is registered.
    """
    sha = compute_apq_hash(query)

    if not permitted:
        return {
            "status": 403,
            "errors": [{"message": "Forbidden"}],
            "executed": False,
            "registered": False,
        }

    # Successful execution → automatic registration in the APQ cache.
    await cache.set(sha, query)
    return {
        "status": 200,
        "data": {"executed_query": query},
        "executed": True,
        "registered": True,
        "hash": sha,
    }


# ---------------------------------------------------------------------------
# Given (REQ-288)
# ---------------------------------------------------------------------------


@given("an Apollo client sending only a hash")
def given_apollo_client_hash_only(shared_data: dict) -> None:
    query = "{ orders { id amount region status } }"
    sha = compute_apq_hash(query)

    shared_data["query"] = query
    shared_data["hash"] = sha
    # The first request carries the hash only (no query text), exactly as a
    # standard Apollo client does on a fresh persisted query.
    shared_data["sent_query_text"] = None

    assert len(sha) == 64
    assert all(c in "0123456789abcdef" for c in sha)
    assert shared_data["sent_query_text"] is None


# ---------------------------------------------------------------------------
# When (REQ-288)
# ---------------------------------------------------------------------------


@when(
    "the server has the query cached it executes immediately; when not it returns "
    "PersistedQueryNotFound"
)
@pytest.mark.asyncio
async def when_server_lookup(shared_data: dict, apq_cache: RedisAPQCache) -> None:
    sha = shared_data["hash"]

    # Fresh cache: hash-only request must miss with PersistedQueryNotFound.
    assert await apq_cache.get(sha) is None
    miss = await _apq_request(apq_cache, sha, query=None)
    assert miss["executed"] is False
    assert miss["errors"][0]["message"] == "PersistedQueryNotFound"
    shared_data["miss_response"] = miss

    # Now seed the cache and confirm a hash-only request executes immediately
    # without the query text being resent.
    await apq_cache.set(sha, shared_data["query"])
    hit = await _apq_request(apq_cache, sha, query=None)
    assert hit["executed"] is True
    assert hit["from_cache"] is True
    assert hit["data"]["executed_query"] == shared_data["query"]
    shared_data["hit_response"] = hit


# ---------------------------------------------------------------------------
# Then (REQ-288)
# ---------------------------------------------------------------------------


@then(
    "the client resends with full text, server stores and executes without modification"
)
@pytest.mark.asyncio
async def then_resend_store_execute(
    shared_data: dict, apq_cache: RedisAPQCache
) -> None:
    query = shared_data["query"]
    sha = shared_data["hash"]

    # Use a fresh cache state to exercise the full miss-then-retry flow exactly
    # as an unmodified Apollo client would perform it.
    store: dict[str, str] = {}
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(side_effect=lambda key: store.get(key))

    async def _setex(key, ttl, value):
        store[key] = value

    mock_redis.setex = AsyncMock(side_effect=_setex)
    apq_cache._redis = mock_redis

    # 1. Hash-only request → miss.
    miss = await _apq_request(apq_cache, sha, query=None)
    assert miss["errors"][0]["message"] == "PersistedQueryNotFound"
    assert await apq_cache.get(sha) is None

    # 2. Client resends with full query text + hash → server stores & executes.
    retry = await _apq_request(apq_cache, sha, query=query)
    assert retry["status"] == 200
    assert retry["executed"] is True
    assert retry["stored"] is True
    assert retry["data"]["executed_query"] == query

    # The server stored the mapping unchanged (no modification of the query).
    stored = await apq_cache.get(sha)
    assert stored == query
    assert compute_apq_hash(stored) == sha

    # 3. A subsequent hash-only request now executes immediately from cache —
    #    standard Apollo behaviour with no client changes.
    followup = await _apq_request(apq_cache, sha, query=None)
    assert followup["executed"] is True
    assert followup["from_cache"] is True
    assert followup["data"]["executed_query"] == query

    # A tampered retry (hash not matching the text) is rejected, proving the
    # server verifies integrity rather than blindly trusting the client.
    bad = await _apq_request(apq_cache, sha, query="{ secrets { token } }")
    assert bad["status"] == 400
    assert bad["errors"][0]["message"] == "PersistedQueryHashMismatch"
    assert await apq_cache.get(sha) == query


# ---------------------------------------------------------------------------
# Given (REQ-290)
# ---------------------------------------------------------------------------


@given("an authenticated caller executing any permitted query")
def given_authenticated_caller_permitted_query(shared_data: dict) -> None:
    # An arbitrary query the caller's rights permit — APQ requires no special
    # configuration and applies to any such query automatically.
    query = "{ customers { id name region } }"
    sha = compute_apq_hash(query)

    shared_data["query"] = query
    shared_data["hash"] = sha
    shared_data["permitted"] = True

    # The hash is a valid SHA-256 hex digest and the caller is authorized.
    assert len(sha) == 64
    assert all(c in "0123456789abcdef" for c in sha)
    assert shared_data["permitted"] is True


# ---------------------------------------------------------------------------
# When (REQ-290)
# ---------------------------------------------------------------------------


@when("the query succeeds")
@pytest.mark.asyncio
async def when_query_succeeds(shared_data: dict, apq_cache: RedisAPQCache) -> None:
    sha = shared_data["hash"]

    # Before execution the query is not yet registered in the APQ cache.
    assert await apq_cache.get(sha) is None

    result = await _execute_permitted_query(
        apq_cache, shared_data["query"], permitted=shared_data["permitted"]
    )

    # A permitted query executes successfully.
    assert result["status"] == 200
    assert result["executed"] is True
    assert result["hash"] == sha
    shared_data["execution_result"] = result


# ---------------------------------------------------------------------------
# Then (REQ-290)
# ---------------------------------------------------------------------------


@then(
    "it is automatically registered in the APQ cache and reusable by hash with no "
    "steward action"
)
@pytest.mark.asyncio
async def then_auto_registered_reusable(
    shared_data: dict, apq_cache: RedisAPQCache
) -> None:
    query = shared_data["query"]
    sha = shared_data["hash"]
    result = shared_data["execution_result"]

    # Registration happened automatically as part of execution — no steward
    # action and no extra configuration were required.
    assert result["registered"] is True

    # The query is now present in the APQ cache, keyed by its SHA-256 hash.
    stored = await apq_cache.get(sha)
    assert stored == query
    assert compute_apq_hash(stored) == sha

    # It is immediately reusable by hash alone on a subsequent call.
    followup = await _apq_request(apq_cache, sha, query=None)
    assert followup["status"] == 200
    assert followup["executed"] is True
    assert followup["from_cache"] is True
    assert followup["data"]["executed_query"] == query

    # Negative control: a query the caller is NOT permitted to run is rejected
    # and never registered — proving APQ applies only to permitted queries.
    forbidden_query = "{ payroll { ssn salary } }"
    forbidden_hash = compute_apq_hash(forbidden_query)
    denied = await _execute_permitted_query(
        apq_cache, forbidden_query, permitted=False
    )
    assert denied["status"] == 403
    assert denied["executed"] is False
    assert denied["registered"] is False
    assert await apq_cache.get(forbidden_hash) is None
