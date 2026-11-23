# Copyright (c) 2026 Kenneth Stott
# Canary: 9b2e7c41-6a3f-4d18-8e5b-1f7a0c3d2e94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The request pre-key: an exact repeat of a request skips parsing and compiling (REQ-1897,
amended 2026-11-23, REQUEST PRE-KEY).

The one pipeline's two entries (``_pipeline._govern_and_route`` for SQL over pgwire, /data/sql and
Flight SQL; ``_pipeline._govern_and_route_compiled`` for Cypher over HTTP, Bolt and Flight, GraphQL,
gRPC, REST and JSON:API) key the request they were handed -- its text, its bound values, the
caller's role, session context and tenant -- to the response-cache key(s) its first compile
produced. A repeat is a hash and a lookup in the org's plan store, then the response-cache read:
no governance, no routing.

Lookup order on every request: this pre-key (text + values) -> the text-only kept plan
(``governed_plan.PlanSlot``: the governed statement, keyed without its values, so the same text
with new values binds and runs) -> full normalisation.

The pre-key is a :class:`~provisa.pgwire.governed_plan.PlanSlot` in its own stage, so it lives in
the org's ``compiled_query_cache`` (per worker, in memory, bounded, least-recently-used eviction)
and is dropped under the same rules as every kept plan:

- the model: ``schema_boot_id`` + ``schema_version`` are in its key, and the tenant it records
  (``cache.tenancy.cache_tenant``) carries the loaded model stamp;
- the caller's rights: the role record, its compilation and RLS contexts and the masking rules
  are anchors compared by identity, and the reader's entitlement mask is in the key;
- anything its compile read: the registered tables, relationships, metrics, source types and the
  bound engine (anchors), the default row limit and security mode (settings);
- it never crosses a role (role in the key), a session context (session variables and the
  temporary-table session in the key) or a tenant (org, environment, region and model in the key).

Values are part of the result's identity: a pre-key always carries the request's values, typed
(:func:`typed_values`), so ``1`` and ``'1'`` never share an entry. When the response-cache entry it
names has expired or been invalidated, the lookup misses and the request takes the full path."""

# Requirements: REQ-1897

from __future__ import annotations

import contextvars
import json
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from provisa.pgwire.governed_plan import PlanSlot

#: The pre-key the request now being planned records into on a response-cache miss. Set by the
#: pipeline entry for the duration of one request's planning; taken (and cleared) by the first
#: cache read made before routing, so a statement planned inside it (a write's key read) never
#: records under the request's pre-key.
_PENDING: contextvars.ContextVar[PlanSlot | None] = contextvars.ContextVar(
    "provisa_request_prekey", default=None
)


def _typed(value: Any) -> Any:
    """A JSON-safe value that keeps the value's type, so distinct values never share a digest."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return [type(value).__name__, value]
    if isinstance(value, (list, tuple)):
        return [type(value).__name__, [_typed(v) for v in value]]
    if isinstance(value, dict):
        return ["dict", sorted(([str(k), _typed(v)] for k, v in value.items()))]
    return [type(value).__qualname__, repr(value)]


def typed_values(values: Any) -> str:
    """The request's bound values as one canonical string."""
    return json.dumps(_typed(values), separators=(",", ":"))


def slot(state: Any, stage: str, role_id: str, *body: Any) -> PlanSlot:
    """The pre-key of one request at one pipeline entry. ``body`` is everything the entry's answer
    depends on beyond the generation, role, anchors and settings ``PlanSlot`` already holds; the
    tenant, temporary-table session and zone every entry shares are added here."""
    from provisa.cache.tenancy import cache_tenant
    from provisa.compiler import temp_tables
    from provisa.core.session_time_zone import zone_cache_key

    return PlanSlot(
        state,
        f"prekey:{stage}",
        role_id,
        *body,
        cache_tenant(state),
        temp_tables.plan_key(),
        temp_tables.slot_key(),
        zone_cache_key(),
    )


@contextmanager
def recording(pending: PlanSlot | None) -> Generator[None, None, None]:
    """Bind ``pending`` as the pre-key this request's first cache read records into."""
    token = _PENDING.set(pending)
    try:
        yield
    finally:
        _PENDING.reset(token)


def take() -> PlanSlot | None:
    """The pre-key bound for this request, cleared so nothing planned after records into it."""
    pending = _PENDING.get()
    if pending is not None:
        _PENDING.set(None)
    return pending


__all__ = ["recording", "slot", "take", "typed_values"]
