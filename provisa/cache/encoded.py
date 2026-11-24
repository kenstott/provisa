# Copyright (c) 2026 Kenneth Stott
# Canary: 9b1f6e3a-27c4-4d8e-b5a0-6c3e8f1d2a47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Encoded responses over the Arrow result cache (REQ-1867, amended 2026-11-23, ENCODED RESPONSES).

A response-cache hit is a ``CachedResult`` read under its key (``cache.middleware.check_cache``).
Beside that entry, each surface keeps the exact bytes it answered the result with, under a field
naming the surface and everything that shapes the bytes (format, the GraphQL selection and its
aliases, the Cypher statement, pagination): :func:`shape_field`. A later hit on the same entry with
the same shape writes those bytes as they are; a hit with a shape not yet kept is encoded from the
Arrow entry once and its bytes kept (:func:`keep`). That is the design, not a fallback: the Arrow
entry is the result, and every encoded response is a copy of one surface's answer of it.

What is kept is only the part of a response the result decides. Content unique to a request
(request and trace identifiers, timings, the stats) is never in the kept bytes; the surface adds
it on the way out. The warnings encoding raised are kept with the bytes and raised again on a hit,
so the response says what the miss's said. An encoded response larger than
:data:`MAX_ENCODED_BYTES` is not kept: its hits are encoded from the Arrow entry.

Encoded responses share their entry's identity (key, tenant, role, session, masks), life (they
expire with it) and invalidation (the store drops them with it): ``cache.store``.
"""

# Requirements: REQ-1867

from __future__ import annotations

import hashlib
import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import orjson

from provisa.cache.store import CachedResult, CacheStore
from provisa.core.statement_warnings import ServerWarning, raised, warn

log = logging.getLogger(__name__)

# REQ-1867 (ENCODED RESPONSES): "Encoded responses are kept only under a size cap; a larger
# result streams from the Arrow entry." The Arrow entry is already bounded in rows by the org's
# large-result threshold (cache.raw_sql); this bounds a kept copy's bytes.
MAX_ENCODED_BYTES = 1 << 20

_LEN = 4  # bytes of the header-length prefix


@dataclass(frozen=True)
class Encoded:
    """One surface's encoded answer of a cached result: ``parts`` its bytes, in the order the
    surface writes them, ``facts`` what the surface reports beside them (row counts and the
    like, small JSON values) and ``warnings`` what encoding the answer raised."""

    parts: tuple[bytes, ...]
    facts: dict[str, Any] = field(default_factory=dict)
    warnings: tuple[ServerWarning, ...] = ()


def shape_field(surface: str, *shape: Any) -> str:  # noqa: ANN401 — whatever shapes the bytes
    """The field an encoded response is kept under: the surface and everything that shapes its
    bytes. Each ``shape`` value is written by its ``repr``: the surfaces pass dataclasses, tuples,
    strings and numbers, whose repr is the same in every process (a value whose repr were not
    would only name a field no hit finds, never another shape's bytes)."""
    digest = hashlib.sha256(repr((surface, shape)).encode("utf-8")).hexdigest()
    return f"{surface}:{digest}"


def pack(encoded: Encoded) -> bytes:
    """``encoded`` as the bytes the store keeps: a length-prefixed JSON header (each part's
    length, the facts, the warnings), then the parts as they are."""
    header = orjson.dumps(
        {
            "lengths": [len(p) for p in encoded.parts],
            "facts": encoded.facts,
            "warnings": [w.as_dict() for w in encoded.warnings],
        }
    )
    return len(header).to_bytes(_LEN, "big") + header + b"".join(encoded.parts)


def unpack(data: bytes) -> Encoded:
    """What :func:`pack` wrote."""
    size = int.from_bytes(data[:_LEN], "big")
    header = orjson.loads(data[_LEN : _LEN + size])
    view = memoryview(data)
    at = _LEN + size
    parts: list[bytes] = []
    for length in header["lengths"]:
        parts.append(bytes(view[at : at + length]))
        at += length
    if at != len(data):
        raise ValueError("encoded response: its parts do not fill what was kept")
    warnings = tuple(
        ServerWarning(code=w["code"], message=w["message"], params=w["params"])
        for w in header["warnings"]
    )
    return Encoded(tuple(parts), header["facts"], warnings)


async def served(state: Any, hit: CachedResult, field_name: str) -> Encoded | None:  # noqa: ANN401 — AppState
    """The encoded response kept as ``field_name`` beside ``hit`` in the one response cache
    (``state.response_cache_store``), its warnings raised again on this request; None when none
    is kept (the surface then encodes the hit and :func:`keep` s it)."""
    store: CacheStore = state.response_cache_store
    if hit.key is None:
        raise ValueError("encoded.served: the hit names no key (read it with check_cache)")
    data = await store.get_encoded(hit.key, field_name, tenant_id=hit.tenant_id)
    if data is None:
        return None
    encoded = unpack(data)
    for w in encoded.warnings:
        warn(w)
    return encoded


async def keep(state: Any, hit: CachedResult, field_name: str, encoded: Encoded) -> bool:  # noqa: ANN401 — AppState
    """Keep ``encoded`` beside ``hit`` in the one response cache (``state.response_cache_store``)
    under ``field_name`` when it is within
    :data:`MAX_ENCODED_BYTES`. Returns whether it was kept: the store keeps nothing beside an
    entry dropped or written again since ``hit`` was read."""
    data = pack(encoded)
    if len(data) > MAX_ENCODED_BYTES:
        return False
    store: CacheStore = state.response_cache_store
    return await store.set_encoded(hit, field_name, data)


def warnings_since(before: Sequence[ServerWarning]) -> tuple[ServerWarning, ...]:
    """The warnings raised since the collector held ``before``: what encoding an answer raised,
    kept with its bytes."""
    return tuple(w for w in raised() if w not in before)


__all__ = [
    "MAX_ENCODED_BYTES",
    "Encoded",
    "keep",
    "pack",
    "served",
    "shape_field",
    "unpack",
    "warnings_since",
]
