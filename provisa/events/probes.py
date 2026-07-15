# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""probe_type — the event loop's input-side change-detection axis (REQ-982).

Orthogonal to the change_signal cadence (ttl | probe | ttl_probe), probe_type selects HOW a poll node
detects change and — the key property — IMPLIES the landing shape:

    watermark -> append   (MAX(wm) advanced; fetch the delta past the cursor; needs watermark_column)
    hash      -> replace   (a content token differs; no row-level info → full refresh)
    count     -> replace   (count(*) changed; coarse, can't localize → full refresh)
    none      -> replace   (no input probe; re-fetch on cadence, REQ-981 output hash gates the ripple)

Availability is gated by the source's capability class (``probe_capabilities``): a file source cannot
watermark, an HTTP API's cheapest probe is an ETag (hash), etc. Each type maps to a per-source
transport returning ``freshness_token() -> str | None``; a None token means the source cannot produce
one this call, degrading the node to its TTL cadence (REQ-847 capability signal, never a silent stale
fallback). The token baseline is persisted per node and compared for equality — Provisa never
interprets a token, only compares stored vs. fresh.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from provisa.core.change_signal import APPEND, REPLACE

# -- the axis ------------------------------------------------------------------
WATERMARK = "watermark"
HASH = "hash"
COUNT = "count"
NONE = "none"
VALID_PROBE_TYPES = frozenset({WATERMARK, HASH, COUNT, NONE})

# -- capability classes (by source type) ---------------------------------------
# Streaming/push sources are not on the probe axis (their change_signal is push, not ttl/probe).
_STREAMING_TYPES = frozenset({"kafka", "websocket", "ingest"})
# File/object sources: cheapest token is mtime+size (a hash); a scan-based watermark/count would defeat
# the point, so only hash | none.
_FILE_TYPES = frozenset({"csv", "parquet", "sqlite", "files"})
# HTTP/API sources: ETag/Last-Modified (hash) is the cheapest; count via a total header, watermark via
# a sortable cursor — both when the API exposes them.
_HTTP_API_TYPES = frozenset(
    {"openapi", "graphql_remote", "grpc_remote", "rss", "prometheus", "google_sheets"}
)

_ALL = frozenset({WATERMARK, HASH, COUNT, NONE})


def probe_capabilities(source_type: str) -> frozenset[str]:
    """The probe_types a source of ``source_type`` supports (REQ-982). SQL/engine-scannable sources
    (the open-ended default) support all four; HTTP APIs all four (watermark/count conditional on the
    API surface); files hash|none; streaming/push sources none (not on the probe axis)."""
    if source_type in _STREAMING_TYPES:
        return frozenset()
    if source_type in _FILE_TYPES:
        return frozenset({HASH, NONE})
    if source_type in _HTTP_API_TYPES:
        return _ALL
    return _ALL  # RDBMS / DW / OLAP / lake / connector NoSQL / graph — engine-scannable


def probe_shape(probe_type: str) -> str:
    """The landing shape a probe_type implies (REQ-982): watermark → append (delta past the cursor);
    hash/count/none → replace (full refresh). This is the single source of truth that supersedes the
    ``watermark_column``-presence heuristic once a probe_type is declared."""
    if probe_type not in VALID_PROBE_TYPES:
        raise ValueError(
            f"invalid probe_type {probe_type!r}; expected one of {sorted(VALID_PROBE_TYPES)}"
        )
    return APPEND if probe_type == WATERMARK else REPLACE


def resolve_probe_type(
    explicit: str | None,
    *,
    source_type: str,
    change_signal: str,
    has_watermark: bool,
) -> str:
    """Resolve the effective probe_type (REQ-982).

    ``change_signal == 'ttl'`` forces ``none`` (cadence-only, no input probe). An explicit probe_type
    is validated against ``probe_capabilities(source_type)`` and returned. When unset under a probing
    cadence (probe / ttl_probe), the per-class default applies: SQL → watermark if a watermark column
    exists else count; HTTP API / file → hash. Raises on a type outside the source's capability set."""
    from provisa.core.change_signal import is_push

    if is_push(change_signal):
        return NONE  # push sources are not polled; probe_type is inert
    if change_signal == "ttl":
        if explicit not in (None, NONE):
            raise ValueError(
                f"change_signal=ttl requires probe_type=none (cadence-only), got {explicit!r}"
            )
        return NONE

    caps = probe_capabilities(source_type)
    if explicit is not None:
        if explicit not in caps:
            raise ValueError(
                f"probe_type {explicit!r} not supported by source type {source_type!r}; "
                f"supported: {sorted(caps)}"
            )
        # REQ-982: the watermark type IS the cursor probe → it requires a watermark_column to filter
        # WHERE wm > cursor. Fail loud at config time — never silently degrade an append node to a
        # full replace because its cursor input is missing.
        if explicit == WATERMARK and not has_watermark:
            raise ValueError("probe_type=watermark requires a watermark_column")
        return explicit

    # default per capability class
    if source_type in _FILE_TYPES or source_type in _HTTP_API_TYPES:
        return HASH
    return WATERMARK if has_watermark else COUNT


# -- transports (probe_type → freshness_token) ---------------------------------
# A transport is ``async () -> str | None`` — the current opaque token, or None when the source cannot
# produce one this call (→ TTL degrade). SQL transports are built over an injected scalar query runner
# so they are engine-agnostic and unit-testable without a live engine.

QueryScalar = Callable[[str], Awaitable[Any]]


async def sql_watermark_token(
    query_scalar: QueryScalar, ref: str, watermark_column: str
) -> str | None:
    """MAX(watermark) as the token — advances monotonically as rows land (REQ-982 watermark probe)."""
    value = await query_scalar(f'SELECT MAX("{watermark_column}") FROM {ref}')
    return None if value is None else str(value)


async def sql_count_token(query_scalar: QueryScalar, ref: str) -> str | None:
    """COUNT(*) as the token — a cheap coarse change signal (REQ-982 count probe). Misses same-count
    updates (delete+insert / in-place); pair with the REQ-981 output hash as the exact backstop."""
    value = await query_scalar(f"SELECT COUNT(*) FROM {ref}")
    return None if value is None else str(value)


Transport = Callable[[], Awaitable["str | None"]]


def build_probe(
    probe_type: str,
    *,
    query_scalar: QueryScalar | None = None,
    ref: str | None = None,
    watermark_column: str | None = None,
) -> Transport:
    """Build the ``freshness_token`` transport for a node from its ``probe_type`` (REQ-982).

    watermark/count over a SQL-scannable source are wired here; ``hash`` on a SQL source has no cheap
    token (a full-scan checksum is the fetch itself), and ``none`` never probes — both return a
    None-token transport that degrades the node to its TTL cadence, where the REQ-981 output hash gate
    still suppresses an unchanged ripple. HTTP/file hash transports (ETag / mtime) plug in here as they
    are wired; until then they too degrade to TTL."""

    async def _none() -> str | None:
        return None

    if probe_type == WATERMARK:
        if query_scalar is None or ref is None or watermark_column is None:
            return _none  # not wired for this source → degrade
        return lambda: sql_watermark_token(query_scalar, ref, watermark_column)
    if probe_type == COUNT:
        if query_scalar is None or ref is None:
            return _none
        return lambda: sql_count_token(query_scalar, ref)
    # hash on SQL (no cheap token) and none → TTL degrade; REQ-981 output gate covers correctness.
    return _none
