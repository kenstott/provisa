# Copyright (c) 2026 Kenneth Stott
# Canary: 3a298b90-6bc5-4e44-99a9-62363b171c99
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

# REQ-1934: a profiler's result relations are appended by its own runs, on its own schedule, and
# read in place: there is no upstream to probe. REQ-1443 (amended 2026-10-12): so are a data-quality
# checker's, by its scans every cache_ttl seconds.
_DIRECT_WRITE_TYPES = frozenset({"data_profiler", "soda", "great_expectations"})

# Mail sources: the provider says whether a mailbox changed (Gmail's history id); there is no
# cursor to watermark by and no count that sees an edit, so only hash | none. The token comes
# from the source's own adapter (``loader_probe``).
_MAIL_TYPES = frozenset({"google_workspace", "microsoft_365"})
# REQ-1996: a Logs source's files say whether they changed (their sizes and last writes); its
# loader gives the token, as a mail source's does.
_LOADER_PROBED = _MAIL_TYPES | {"logs"}

_ALL = frozenset({WATERMARK, HASH, COUNT, NONE})


#: The source kinds whose loader is asked, as a whole, whether the source changed (its
#: ``freshness_probe``, which :func:`build_probe` uses under the hash probe type), so that a
#: ``probe`` / ``ttl_probe`` signal set on the SOURCE means something for tables that have no
#: probe query of their own. The mail kinds: Gmail's history id, Microsoft Graph's delta links;
#: and a Logs source's files.
LOADER_PROBE_TYPES: frozenset[str] = _LOADER_PROBED


def probe_capabilities(source_type: str) -> frozenset[str]:
    """The probe_types a source of ``source_type`` supports (REQ-982). SQL/engine-scannable sources
    (the open-ended default) support all four; HTTP APIs all four (watermark/count conditional on the
    API surface); files hash|none; streaming/push sources, and sources that write their own rows (a
    profiler, a data-quality checker), none (not on the probe axis)."""
    if source_type in _STREAMING_TYPES or source_type in _DIRECT_WRITE_TYPES:
        return frozenset()
    if source_type in _FILE_TYPES or source_type in _LOADER_PROBED:
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
    if (
        source_type in _FILE_TYPES
        or source_type in _HTTP_API_TYPES
        or source_type in _LOADER_PROBED
    ):
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


async def glob_token(source_path: str, file_glob: str) -> str | None:
    """The files-glob change token (REQ-788): the matched files' count, latest mtime and total
    size. None when the glob cannot be stat'd this call (→ TTL degrade)."""
    import asyncio

    from provisa.file_source.files_glob import glob_freshness_token, stat_matched_files

    try:
        stats = await asyncio.to_thread(stat_matched_files, source_path, file_glob)
    except OSError:
        return None
    return glob_freshness_token(stats)


Transport = Callable[[], Awaitable["str | None"]]


def can_probe(
    probe_type: str,
    *,
    watermark_column: str | None = None,
    sentinel_path: str | None = None,
    source_path: str | None = None,
    file_glob: str | None = None,
    loader_probe: bool = False,
) -> bool:
    """Whether :func:`build_probe` has a transport that asks the source for a token, given a
    SQL scalar runner and table ref for the types that need them. False is the cases
    ``build_probe`` answers with no token every time: ``none``, and ``hash`` with nothing to
    read a token from. The two are held together by tests/unit/test_probes.py."""
    if file_glob and source_path:
        return True
    if probe_type == WATERMARK:
        return watermark_column is not None
    if probe_type == COUNT:
        return True
    return probe_type == HASH and (bool(sentinel_path) or loader_probe)


def loader_probe(loaders: dict[str, Any], source: Any, table: Any) -> "Transport | None":
    """The token transport ``source``'s own adapter gives for ``table``: an adapter loader may
    carry ``freshness_probe(source, table)``, answering the transport, or None for a table it
    has no cheap token for. None also for a source whose adapter carries none."""
    make = getattr(loaders.get(source.type), "freshness_probe", None)
    return make(source, table) if make is not None else None


def source_transport(
    state: Any, loaders: dict[str, Any], source: Any, table: Any
) -> "Transport | None":
    """The token transport for ``table`` beyond a SQL scan: what its source's adapter gives, or,
    for a table dbt builds, dbt's run results (REQ-2228, REQ-858)."""
    from provisa.dbt.store import model_probe

    return loader_probe(loaders, source, table) or model_probe(state, source, table)


def build_probe(
    probe_type: str,
    *,
    query_scalar: QueryScalar | None = None,
    ref: str | None = None,
    watermark_column: str | None = None,
    sentinel_path: str | None = None,
    source_path: str | None = None,
    file_glob: str | None = None,
    loader_probe: Transport | None = None,
) -> Transport:
    """Build the ``freshness_token`` transport for a node from its ``probe_type`` (REQ-982).

    watermark/count over a SQL-scannable source are wired here; ``hash`` on a SQL source has no cheap
    token (a full-scan checksum is the fetch itself), and ``none`` never probes — both return a
    None-token transport that degrades the node to its TTL cadence, where the REQ-981 output hash gate
    still suppresses an unchanged ripple.

    REQ-1148: a ``hash`` probe with a configured ``sentinel_path`` reads a zero-byte SENTINEL marker
    (file/ftp/sftp/http) instead of the data — the token is exists+mtime+size or ETag/Last-Modified.
    This lights the previously-dark ``hash``-on-any-source case without touching the gate machinery."""

    async def _none() -> str | None:
        return None

    if file_glob and source_path:  # REQ-788: a files-glob table probes its matched files
        return lambda: glob_token(source_path, file_glob)

    if probe_type == WATERMARK:
        if query_scalar is None or ref is None or watermark_column is None:
            return _none  # not wired for this source → degrade
        return lambda: sql_watermark_token(query_scalar, ref, watermark_column)
    if probe_type == COUNT:
        if query_scalar is None or ref is None:
            return _none
        return lambda: sql_count_token(query_scalar, ref)
    if probe_type == HASH and sentinel_path:
        from provisa.events.sentinel_probe import build_sentinel_probe

        return build_sentinel_probe(sentinel_path)
    if probe_type == HASH and loader_probe is not None:
        return loader_probe  # the source's own adapter asks its provider (a mailbox's history id)
    # hash with no sentinel (no cheap token) and none → TTL degrade; REQ-981 output gate covers it.
    return _none
