# Copyright (c) 2026 Kenneth Stott
# Canary: 3c0ecee5-252d-438f-be9f-450dce8869fb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GraphQL directive extraction and SQL comment counterpart parsing.

Supported GraphQL directives (operation-level unless noted):

    @route(engine: RouteEngine!)                    — FEDERATED | DIRECT
    @join(strategy: JoinStrategy!)                  — BROADCAST | PARTITIONED
    @reorder(enabled: Boolean!)                     — false = disable join reordering
    @broadcastSize(size: String!)                   — max broadcast table size for the engine
    @watermark                                      — field-level; marks watermark column
    @sink(topic: String!, broker: String)           — Kafka sink redirect
    @redirect(format: String, threshold: Int)       — redirect large results to object store
    @cached(ttl: Int)                               — opt this request INTO the response cache;
                                                      ttl (optional) = seconds an entry lives

Equivalent SQL comment syntax (``-- @provisa key=value``):

    -- @provisa route=federated | route=direct
    -- @provisa join=broadcast | join=partitioned
    -- @provisa reorder=off
    -- @provisa broadcast_size=1GB
    -- @provisa watermark=column_name
    -- @provisa sink=topic_name
    -- @provisa broker=broker_host:port
    -- @provisa redirect_format=parquet | csv | arrow
    -- @provisa redirect_threshold=10000
    -- @provisa cache=true                  (opt into the response cache)
    -- @provisa cache_ttl=N                 (opt in, entry lives N seconds)

The response cache is OPT-IN per request (REQ-544, amended 2026-09-30): without ``@cached`` / a
``cache``/``cache_ttl`` comment a request neither reads nor writes it. Opting in only trades recency
for speed above the operator's floor — it never makes a read fresher than the operator's settings
(landing/replica freshness, row_materialize, load_protected snapshots), which apply unchanged.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from graphql import DocumentNode
from graphql.language.ast import (
    BooleanValueNode,
    EnumValueNode,
    FieldNode,
    FloatValueNode,
    IntValueNode,
    NullValueNode,
    OperationDefinitionNode,
    SelectionSetNode,
    StringValueNode,
)

from provisa.core.operator_floor import OperatorFloorError

# Requirements: REQ-137, REQ-176, REQ-277, REQ-278, REQ-279, REQ-281, REQ-283


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class QueryDirectives:  # REQ-277, REQ-279, REQ-281
    """Extracted directive values for a single GraphQL operation."""

    # @route
    route: str | None = None  # "FEDERATED" | "DIRECT" | None

    # @join
    join_strategy: str | None = None  # "BROADCAST" | "PARTITIONED" | None

    # @reorder
    reorder_enabled: bool | None = None  # False = disable

    # @broadcastSize
    broadcast_size: str | None = None

    # @watermark (field-level) — set of field names marked with @watermark
    watermark_fields: set[str] = field(default_factory=set)

    # @sink
    sink_topic: str | None = None
    sink_broker: str | None = None

    # @redirect
    redirect_format: str | None = None  # "parquet" | "csv" | "arrow"
    redirect_threshold: int | None = None

    # @cached / -- @provisa cache=true | cache_ttl=N — the request's response-cache opt-in
    cache_opt_in: bool = False  # False = no cache read, no cache write (REQ-544, amended)
    cache_ttl: int | None = None  # seconds the entry lives; None = the operator-resolved TTL

    # -----------------------------------------------------------------------
    # Convenience helpers
    # -----------------------------------------------------------------------

    @property
    def steward_hint(self) -> str | None:
        """Translate @route to internal steward_hint string."""
        if self.route == "FEDERATED":
            return "engine"
        if self.route == "DIRECT":
            return "direct"
        return None

    def to_session_props(self) -> dict[str, str]:
        """Convert join/reorder/broadcastSize directives to the engine session props."""
        props: dict[str, str] = {}
        if self.join_strategy == "BROADCAST":
            props["join_distribution_type"] = "BROADCAST"
        elif self.join_strategy == "PARTITIONED":
            props["join_distribution_type"] = "PARTITIONED"
        if self.reorder_enabled is False:
            props["join_reordering_strategy"] = "NONE"
        if self.broadcast_size:
            props["join_max_broadcast_table_size"] = self.broadcast_size
        return props

    @property
    def watermark_column(self) -> str | None:
        """Return the single watermark field name, or None."""
        if self.watermark_fields:
            return next(iter(self.watermark_fields))
        return None


class SessionPropFloorViolation(OperatorFloorError):
    """A request hint overrides an engine session property the operator set (REQ-281, amended
    2026-09-30). The operator's source federation hints are the floor for the platform's engine."""

    def __init__(self, prop: str, operator_value: str, requested: str) -> None:
        super().__init__(
            f"{prop}={requested} would override the operator's floor: the operator set {prop}="
            f"{operator_value} on a source this query reads. Remove the hint."
        )


def merge_session_props(  # REQ-281, REQ-030
    operator: dict[str, str], *requested: dict[str, str]
) -> dict[str, str]:
    """The engine session props for one query: the operator's source hints, then the request's.

    A request may set a property the operator left open; a request value that differs from one
    the operator set raises :class:`SessionPropFloorViolation` — never silently applied, never
    silently dropped."""
    merged = dict(operator)
    for props in requested:
        for key, value in props.items():
            if key in operator and operator[key] != value:
                raise SessionPropFloorViolation(key, operator[key], value)
            merged[key] = value
    return merged


def translate_federation_hints(hints: dict[str, str]) -> dict[str, str]:  # REQ-278, REQ-281
    """REQ-281: map Provisa-branded source federation hints to the engine session props.

    Source-level ``federation_hints`` use the same vocabulary as the @provisa query
    directives so admins never write the engine-specific names:
      - ``join``: ``broadcast`` | ``partitioned`` → ``join_distribution_type``
      - ``reorder``: ``none``/``false`` → ``join_reordering_strategy=NONE``;
        ``auto``/``true`` → ``AUTOMATIC``
      - ``broadcast_size``: ``<size>`` → ``join_max_broadcast_table_size``

    Unrecognized keys pass through unchanged (the engine validates them) so configs still using
    raw the engine session-prop names keep working during the transition.
    """
    out: dict[str, str] = {}
    for key, value in hints.items():
        k = key.lower()
        v = str(value)
        if k == "join":
            vu = v.upper()
            if vu in ("BROADCAST", "PARTITIONED"):
                out["join_distribution_type"] = vu
        elif k == "reorder":
            vl = v.lower()
            if vl in ("none", "false", "off", "no"):
                out["join_reordering_strategy"] = "NONE"
            elif vl in ("auto", "automatic", "true", "on", "yes"):
                out["join_reordering_strategy"] = "AUTOMATIC"
        elif k == "broadcast_size":
            out["join_max_broadcast_table_size"] = v
        else:
            out[key] = value  # passthrough (raw the engine prop) — transitional
    return out


# ---------------------------------------------------------------------------
# AST value extraction helper
# ---------------------------------------------------------------------------


def _arg_value(node: Any) -> Any:
    if isinstance(node, StringValueNode):
        return node.value
    if isinstance(node, IntValueNode):
        return int(node.value)
    if isinstance(node, FloatValueNode):
        return float(node.value)
    if isinstance(node, BooleanValueNode):
        return node.value
    if isinstance(node, EnumValueNode):
        return node.value  # already a string like "FEDERATED"
    if isinstance(node, NullValueNode):
        return None
    return None


# ---------------------------------------------------------------------------
# Watermark field scanner (recursive)
# ---------------------------------------------------------------------------


def _scan_watermark_fields(selection_set: SelectionSetNode) -> set[str]:
    found: set[str] = set()
    for sel in selection_set.selections:
        if not isinstance(sel, FieldNode):
            continue
        for d in sel.directives:
            if d.name.value == "watermark":
                found.add(sel.name.value)
        if sel.selection_set:
            found |= _scan_watermark_fields(sel.selection_set)
    return found


# ---------------------------------------------------------------------------
# GraphQL AST extraction
# ---------------------------------------------------------------------------


def extract_directives(document: DocumentNode) -> QueryDirectives:  # REQ-277, REQ-279, REQ-283
    """Extract all provisa directives from a parsed GraphQL ``DocumentNode``.

    Reads operation-level directives (``@route``, ``@join``, ``@reorder``,
    ``@broadcastSize``, ``@sink``) and recursively scans fields for
    ``@watermark``.
    """
    result = QueryDirectives()

    for defn in document.definitions:
        if not isinstance(defn, OperationDefinitionNode):
            continue

        # Operation-level directives
        for directive in defn.directives:
            name = directive.name.value
            args = {a.name.value: _arg_value(a.value) for a in directive.arguments}

            if name == "route":
                result.route = str(args.get("engine", "")).upper() or None
            elif name == "join":
                result.join_strategy = str(args.get("strategy", "")).upper() or None
            elif name == "reorder":
                val = args.get("enabled")
                if val is not None:
                    result.reorder_enabled = bool(val)
            elif name == "broadcastSize":
                result.broadcast_size = str(args.get("size", "")) or None
            elif name == "sink":
                result.sink_topic = str(args.get("topic", "")) or None
                broker = args.get("broker")
                if broker:
                    result.sink_broker = str(broker)
            elif name == "redirect":
                fmt = args.get("format")
                if fmt:
                    result.redirect_format = str(fmt)
                threshold = args.get("threshold")
                if threshold is not None:
                    assert isinstance(threshold, (int, float, str))
                    result.redirect_threshold = int(threshold)
            elif name == "cached":
                result.cache_opt_in = True
                ttl = args.get("ttl")
                if ttl is not None:
                    assert isinstance(ttl, (int, float, str))
                    result.cache_ttl = int(ttl)

        # Field-level @watermark scan
        if defn.selection_set:
            result.watermark_fields |= _scan_watermark_fields(defn.selection_set)

    return result


# ---------------------------------------------------------------------------
# SQL comment counterpart parsing
# ---------------------------------------------------------------------------

_COMMENT_RE = re.compile(r"--\s*@provisa\s+(.*)")
_CYPHER_COMMENT_RE = re.compile(r"//\s*@provisa\s+(.*)")
_KV_RE = re.compile(r"(\w+)=(\S+)")


def extract_directives_from_cypher_comments(cypher: str) -> QueryDirectives:  # REQ-544
    """``// @provisa key=value`` comment lines on a Cypher statement — the same keys as the SQL
    form (:func:`extract_directives_from_sql_comments`), in Cypher's comment syntax."""
    return _directives_from_comments(cypher, _CYPHER_COMMENT_RE)


def extract_directives_from_sql_comments(sql: str) -> QueryDirectives:  # REQ-279, REQ-281
    """Parse ``-- @provisa key=value`` comment lines from a SQL string.

    This is the SQL counterpart to :func:`extract_directives`.  Both produce
    a :class:`QueryDirectives` so callers handle them identically.

    Supported keys:
        route=federated | route=direct
        join=broadcast | join=partitioned
        reorder=off
        broadcast_size=<value>
        watermark=<column_name>
        sink=<topic>
        broker=<host:port>
    """
    return _directives_from_comments(sql, _COMMENT_RE)


def _directives_from_comments(text: str, comment_re: re.Pattern[str]) -> QueryDirectives:
    result = QueryDirectives()

    for line in text.splitlines():
        m = comment_re.search(line)
        if not m:
            continue
        for key, value in _KV_RE.findall(m.group(1)):
            key = key.lower()
            value_lower = value.lower()
            if key == "route":
                result.route = (
                    "FEDERATED"
                    if value_lower == "federated"
                    else "DIRECT"
                    if value_lower == "direct"
                    else None
                )
            elif key == "join":
                result.join_strategy = (
                    "BROADCAST"
                    if value_lower == "broadcast"
                    else "PARTITIONED"
                    if value_lower == "partitioned"
                    else None
                )
            elif key == "reorder" and value_lower == "off":
                result.reorder_enabled = False
            elif key == "broadcast_size":
                result.broadcast_size = value
            elif key == "watermark":
                result.watermark_fields.add(value)
            elif key == "sink":
                result.sink_topic = value
            elif key == "broker":
                result.sink_broker = value
            elif key == "redirect_format":
                result.redirect_format = value
            elif key == "redirect_threshold":
                try:
                    result.redirect_threshold = int(value)
                except ValueError:
                    pass
            elif key == "cache" and value_lower in ("true", "1", "yes"):
                result.cache_opt_in = True
            elif key == "cache_ttl":
                result.cache_opt_in = True
                result.cache_ttl = int(value)  # a malformed TTL fails the request, never ignored

    return result


def merge_directives(*sources: QueryDirectives) -> QueryDirectives:  # REQ-277, REQ-278
    """Merge multiple :class:`QueryDirectives`, later sources taking precedence."""
    result = QueryDirectives()
    for src in sources:
        if src.route is not None:
            result.route = src.route
        if src.join_strategy is not None:
            result.join_strategy = src.join_strategy
        if src.reorder_enabled is not None:
            result.reorder_enabled = src.reorder_enabled
        if src.broadcast_size is not None:
            result.broadcast_size = src.broadcast_size
        if src.sink_topic is not None:
            result.sink_topic = src.sink_topic
        if src.sink_broker is not None:
            result.sink_broker = src.sink_broker
        if src.redirect_format is not None:
            result.redirect_format = src.redirect_format
        if src.redirect_threshold is not None:
            result.redirect_threshold = src.redirect_threshold
        if src.cache_ttl is not None:
            result.cache_ttl = src.cache_ttl
        if src.cache_opt_in:
            result.cache_opt_in = True
        result.watermark_fields |= src.watermark_fields
    return result


# ---------------------------------------------------------------------------
# The response-cache opt-in a request carries (REQ-544, amended 2026-09-30)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CacheHint:  # REQ-544
    """A request's response-cache opt-in, carried onto its compiled plan by the one pipeline."""

    opt_in: bool
    ttl: int | None


NO_CACHE_HINT = CacheHint(opt_in=False, ttl=None)

_GRPC_CACHE_KEY = "x-provisa-cache"
_GRPC_CACHE_TTL_KEY = "x-provisa-cache-ttl"


def cache_hint_for(language: str, text: str) -> CacheHint:  # REQ-544
    """The opt-in written in a request's own query text: GraphQL ``@cached(ttl)`` (and its
    `-- @provisa` comment lines, merged as the HTTP endpoint merges them), Cypher ``// @provisa cache=true|cache_ttl=N``,
    SQL ``-- @provisa cache=true|cache_ttl=N``. One parser per language, shared by every
    transport that receives that language."""
    if language == "graphql":
        from graphql import parse

        d = merge_directives(
            extract_directives_from_sql_comments(text), extract_directives(parse(text))
        )
    elif language == "cypher":
        d = extract_directives_from_cypher_comments(text)
    elif language == "sql":
        d = extract_directives_from_sql_comments(text)
    else:
        raise ValueError(f"no response-cache hint syntax for language {language!r}")
    return CacheHint(opt_in=d.cache_opt_in, ttl=d.cache_ttl)


def cache_hint_from_grpc_metadata(metadata: Any) -> CacheHint:  # REQ-544
    """gRPC's typed requests carry no query text, so the opt-in rides in call metadata, like the
    role (``x-provisa-role``): ``x-provisa-cache: true`` and/or ``x-provisa-cache-ttl: N``."""
    values = {str(k).lower(): str(v) for k, v in (metadata or ())}
    ttl_raw = values.get(_GRPC_CACHE_TTL_KEY)
    ttl = int(ttl_raw) if ttl_raw is not None else None  # malformed fails the call, never ignored
    opt_in = ttl is not None or values.get(_GRPC_CACHE_KEY, "").lower() in ("true", "1", "yes")
    return CacheHint(opt_in=opt_in, ttl=ttl)
