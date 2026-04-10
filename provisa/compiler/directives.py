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
    @broadcastSize(size: String!)                   — max broadcast table size for Trino
    @watermark                                      — field-level; marks watermark column
    @sink(topic: String!, broker: String)           — Kafka sink redirect
    @redirect(format: String, threshold: Int)       — redirect large results to object store

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
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

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


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class QueryDirectives:
    """Extracted directive values for a single GraphQL operation."""

    # @route
    route: str | None = None           # "FEDERATED" | "DIRECT" | None

    # @join
    join_strategy: str | None = None   # "BROADCAST" | "PARTITIONED" | None

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
    redirect_format: str | None = None    # "parquet" | "csv" | "arrow"
    redirect_threshold: int | None = None

    # -----------------------------------------------------------------------
    # Convenience helpers
    # -----------------------------------------------------------------------

    @property
    def steward_hint(self) -> str | None:
        """Translate @route to internal steward_hint string."""
        if self.route == "FEDERATED":
            return "trino"
        if self.route == "DIRECT":
            return "direct"
        return None

    def to_session_props(self) -> dict[str, str]:
        """Convert join/reorder/broadcastSize directives to Trino session props."""
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


# ---------------------------------------------------------------------------
# AST value extraction helper
# ---------------------------------------------------------------------------

def _arg_value(node: object) -> object:
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

def extract_directives(document: DocumentNode) -> QueryDirectives:
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
                    result.redirect_threshold = int(threshold)

        # Field-level @watermark scan
        if defn.selection_set:
            result.watermark_fields |= _scan_watermark_fields(defn.selection_set)

    return result


# ---------------------------------------------------------------------------
# SQL comment counterpart parsing
# ---------------------------------------------------------------------------

_COMMENT_RE = re.compile(r"--\s*@provisa\s+(.*)")
_KV_RE = re.compile(r"(\w+)=(\S+)")


def extract_directives_from_sql_comments(sql: str) -> QueryDirectives:
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
    result = QueryDirectives()

    for line in sql.splitlines():
        m = _COMMENT_RE.search(line)
        if not m:
            continue
        for key, value in _KV_RE.findall(m.group(1)):
            key = key.lower()
            value_lower = value.lower()
            if key == "route":
                result.route = "FEDERATED" if value_lower == "federated" else "DIRECT" if value_lower == "direct" else None
            elif key == "join":
                result.join_strategy = "BROADCAST" if value_lower == "broadcast" else "PARTITIONED" if value_lower == "partitioned" else None
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

    return result


def merge_directives(*sources: QueryDirectives) -> QueryDirectives:
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
        result.watermark_fields |= src.watermark_fields
    return result
