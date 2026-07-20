# Copyright (c) 2026 Kenneth Stott
# Canary: be981c7c-9b18-48ee-b32a-29b8b4921c97
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Property-based tests for query directives (REQ-277, REQ-279, REQ-281).

Directives steer routing, join strategy, caching, and redirect — merged across
precedence layers (server default < operation < field). A merge bug applies the
wrong layer's directive (wrong route, stale cache, cache when @noCache was set); a
parser bug silently drops a directive. Two contracts:

  * merge_directives is last-writer-wins per scalar, set-union for watermark fields,
    OR for no_cache, with the empty directive as identity — the precedence semantics
    callers rely on.
  * extract_directives_from_sql_comments round-trips: rendering a directive set to
    `-- @provisa key=value` comments and re-parsing yields the same set.
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from provisa.compiler.directives import (
    QueryDirectives,
    extract_directives_from_sql_comments,
    merge_directives,
)

# The scalar fields merge_directives copies with last-writer-wins semantics.
_SCALARS = [
    "route",
    "join_strategy",
    "reorder_enabled",
    "broadcast_size",
    "sink_topic",
    "sink_broker",
    "redirect_format",
    "redirect_threshold",
    "cache_ttl",
]


@st.composite
def _directives(draw) -> QueryDirectives:
    d = QueryDirectives()
    d.route = draw(st.sampled_from([None, "FEDERATED", "DIRECT"]))
    d.join_strategy = draw(st.sampled_from([None, "BROADCAST", "PARTITIONED"]))
    d.reorder_enabled = draw(st.sampled_from([None, False]))
    d.broadcast_size = draw(st.sampled_from([None, "1MB", "2GB"]))
    d.watermark_fields = set(
        draw(st.lists(st.sampled_from(["ts", "created", "updated"]), max_size=3, unique=True))
    )
    d.sink_topic = draw(st.sampled_from([None, "t1", "t2"]))
    d.sink_broker = draw(st.sampled_from([None, "h:9092"]))
    d.redirect_format = draw(st.sampled_from([None, "parquet", "csv", "arrow"]))
    d.redirect_threshold = draw(st.sampled_from([None, 0, 100]))
    d.cache_ttl = draw(st.sampled_from([None, 0, 60]))
    d.no_cache = draw(st.booleans())
    return d


def _fields_equal(x: QueryDirectives, y: QueryDirectives) -> None:
    for f in _SCALARS:
        assert getattr(x, f) == getattr(y, f), f
    assert x.watermark_fields == y.watermark_fields
    assert x.no_cache == y.no_cache


@settings(max_examples=300, deadline=None)
@given(a=_directives(), b=_directives())
def test_merge_is_last_writer_wins(a: QueryDirectives, b: QueryDirectives) -> None:
    """Each scalar takes the later source's value when set, else the earlier's;
    watermark fields union; no_cache is sticky (OR)."""
    m = merge_directives(a, b)
    for f in _SCALARS:
        bv, av = getattr(b, f), getattr(a, f)
        assert getattr(m, f) == (bv if bv is not None else av), f
    assert m.watermark_fields == (a.watermark_fields | b.watermark_fields)
    assert m.no_cache == (a.no_cache or b.no_cache)


@settings(max_examples=300, deadline=None)
@given(a=_directives())
def test_empty_directive_is_merge_identity(a: QueryDirectives) -> None:
    """The empty directive is a no-op on either side, and merging a single source
    reproduces it — so a missing precedence layer never perturbs the result."""
    empty = QueryDirectives()
    _fields_equal(merge_directives(a), a)
    _fields_equal(merge_directives(empty, a), a)
    _fields_equal(merge_directives(a, empty), a)


def _render(d: QueryDirectives) -> str:
    """Render a directive set to a `-- @provisa key=value` SQL comment (values are
    the \\S+ tokens the parser accepts)."""
    kv: list[str] = []
    if d.route:
        kv.append(f"route={d.route.lower()}")
    if d.join_strategy:
        kv.append(f"join={d.join_strategy.lower()}")
    if d.reorder_enabled is False:
        kv.append("reorder=off")
    if d.broadcast_size:
        kv.append(f"broadcast_size={d.broadcast_size}")
    for w in sorted(d.watermark_fields):
        kv.append(f"watermark={w}")
    if d.sink_topic:
        kv.append(f"sink={d.sink_topic}")
    if d.sink_broker:
        kv.append(f"broker={d.sink_broker}")
    if d.redirect_format:
        kv.append(f"redirect_format={d.redirect_format}")
    if d.redirect_threshold is not None:
        kv.append(f"redirect_threshold={d.redirect_threshold}")
    if d.cache_ttl is not None:
        kv.append(f"cache_ttl={d.cache_ttl}")
    if d.no_cache:
        kv.append("no_cache=true")
    return "SELECT 1 -- @provisa " + " ".join(kv)


@settings(max_examples=300, deadline=None)
@given(d=_directives())
def test_sql_comment_extraction_round_trips(d: QueryDirectives) -> None:
    """Rendering a directive set to `-- @provisa` comments and re-parsing recovers
    every field the SQL-comment form supports — no directive silently dropped."""
    parsed = extract_directives_from_sql_comments(_render(d))
    _fields_equal(parsed, d)
