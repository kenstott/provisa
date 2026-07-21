# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1174: per-role query-complexity limits (depth / nodes) measured at the IR-compile boundary."""

from __future__ import annotations

import pytest
from graphql import parse

from provisa.compiler.limits import (
    QueryLimitError,
    enforce_limits,
    measure_query,
    role_query_limits,
)


def test_depth_and_node_count_basic():
    doc = parse("{ orders { id customer { id name } } }")
    depth, nodes = measure_query(doc)
    assert depth == 3  # orders -> customer -> {id,name}
    assert nodes == 5  # orders, id, customer, id, name


def test_typename_is_free():
    doc = parse("{ orders { __typename id } }")
    depth, nodes = measure_query(doc)
    assert (depth, nodes) == (2, 2)  # __typename not counted


def test_inline_fragment_does_not_add_depth():
    doc = parse("{ node { ... on Order { id } } }")
    depth, _ = measure_query(doc)
    assert depth == 2  # node -> id (inline fragment shares parent depth)


def test_named_fragment_depth_is_resolved():
    # A depth attack hidden behind a named fragment must still count.
    doc = parse("{ a { ...F } } fragment F on T { b { c { d } } }")
    depth, nodes = measure_query(doc)
    assert depth == 4  # a -> b -> c -> d
    assert nodes == 4


def test_self_referential_fragment_terminates():
    # Cycle guard: a fragment that spreads itself must not recurse forever.
    doc = parse("{ a { ...F } } fragment F on T { b { ...F } }")
    depth, _ = measure_query(doc)  # must return, not hang
    assert depth >= 2


def test_enforce_depth_raises_over_limit():
    doc = parse("{ a { b { c { d } } } }")  # depth 4
    with pytest.raises(QueryLimitError) as ei:
        enforce_limits(doc, max_depth=3)
    assert ei.value.dimension == "depth" and ei.value.limit == 3 and ei.value.actual == 4


def test_enforce_nodes_raises_over_limit():
    doc = parse("{ a b c d e }")  # 5 nodes
    with pytest.raises(QueryLimitError) as ei:
        enforce_limits(doc, max_nodes=4)
    assert ei.value.dimension == "nodes"


def test_enforce_passes_within_limits():
    doc = parse("{ a { b } }")  # depth 2, nodes 2
    enforce_limits(doc, max_depth=5, max_nodes=10)  # no raise


def test_enforce_noop_when_no_limits():
    doc = parse("{ a { b { c { d } } } }")
    enforce_limits(doc)  # both None → never raises regardless of size
    enforce_limits(doc, max_depth=None, max_nodes=None)


@pytest.mark.parametrize(
    "role, expected",
    [
        ({"rate_limit": {"max_query_depth": 5, "max_query_nodes": 100, "max_query_time_ms": 3000}}, (5, 100, 3000)),
        ({"rate_limit": {"requests_per_second": 10}}, (None, None, None)),
        ({}, (None, None, None)),
        ({"rate_limit": None}, (None, None, None)),
    ],
)
def test_role_query_limits_extraction(role, expected):
    assert role_query_limits(role) == expected


def test_role_query_limits_from_model():
    from provisa.core.models import RoleRateLimit

    rl = RoleRateLimit(max_query_depth=7, max_query_nodes=50, max_query_time_ms=1000)

    class _R:
        rate_limit = rl

    assert role_query_limits(_R()) == (7, 50, 1000)
