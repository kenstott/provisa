# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The front-end answers comparison (scripts/graphql_golden.front_answers): a nested level the read
gives no order_by is in no order on either side, so its lists are compared as multisets; a level
the read orders keeps its order. Without this the comparison held only by the accident of an
engine's aggregation order (DuckDB 1.5.5's), which DuckDB 2.0 does not repeat."""

# Requirements: REQ-863, REQ-2135

from __future__ import annotations

import sys
from pathlib import Path

from graphql import parse

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import graphql_golden  # noqa: E402

_READ = parse(
    """
    query {
      customers(where: {id: {eq: 1}}) {
        id
        orders { id lines(order_by: {qty: desc}) { qty product { name tags { t } } } }
        tags: labels { t }
      }
      other { id kids { id } }
    }
    """
)


def test_the_unordered_levels_are_those_without_an_order_by():
    assert graphql_golden._unordered_levels(_READ, "customers") == {  # noqa: SLF001
        ("orders",),
        ("orders", "lines", "product"),
        ("orders", "lines", "product", "tags"),
        ("tags",),
    }


def test_an_unordered_level_compares_as_a_multiset():
    unordered = graphql_golden._unordered_levels(_READ, "customers")  # noqa: SLF001
    a = {"id": 1, "orders": [{"id": 2, "lines": []}, {"id": 1, "lines": []}]}
    b = {"id": 1, "orders": [{"id": 1, "lines": []}, {"id": 2, "lines": []}]}
    assert graphql_golden._in_read_order(a, unordered) == graphql_golden._in_read_order(  # noqa: SLF001
        b, unordered
    )


def test_an_ordered_level_keeps_its_order():
    unordered = graphql_golden._unordered_levels(_READ, "customers")  # noqa: SLF001
    a = {"orders": [{"id": 1, "lines": [{"qty": 2}, {"qty": 1}]}]}
    b = {"orders": [{"id": 1, "lines": [{"qty": 1}, {"qty": 2}]}]}
    assert graphql_golden._in_read_order(a, unordered) != graphql_golden._in_read_order(  # noqa: SLF001
        b, unordered
    )


def test_a_level_below_an_ordered_one_is_still_unordered():
    unordered = graphql_golden._unordered_levels(_READ, "customers")  # noqa: SLF001
    a = {"orders": [{"lines": [{"qty": 1, "product": {"tags": [{"t": "b"}, {"t": "a"}]}}]}]}
    b = {"orders": [{"lines": [{"qty": 1, "product": {"tags": [{"t": "a"}, {"t": "b"}]}}]}]}
    assert graphql_golden._in_read_order(a, unordered) == graphql_golden._in_read_order(  # noqa: SLF001
        b, unordered
    )
