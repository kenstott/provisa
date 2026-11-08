# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The name conversions a role's schema and context ask for every column are answered from a
memo (REQ-1882).

A schema rebuild names every column of every table for each role it builds, and the conversions
were 15% of a rebuild's CPU on the core lane's runner (run 42440880718, py-spy). They are pure
string functions, memoized as the module's other conversions are (test_graphql_plan_cache).
"""

# Requirements: REQ-1882

from __future__ import annotations

from provisa.compiler import naming, sql_types


def _clear() -> None:
    for fn in (
        naming._to_pascal_case,
        naming._to_camel_case,
        naming.apply_convention,
        naming._gql_name,
        sql_types.is_json_type,
        sql_types.is_integer_type,
    ):
        fn.cache_clear()


def test_a_repeated_name_is_converted_once(monkeypatch):
    _clear()
    splits = {"n": 0}
    real_split = naming.re.split

    def _counting_split(*args, **kwargs):
        splits["n"] += 1
        return real_split(*args, **kwargs)

    monkeypatch.setattr(naming.re, "split", _counting_split)
    assert naming.apply_gql_name("order_line_items") == "orderLineItems"
    assert naming.apply_convention("order_line_items", "apollo_graphql") == "orderLineItems"
    assert naming.to_pascal_case("order_line_items") == "OrderLineItems"
    first = splits["n"]
    assert first > 0
    for _ in range(50):
        assert naming.apply_gql_name("order_line_items") == "orderLineItems"
        assert naming.apply_convention("order_line_items", "apollo_graphql") == "orderLineItems"
        assert naming.to_pascal_case("order_line_items") == "OrderLineItems"
    assert splits["n"] == first, "a repeated name was converted again"


def test_the_gql_name_follows_the_convention_in_force(monkeypatch):
    """The memo is keyed by the convention: a change of convention is never answered from a name
    converted under the one before."""
    _clear()
    assert naming.apply_gql_name("order_items") == "orderItems"
    monkeypatch.setattr(naming, "_gql_convention", "hasura_graphql")
    assert naming.apply_gql_name("order_items") == "order_items"
    assert naming.apply_gql_name("orderItems", "apollo_graphql") == "orderItems"


def test_an_invalid_gql_name_is_still_made_valid():
    _clear()
    assert naming.apply_gql_name("1844 doses") == "_1844_doses"
    assert naming.apply_gql_name("1844 doses") == "_1844_doses"


def test_the_type_tests_answer_as_before():
    _clear()
    assert sql_types.is_json_type(" JSONB ") and sql_types.is_json_type("json")
    assert not sql_types.is_json_type("text") and not sql_types.is_json_type(None)
    assert sql_types.is_integer_type("BIGINT") and not sql_types.is_integer_type("decimal")
    assert not sql_types.is_integer_type(None)
