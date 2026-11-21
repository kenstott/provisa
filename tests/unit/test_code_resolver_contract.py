# Copyright (c) 2026 Kenneth Stott
# Canary: 5e0b7c93-2a41-4d86-b1f8-0c6e9d2a7f45
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a code resolver must be: a code @query taking the source and the target table as its two
table parameters, whose rows carry the key of each side. Anything else is refused with the reason.
Its matches are read as the call a view over it holds, and that view reads both tables."""

# Requirements: REQ-2249, REQ-2226

from __future__ import annotations

import pytest

from provisa.codesource import commands
from provisa.codesource.resolver import ResolverRefused, call_sql, check_resolver, is_resolver
from provisa.mv.view_inputs import call_table_refs


def _table(name: str) -> dict:
    return {"name": name, "type": "String", "arg_kind": "table_ref", "columns": []}


def _resolver(**more) -> dict:
    command = {
        "name": "match_stores",
        "impl_kind": commands.CODE_COMMAND,
        "kind": "query",
        "arguments": [_table("stores"), _table("regions")],
        "output_columns": [
            {"name": "store_id", "type": "Int"},
            {"name": "region_id", "type": "Int"},
            {"name": "score", "type": "Float"},
        ],
    }
    command.update(more)
    return command


def test_a_code_query_over_two_tables_returning_both_keys_is_a_resolver():
    assert is_resolver(_resolver())
    check_resolver(_resolver(), "store_id", "region_id")


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"impl_kind": "source_procedure"}, "not a function of a code source"),
        ({"kind": "mutation"}, "a resolver is a @query"),
        ({"arguments": [_table("stores")]}, "takes 1 table parameter"),
        (
            {
                "arguments": [
                    _table("stores"),
                    _table("regions"),
                    {"name": "radius", "type": "Float"},
                ]
            },
            "takes radius besides its two tables",
        ),
        ({"output_columns": []}, "returns no columns"),
    ],
)
def test_a_command_not_shaped_as_a_resolver_is_refused_with_the_reason(change, reason):
    assert not is_resolver(_resolver(**change))
    with pytest.raises(ResolverRefused, match=reason) as refused:
        check_resolver(_resolver(**change), "store_id", "region_id")
    assert refused.value.params["function"] == "match_stores"


@pytest.mark.parametrize(
    ("source_key", "target_key", "missing"),
    [
        ("shop_id", "region_id", "'shop_id'"),
        ("store_id", "zone_id", "'zone_id'"),
        ("shop_id", "zone_id", "'shop_id' or 'zone_id'"),
    ],
)
def test_a_resolver_whose_rows_lack_either_sides_key_is_refused_naming_it(
    source_key, target_key, missing
):
    with pytest.raises(ResolverRefused, match=f"its rows carry no {missing}"):
        check_resolver(_resolver(), source_key, target_key)


def test_the_matches_are_read_as_the_call_given_source_then_target():
    sql = call_sql(_resolver(), "retail.stores", "geo.regions")
    assert sql == "SELECT * FROM match_stores('retail.stores', 'geo.regions') AS m"
    assert call_table_refs(sql, {"match_stores": _resolver()}) == [
        ("retail", "stores"),
        ("geo", "regions"),
    ]
