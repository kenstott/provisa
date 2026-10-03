# Copyright (c) 2026 Kenneth Stott
# Canary: 1c7e4b92-6a35-4d18-b0f9-5e2a8d3c7f40
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A command carries one role list, and its GraphQL field applies what it is asked or refuses."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from provisa.api.data.mutations import _apply_action_filters
from provisa.api.errors import ApiError
from provisa.core.models import Function

_ROWS = [{"id": 1, "region": "east"}, {"id": 2, "region": "west"}, {"id": 3, "region": "east"}]


def test_a_command_configured_with_a_second_role_list_fails_naming_the_key():
    with pytest.raises(ValidationError, match="'writable_by' is not a command key"):
        Function.model_validate(
            {
                "name": "refund",
                "source_id": "s",
                "function_name": "refund",
                "returns": "",
                "visible_to": ["seller"],
                "writable_by": ["seller"],
            }
        )


def test_filters_it_knows_are_applied():
    got = _apply_action_filters(
        _ROWS, {"where": {"region": {"_eq": "east"}}, "order_by": ["id desc"], "limit": 1}
    )
    assert got == [{"id": 3, "region": "east"}]


@pytest.mark.parametrize(
    "args, named",
    [
        ({"where": {"region": {"eq": "east"}}}, "where operator 'eq'"),
        ({"where": {"nope": {"_eq": 1}}}, "'nope' is not a column"),
        ({"order_by": ["id sideways"]}, "order_by 'id sideways'"),
        ({"order_by": ["nope"]}, "'nope' is not a column"),
        ({"where": ["region"]}, "where takes an object"),
    ],
)
def test_a_filter_it_cannot_apply_is_refused_by_name(args, named):
    with pytest.raises(ApiError) as refused:
        _apply_action_filters(_ROWS, args, "s__orders_by")
    assert refused.value.status_code == 400
    assert named in str(refused.value.detail)
    assert str(refused.value.detail).startswith("s__orders_by:")
