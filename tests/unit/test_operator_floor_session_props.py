# Copyright (c) 2026 Kenneth Stott
# Canary: 4e8a2c19-7d5f-4b63-9a10-2f6c8e3d5b71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An engine session property the operator set on a source is the floor (REQ-281, amended
2026-09-30).

The operator's federation hints on a source (join distribution, join reordering, broadcast size)
protect the platform's engine. A request hint (``@join``/``@reorder``/``@broadcastSize``, or a
``/*+ ... */`` comment) may set a property the operator left open, but never override one the
operator set — that is rejected naming the property, not silently applied or ignored.
"""

# Requirements: REQ-281, REQ-030

from __future__ import annotations

import pytest

from provisa.compiler.directives import SessionPropFloorViolation, merge_session_props
from provisa.core.operator_floor import OperatorFloorError


def test_a_request_may_set_a_property_the_operator_left_open():
    merged = merge_session_props(
        {"join_distribution_type": "PARTITIONED"}, {"join_reordering_strategy": "NONE"}
    )
    assert merged == {
        "join_distribution_type": "PARTITIONED",
        "join_reordering_strategy": "NONE",
    }


def test_a_request_repeating_the_operators_value_is_accepted():
    merged = merge_session_props(
        {"join_distribution_type": "PARTITIONED"}, {"join_distribution_type": "PARTITIONED"}
    )
    assert merged == {"join_distribution_type": "PARTITIONED"}


@pytest.mark.parametrize("source", ["directive", "comment"])
def test_a_request_overriding_an_operator_property_is_rejected(source):
    operator = {"join_max_broadcast_table_size": "100MB"}
    request = {"join_max_broadcast_table_size": "50GB"}
    args = (request, {}) if source == "directive" else ({}, request)
    with pytest.raises(SessionPropFloorViolation) as exc:
        merge_session_props(operator, *args)
    assert "join_max_broadcast_table_size" in str(exc.value)
    assert "100MB" in str(exc.value)


def test_the_violation_is_an_operator_floor_error():
    assert issubclass(SessionPropFloorViolation, OperatorFloorError)
