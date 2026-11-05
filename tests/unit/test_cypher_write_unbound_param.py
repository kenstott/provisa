# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1e4b92-5d3a-4f08-b6e1-9a2f0c8d7e31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A write naming a parameter the request does not supply is refused by name (REQ-818), before
its counters are read from the value (REQ-670)."""

from __future__ import annotations

import pytest

from provisa.cypher.counters import set_values
from provisa.cypher.params import CypherParamError
from provisa.cypher.write_translator import CypherParam

pytestmark = [pytest.mark.unit]


def test_a_supplied_parameter_is_its_value():
    assert set_values([1, CypherParam("region")], {"region": "east"}) == [1, "east"]


def test_a_parameter_not_supplied_is_refused_by_name():
    with pytest.raises(CypherParamError, match=r"Unbound Cypher parameters: \['region'\]"):
        set_values([CypherParam("id"), CypherParam("region")], {"id": 71})
