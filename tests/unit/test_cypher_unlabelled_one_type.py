# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An unlabelled node a property narrows to one type is still a node of the types it was read
over (#292, REQ-345): ``MATCH (n) WHERE n.city = 'oslo' RETURN n`` reads only shops, the one
type with a city, and the node is built from that arm's own columns, as a node of several types
is. It had been built as the shop table's row, from columns the arm does not carry, and failed.
Each case runs on DuckDB."""

# Requirements: REQ-345

import json

from tests.unit import test_cypher_backward_inferred_end as _hops

rows = _hops.rows  # the pets-and-shops DuckDB fixture


def _node(value) -> dict:
    return json.loads(value) if isinstance(value, str) else value


def test_a_node_a_property_narrows_to_one_type_is_returned(rows):
    ((node,),) = rows("MATCH (n) WHERE n.city = 'oslo' RETURN n")
    node = _node(node)
    assert node["label"] == "Shop"
    assert node["city"] == "oslo"


def test_its_property_beside_it_is_read_from_the_same_arm(rows):
    got = rows("MATCH (n) WHERE n.city IS NOT NULL RETURN n.city AS c, n")
    assert [(c, _node(n)["city"]) for c, n in got] == [("oslo", "oslo"), ("rome", "rome")]
