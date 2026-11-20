# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7e9a14-58b2-4f0d-9e61-b2a4d8c07f35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1929: proto_relation_field_name against the examples the UI's protoRelationFieldName
(provisa-ui relationshipQueries.ts) is pinned to, so a relationship's gRPC query link names the
field proto_gen declares."""

import json
from pathlib import Path

import pytest

from provisa.grpc.proto_gen import proto_relation_field_name

_EXAMPLES = json.loads(
    (Path(__file__).parents[1] / "fixtures" / "proto_relation_field_names.json").read_text()
)["examples"]


@pytest.mark.parametrize(("graphql_field", "proto_field"), _EXAMPLES)
def test_relationship_field_translates_as_the_ui_does(graphql_field: str, proto_field: str):
    assert proto_relation_field_name(graphql_field) == proto_field
