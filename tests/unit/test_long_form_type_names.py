# Copyright (c) 2026 Kenneth Stott
# Canary: f2da2d49-4340-444b-9dd9-4f9528815d04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column type in the SQL standard's long form (``character varying``, ``character``,
``timestamp without time zone``) is mapped by every engine-type map a registered column goes
through -- GraphQL, gRPC proto and Arrow -- as its short form is. A ServiceNow table's columns
arrive in these spellings; each map met one in turn on its own CI round."""

from __future__ import annotations

import pyarrow as pa
import pytest
from graphql import GraphQLString

from provisa.compiler.type_map import DateTime, column_type_to_graphql
from provisa.core.catalog_types import _physical_type_to_arrow
from provisa.grpc.proto_gen import _physical_to_proto

pytestmark = pytest.mark.unit

# The names a ServiceNow table's columns arrive by (the adapter's ListingTables.postgresName),
# and the SQL standard's other long forms.
LONG_FORMS = {
    "character varying": ("string", pa.utf8(), GraphQLString),
    "character varying(40)": ("string", pa.utf8(), GraphQLString),
    "character": ("string", pa.utf8(), GraphQLString),
    "character(3)": ("string", pa.utf8(), GraphQLString),
    "timestamp without time zone": ("google.protobuf.Timestamp", pa.timestamp("us"), DateTime),
    "double precision": ("double", pa.float64(), None),
    "integer": ("int32", pa.int32(), None),
    "bigint": ("int64", pa.int64(), None),
    "boolean": ("bool", pa.bool_(), None),
    "timestamp": ("google.protobuf.Timestamp", pa.timestamp("us"), DateTime),
    "date": ("string", pa.date32(), None),
    "time": ("string", pa.time64("us"), None),
}


@pytest.mark.parametrize("spelling", sorted(LONG_FORMS))
def test_every_engine_type_map_names_the_spelling(spelling):
    proto, arrow, graphql = LONG_FORMS[spelling]
    assert _physical_to_proto(spelling) == proto
    assert _physical_type_to_arrow(spelling) == arrow
    mapped = column_type_to_graphql(spelling)
    if graphql is not None:
        assert mapped is graphql


def test_a_type_no_map_or_the_vocabulary_names_is_text():
    # REQ-2171: every type maps onto the IR; one naming none of it is text.
    assert column_type_to_graphql("character wobbly") == column_type_to_graphql("text")
    assert _physical_to_proto("character wobbly") == _physical_to_proto("text")
