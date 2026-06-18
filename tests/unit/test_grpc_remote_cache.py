# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-327: gRPC-remote result caching — column type mapping for the PG cache table."""

from __future__ import annotations

import pytest

from provisa.api.data.endpoint import _grpc_cache_type


@pytest.mark.parametrize(
    "sql_type,expected",
    [
        ("VARCHAR", "string"),
        ("TEXT", "string"),
        ("BIGINT", "integer"),
        ("INTEGER", "integer"),
        ("INT", "integer"),
        ("DOUBLE", "number"),
        ("REAL", "number"),
        ("DECIMAL(10,2)", "number"),
        ("NUMERIC", "number"),
        ("BOOLEAN", "boolean"),
        ("TIMESTAMP", "string"),  # unmapped → varchar-backed string
        ("", "string"),
    ],
)
def test_grpc_cache_type_maps_to_cache_vocabulary(sql_type, expected):
    assert _grpc_cache_type(sql_type) == expected
