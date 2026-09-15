# Copyright (c) 2026 Kenneth Stott
# Canary: 8c4d2e6f-1a3b-4c5d-9e7f-2a4b6c8d0e1f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa.compiler.type_map.column_type_to_graphql."""

import pytest

from provisa.compiler.type_map import GraphQLString, column_type_to_graphql


# ── saphana's own SQL_TYPE_NAMEs (REQ-1753) ─────────────────────────────────────────────────────
def test_column_type_to_graphql_nvarchar():
    assert column_type_to_graphql("NVARCHAR") is GraphQLString


def test_column_type_to_graphql_nvarchar_parameterized():
    assert column_type_to_graphql("NVARCHAR(64)") is GraphQLString


def test_column_type_to_graphql_nclob():
    assert column_type_to_graphql("NCLOB") is GraphQLString


def test_column_type_to_graphql_unmapped_type_raises():
    with pytest.raises(ValueError, match="Unmapped the engine type"):
        column_type_to_graphql("some_made_up_type")
