# Copyright (c) 2025 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Trino type → GraphQL type mapping."""

import pytest
from graphql import GraphQLBoolean, GraphQLFloat, GraphQLInt, GraphQLList, GraphQLString

from provisa.compiler.type_map import (
    BigInt,
    Date,
    DateTime,
    JSONScalar,
    trino_to_graphql,
)


class TestTrinoToGraphQL:
    # String types
    def test_varchar(self):
        assert trino_to_graphql("varchar") is GraphQLString

    def test_varchar_parameterized(self):
        assert trino_to_graphql("varchar(255)") is GraphQLString

    def test_char(self):
        assert trino_to_graphql("char") is GraphQLString

    def test_char_parameterized(self):
        assert trino_to_graphql("char(10)") is GraphQLString

    def test_varbinary(self):
        assert trino_to_graphql("varbinary") is GraphQLString

    def test_uuid(self):
        assert trino_to_graphql("uuid") is GraphQLString

    # Integer types
    def test_integer(self):
        assert trino_to_graphql("integer") is GraphQLInt

    def test_int(self):
        assert trino_to_graphql("int") is GraphQLInt

    def test_smallint(self):
        assert trino_to_graphql("smallint") is GraphQLInt

    def test_tinyint(self):
        assert trino_to_graphql("tinyint") is GraphQLInt

    def test_bigint(self):
        assert trino_to_graphql("bigint") is BigInt

    # Float types
    def test_real(self):
        assert trino_to_graphql("real") is GraphQLFloat

    def test_double(self):
        assert trino_to_graphql("double") is GraphQLFloat

    def test_decimal(self):
        assert trino_to_graphql("decimal") is GraphQLFloat

    def test_decimal_parameterized(self):
        assert trino_to_graphql("decimal(10,2)") is GraphQLFloat

    def test_numeric(self):
        assert trino_to_graphql("numeric") is GraphQLFloat

    # Boolean
    def test_boolean(self):
        assert trino_to_graphql("boolean") is GraphQLBoolean

    # Date/time
    def test_date(self):
        assert trino_to_graphql("date") is Date

    def test_timestamp(self):
        assert trino_to_graphql("timestamp") is DateTime

    def test_timestamp_with_tz(self):
        assert trino_to_graphql("timestamp with time zone") is DateTime

    def test_time(self):
        assert trino_to_graphql("time") is GraphQLString

    # JSON
    def test_json(self):
        assert trino_to_graphql("json") is JSONScalar

    # Array
    def test_array_varchar(self):
        result = trino_to_graphql("array(varchar)")
        assert isinstance(result, GraphQLList)

    def test_array_integer(self):
        result = trino_to_graphql("array(integer)")
        assert isinstance(result, GraphQLList)

    # Case insensitivity
    def test_case_insensitive(self):
        assert trino_to_graphql("VARCHAR") is GraphQLString
        assert trino_to_graphql("Integer") is GraphQLInt

    # Unknown type
    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unmapped Trino type"):
            trino_to_graphql("geometry")

    # Whitespace
    def test_whitespace_stripped(self):
        assert trino_to_graphql("  varchar  ") is GraphQLString
