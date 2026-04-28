# Copyright (c) 2026 Kenneth Stott
# Canary: d0e1f2a3-b4c5-6789-3456-890123456789
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Neo4j query preview and shape validation (Phase AO)."""

import pytest

from provisa.neo4j.preview import (
    Neo4jNodeObjectError,
    _ensure_limit,
    validate_shape,
)


class TestEnsureLimit:
    def test_appends_limit_when_absent(self):
        cypher = "MATCH (n:Order) RETURN n.id AS id, n.amount AS amount"
        result = _ensure_limit(cypher, limit=5)
        assert result.endswith(" LIMIT 5")

    def test_preserves_existing_limit(self):
        cypher = "MATCH (n) RETURN n.id AS id LIMIT 10"
        result = _ensure_limit(cypher)
        assert result == cypher

    def test_existing_limit_case_insensitive(self):
        cypher = "MATCH (n) RETURN n.name AS name limit 20"
        result = _ensure_limit(cypher)
        assert result == cypher

    def test_strips_trailing_semicolon_before_appending(self):
        cypher = "MATCH (n) RETURN n.id AS id;"
        result = _ensure_limit(cypher, limit=5)
        assert not result.endswith(";")
        assert "LIMIT 5" in result

    def test_does_not_double_limit(self):
        cypher = "MATCH (n) RETURN n.id AS id LIMIT 5"
        result = _ensure_limit(cypher, limit=5)
        assert result.count("LIMIT") == 1


class TestValidateShape:
    def test_flat_scalars_pass(self):
        rows = [
            {"name": "Alice", "age": 30, "active": True},
            {"name": "Bob", "age": 25, "active": False},
        ]
        # Should not raise
        validate_shape(rows)

    def test_none_values_pass(self):
        rows = [{"name": None, "age": 0}]
        validate_shape(rows)  # should not raise

    def test_dict_value_raises(self):
        rows = [{"n": {"labels": ["Order"], "properties": {"id": 1}}}]
        with pytest.raises(Neo4jNodeObjectError, match="node object"):
            validate_shape(rows)

    def test_list_value_raises(self):
        rows = [{"tags": ["a", "b"]}]
        with pytest.raises(Neo4jNodeObjectError, match="node object or list"):
            validate_shape(rows)

    def test_error_message_includes_column_name(self):
        rows = [{"my_column": {"nested": "object"}}]
        with pytest.raises(Neo4jNodeObjectError, match="my_column"):
            validate_shape(rows)

    def test_error_message_includes_guidance(self):
        rows = [{"n": {"id": 1}}]
        with pytest.raises(Neo4jNodeObjectError, match="n.prop"):
            validate_shape(rows)

    def test_empty_rows_pass(self):
        validate_shape([])  # should not raise
