# Copyright (c) 2026 Kenneth Stott
# Canary: 5b2e9c71-4d08-4a36-8f15-0c7a3e9d6b24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Two relationships of one type never share a GraphQL field (REQ-194). Two edges from one table
to the same target, neither with its own GraphQL alias, are both named for the target: the later
one used to replace the earlier in the schema with nothing said. The build refuses it, naming
both; given an alias each, both are fields."""

# Requirements: REQ-194

from __future__ import annotations

import pytest

from provisa.compiler import naming as _naming
from provisa.compiler.context import RelationshipFieldTaken, build_context
from provisa.compiler.schema_gen import SchemaInput
from tests.unit.test_group_by import _col


def _input(**aliases: str | None) -> SchemaInput:
    """referrals (1) with two keys into adopters (2): referrer_id and referred_id."""
    _naming.configure(gql="snake")
    tables = [
        {
            "id": table_id,
            "source_id": "pg",
            "domain_id": "care",
            "schema_name": "public",
            "table_name": name,
            "columns": [{"column_name": c, "visible_to": ["admin"]} for c in columns],
            "write_ops": [],
            "write_returns_rows": False,
            "write_refused_forms": [],
        }
        for table_id, name, columns in (
            (1, "referrals", ["id", "referrer_id", "referred_id"]),
            (2, "adopters", ["id", "name"]),
        )
    ]
    column_types = {
        1: [_col("id", "integer"), _col("referrer_id", "integer"), _col("referred_id", "integer")],
        2: [_col("id", "integer"), _col("name", "varchar(100)")],
    }
    relationships = [
        {
            "id": rel_id,
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": column,
            "target_column": "id",
            "cardinality": "many-to-one",
            "graphql_alias": aliases.get(rel_id),
        }
        for rel_id, column in (("to-referrer", "referrer_id"), ("to-referred", "referred_id"))
    ]
    return SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
        domains=[{"id": "care", "description": "Care"}],
    )


def test_two_relationships_named_for_one_field_are_refused_naming_both():
    with pytest.raises(RelationshipFieldTaken) as err:
        build_context(_input())
    assert (err.value.held_by, err.value.refused) == ("to-referrer", "to-referred")
    assert "to-referrer" in str(err.value) and "to-referred" in str(err.value)


def test_each_with_its_own_graphql_alias_is_a_field():
    ctx = build_context(_input(**{"to-referrer": "referrer", "to-referred": "referred"}))
    fields = {field for (_type, field) in ctx.joins}
    assert {"referrer", "referred"} <= fields


def test_a_relationship_named_for_a_column_is_refused_naming_it():
    """REQ-1929: a relationship never hides a column. An alias that is a column's field is
    refused at schema build, naming the relationship, rather than replacing the column."""
    from provisa.compiler.context import RelationshipHidesColumn

    si = _input(**{"to-referrer": "referred_id", "to-referred": "referred"})
    with pytest.raises(RelationshipHidesColumn) as err:
        build_context(si)
    assert (err.value.field, err.value.refused) == ("referred_id", "to-referrer")
    assert "to-referrer" in str(err.value) and "referred_id" in str(err.value)
