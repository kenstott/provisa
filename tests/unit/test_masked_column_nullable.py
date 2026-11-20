# Copyright (c) 2026 Kenneth Stott
# Canary: 5e2b9c41-7a3d-4f08-b6e1-2d9f0c8a4b73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2131 (amended 8fa0d1c262): a column a reader sees under a mask is reported nullable to that
reader in every catalog the surfaces expose, though it is declared NOT NULL: the GraphQL schema,
the catalog pgwire and Flight SQL list (attnotnull, information_schema, the Arrow schema). A reader
who sees it in clear is told what it declares. pgwire's meta views are covered with the live
catalog (test_catalog_views_live)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from graphql import GraphQLNonNull, get_named_type

from provisa.api.flight.flightsql_catalog import role_catalog, table_arrow_schema
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.security.masking import MaskingRule, MaskType, columns_masked_for

ROLE = "reader"
COLUMN_TYPES = {
    1: [
        ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
        ColumnMetadata(column_name="amount", data_type="integer", is_nullable=False),
        ColumnMetadata(column_name="email", data_type="varchar", is_nullable=False),
    ]
}
NULLED = MaskingRule(mask_type=MaskType.constant, value=None)
STARRED = MaskingRule(mask_type=MaskType.constant, value="***")
FAKED = MaskingRule(mask_type=MaskType.fake, fake="email")


def _schema_input(masked: frozenset[tuple[int, str]]) -> SchemaInput:
    return SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "pg",
                "domain_id": "sales",
                "schema_name": "sales",
                "table_name": "orders",
                "columns": [
                    {"column_name": c, "visible_to": [ROLE]} for c in ("id", "amount", "email")
                ],
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
        ],
        relationships=[],
        column_types=COLUMN_TYPES,
        naming_rules=[],
        role={"id": ROLE, "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
        masked_columns=masked,
    )


def test_the_masked_columns_of_a_reader_are_its_own_and_no_fake():
    rules = {
        (1, ROLE): {"amount": (NULLED, "integer"), "email": (FAKED, "varchar")},
        (1, "other"): {"id": (NULLED, "integer")},
        (2, ROLE): {"name": (STARRED, "varchar")},
    }
    assert columns_masked_for(rules, ROLE) == {(1, "amount"), (2, "name")}


def _graphql_fields(masked: frozenset[tuple[int, str]]) -> dict:
    schema = generate_schema(_schema_input(masked))
    assert schema.query_type is not None
    root = next(f for name, f in schema.query_type.fields.items() if name.endswith("orders"))
    return get_named_type(root.type).fields  # pyright: ignore[reportAttributeAccessIssue]


def test_the_graphql_schema_gives_a_masked_not_null_column_as_nullable():
    fields = _graphql_fields(frozenset({(1, "amount"), (1, "email")}))
    assert isinstance(fields["id"].type, GraphQLNonNull)
    assert not isinstance(fields["amount"].type, GraphQLNonNull)
    assert not isinstance(fields["email"].type, GraphQLNonNull)


def test_the_graphql_schema_of_a_reader_seeing_it_in_clear_keeps_not_null():
    fields = _graphql_fields(frozenset())
    assert all(isinstance(fields[c].type, GraphQLNonNull) for c in ("id", "amount", "email"))


def _state(masking_rules: dict) -> SimpleNamespace:
    si = _schema_input(frozenset())
    return SimpleNamespace(
        contexts={ROLE: build_context(si)},
        schema_build_cache={"column_types": COLUMN_TYPES},
        config=None,
        multitenancy=False,
        masking_rules=masking_rules,
    )


@pytest.mark.parametrize("masked", [True, False])
def test_the_sql_catalog_lists_a_masked_column_nullable_to_its_reader(masked):
    rules = {(1, ROLE): {"amount": (NULLED, "integer")}} if masked else {}
    catalog = role_catalog(_state(rules), ROLE)
    [columns] = catalog.columns.values()
    assert columns == [
        ("id", "integer", False),
        ("amount", "integer", masked),
        ("email", "varchar", False),
        ("_name_", "varchar", True),
        ("_domain_", "varchar", True),
    ]


def test_the_flight_schema_of_a_masked_column_is_nullable():
    catalog = role_catalog(_state({(1, ROLE): {"amount": (NULLED, "integer")}}), ROLE)
    [columns] = catalog.columns.values()
    schema = table_arrow_schema(columns)
    assert schema.field("id").nullable is False
    assert schema.field("amount").nullable is True
    assert schema.field("amount").metadata[b"XDBC_IS_NULLABLE"] == b"YES"
