# Copyright (c) 2026 Kenneth Stott
# Canary: 6d0f5a13-2f0e-4a1c-9c3a-1b0d2a5e7c41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1614: a table's API parameters do not, by themselves, put it in a role's schema.

Native filter columns (REST path/query parameters) are exempt from the per-column ``visible_to``
gate, and they stay exempt -- a role that can read an endpoint's rows may always pass its
arguments. What they are not is a way INTO the table. There is no table-level grant in this
model, so the reach the parameters ride on is implied from the column grants: at least one data
column visible to the role. Without that condition a role no column was granted to still saw
every API-backed table, which is exactly what reduced the sandbox visitor's domain (REQ-1597) to
a handful of endpoint fields whose only arguments were the ones the gate never covered.
"""

from __future__ import annotations

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import _build_visible_tables
from provisa.compiler.schema_types import SchemaInput

TABLE_ID = 7


def _table(*, data_visible_to: list[str]) -> dict:
    """One REST-backed table: a granted data column and a path parameter."""
    return {
        "id": TABLE_ID,
        "domain_id": "pet-store",
        "source_id": "petstore-api",
        "schema_name": "api",
        "table_name": "get_pet_by_id",
        "columns": [
            {"column_name": "name", "visible_to": data_visible_to, "native_filter_type": None},
            {"column_name": "pet_id", "visible_to": [], "native_filter_type": "path"},
        ],
    }


def _schema_input(role_id: str, *, data_visible_to: list[str]) -> SchemaInput:
    return SchemaInput(
        tables=[_table(data_visible_to=data_visible_to)],
        relationships=[],
        column_types={
            TABLE_ID: [
                ColumnMetadata(column_name="name", data_type="varchar", is_nullable=True),
                ColumnMetadata(column_name="pet_id", data_type="bigint", is_nullable=False),
            ]
        },
        naming_rules=[],
        role={"id": role_id, "domain_access": ["*"], "capabilities": []},
        domains=[{"id": "pet-store"}],
    )


def test_ungranted_role_does_not_reach_the_table_through_its_parameters():
    si = _schema_input("sandbox", data_visible_to=["org_admin"])
    assert _build_visible_tables(si) == []


def test_granted_role_gets_the_table_and_its_parameters():
    si = _schema_input("org_admin", data_visible_to=["org_admin"])
    (info,) = _build_visible_tables(si)
    assert [c["column_name"] for c in info.visible_columns] == ["name"]
    assert [c["column_name"] for c in info.native_filter_columns] == ["pet_id"]


def test_an_unrestricted_data_column_still_carries_every_role_in():
    """visible_to=[] is unrestricted, so the implied reach is every role's -- as before."""
    si = _schema_input("sandbox", data_visible_to=[])
    (info,) = _build_visible_tables(si)
    assert [c["column_name"] for c in info.visible_columns] == ["name"]
    assert [c["column_name"] for c in info.native_filter_columns] == ["pet_id"]
