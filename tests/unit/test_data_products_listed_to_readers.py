# Copyright (c) 2026 Kenneth Stott
# Canary: 9494a217-e721-4518-a8bb-2762faff29a9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2155 (DATA BUYER): the catalog lists data products only to a role holding
data_product_read. Viewer no longer does, so data_products and data_product_edges are served,
by the schema build and by the SQL path alike, to Data buyer, Data product owner, Data steward
and Org admin, and to no role built on Viewer alone. The rest of the catalog is unchanged.
"""

# Requirements: REQ-2155

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import _build_visible_tables
from provisa.compiler.schema_types import SchemaInput
from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.compiler.stage2 import build_governance_context
from provisa.core.db import STANDARD_ROLE_RIGHTS
from provisa.security.rights import META_DOMAIN_ID, META_PRODUCT_TABLES

#: (table id, catalog name, view name): the two tables that list products and one that does not.
_META = [
    (1, "data_products", "data_products_meta"),
    (2, "data_product_edges", "data_product_edges"),
    (3, "registered_tables", "registered_tables_meta"),
]


def _tables(reader: str) -> list[dict]:
    return [
        {
            "id": tid,
            "domain_id": META_DOMAIN_ID,
            "source_id": "provisa-admin",
            "schema_name": "public",
            "table_name": name,
            "columns": [
                {
                    "column_name": "id",
                    "data_type": "varchar",
                    "visible_to": [reader],
                    "scope": "domain",
                    "native_filter_type": None,
                }
            ],
            "write_ops": [],
            "write_returns_rows": True,
            "write_refused_forms": [],
        }
        for tid, name, _view in _META
    ]


def _role(standard: str) -> dict:
    return {
        "id": standard,
        "capabilities": sorted(STANDARD_ROLE_RIGHTS[standard]),
        "domain_access": ["*"],
    }


def _schema_tables(role: dict) -> set[str]:
    tables = _tables(role["id"])
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types={
            t["id"]: [ColumnMetadata(column_name="id", data_type="varchar", is_nullable=True)]
            for t in tables
        },
        naming_rules=[],
        role=role,
        domains=[{"id": META_DOMAIN_ID}],
    )
    return {info.table_name for info in _build_visible_tables(si)}


def _sql_visible(role: dict) -> dict[str, frozenset[str] | None]:
    ctx = CompilationContext()
    for tid, name, view in _META:
        ctx.tables[name] = TableMeta(
            table_id=tid,
            field_name=name,
            type_name=name,
            source_id="provisa-admin",
            catalog_name="main",
            schema_name="public",
            table_name=view,
            domain_id=META_DOMAIN_ID,
            original_table_name=name if view != name else "",
        )
    gov = build_governance_context(
        role["id"], SimpleNamespace(rules={}), {}, ctx, _tables(role["id"]), role
    )
    return {name: gov.visible_columns.get(tid) for tid, name, _view in _META}


def test_the_tables_that_list_products_are_named():
    assert META_PRODUCT_TABLES == {"data_products", "data_product_edges"}


@pytest.mark.parametrize(
    "standard", ["data_buyer", "data_product_owner", "data_steward", "org_admin", "sandbox"]
)
def test_a_role_reading_data_products_is_served_them(standard):
    assert _schema_tables(_role(standard)) == {name for _tid, name, _view in _META}
    visible = _sql_visible(_role(standard))
    assert all(visible[name] == frozenset({"id"}) for name in META_PRODUCT_TABLES)


@pytest.mark.parametrize("standard", ["viewer", "analyst", "developer", "dq_reader"])
def test_a_role_built_on_viewer_alone_is_served_no_product_and_the_rest_of_the_catalog(standard):
    assert _schema_tables(_role(standard)) == {"registered_tables"}
    visible = _sql_visible(_role(standard))
    assert all(visible[name] == frozenset() for name in META_PRODUCT_TABLES)
    assert visible["registered_tables"] == frozenset({"id"})
