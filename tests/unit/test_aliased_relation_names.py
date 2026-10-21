# Copyright (c) 2026 Kenneth Stott
# Canary: d70186da-1e21-42de-bf6f-a38d4f836797
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table read through a physical alias -- a meta view (registered_tables ->
registered_tables_meta), an ops log view, a Kafka topic's table -- is a registered relation
under its physical schema.table, as any other table is.

The compiled context names such a table by its alias; the registry by its stored name. A /data
read lowered to ``schema.table`` without a catalog named neither, and every such read was refused
V006 "is not a registered table".
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import sqlglot

from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.compiler.sql_validator import _check_registered_relations
from provisa.compiler.stage2 import build_governance_context
from provisa.security.rights import META_DOMAIN_ID

_TID = 5


def _governance():
    ctx = CompilationContext()
    ctx.tables["registered_tables"] = TableMeta(
        table_id=_TID,
        field_name="registered_tables",
        type_name="RegisteredTables",
        source_id="provisa-admin",
        catalog_name="provisa_admin",
        schema_name="org_e2e",
        table_name="registered_tables_meta",  # the physical alias the meta view is read by
        domain_id=META_DOMAIN_ID,
    )
    tables = [
        {
            "id": _TID,
            "domain_id": META_DOMAIN_ID,
            "schema_name": "org_e2e",
            "table_name": "registered_tables",  # the stored name
            "columns": [{"column_name": "id", "data_type": "integer"}],
        }
    ]
    role = {"id": "analyst", "capabilities": [], "domain_access": [META_DOMAIN_ID]}
    gov = build_governance_context(
        "analyst", SimpleNamespace(rules={}), {}, ctx, tables, role, relationships=[]
    )
    return ctx, gov


@pytest.mark.parametrize(
    "relation",
    [
        "org_e2e.registered_tables_meta",  # physical, as a /data read is lowered without a catalog
        "provisa_admin.org_e2e.registered_tables_meta",  # physical, with its catalog
        "meta.registered_tables",  # semantic
        "org_e2e.registered_tables",  # stored
    ],
)
def test_an_aliased_table_is_registered_under_each_of_its_names(relation):
    ctx, gov = _governance()
    tree = sqlglot.parse_one(f"SELECT id FROM {relation}", read="postgres")
    assert _check_registered_relations(tree, gov, ctx) == []
    assert gov.table_map[relation] == _TID
