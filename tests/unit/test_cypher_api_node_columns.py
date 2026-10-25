# Copyright (c) 2026 Kenneth Stott
# Canary: b50a86be-2844-4278-990b-2f0c46caccb8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-301: a Cypher node over an API-backed table reads the columns it was materialized as."""

from __future__ import annotations

from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator_helpers import _node_table_expr
from provisa.security.rights import ALL_DOMAINS


def _ctx(source_type: str) -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables["pet_by_status"] = TableMeta(
        table_id=1,
        field_name="pet_by_status",
        type_name="PetByStatus",
        source_id="pet-store",
        catalog_name="pet_store",
        schema_name="public",
        table_name="pet_by_status",
        source_type=source_type,
    )
    ctx.aggregate_columns[1] = [("id", "integer"), ("photoUrls", "text")]
    ctx.physical_to_sql[(1, "id")] = "id"
    ctx.physical_to_sql[(1, "photoUrls")] = "photo_urls"
    return ctx


def test_an_api_node_reads_the_sql_names_it_was_materialized_under():
    nm = CypherLabelMap.from_schema(_ctx("openapi"), [ALL_DOMAINS]).nodes["PetByStatus"]

    assert nm.physical_properties["photoUrls"] == "photo_urls"
    # No wrapper renaming a field's own spelling, which is no column of the hot CTE.
    assert "photoUrls" not in _node_table_expr(nm, "n").sql(dialect="postgres")


def test_a_database_node_keeps_its_physical_names():
    nm = CypherLabelMap.from_schema(_ctx("postgresql"), [ALL_DOMAINS]).nodes["PetByStatus"]

    assert nm.physical_properties["photoUrls"] == "photoUrls"
    assert '"photoUrls" AS photo_urls' in _node_table_expr(nm, "n").sql(dialect="postgres")
