# Copyright (c) 2026 Kenneth Stott
# Canary: 65772518-3253-46e7-9bbb-f16825522a1f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table served under an alias has its published columns lowered like any other (REQ-641,
issue #140): customer360's Salesforce ``Account``, served as ``accounts``, has ``BillingCountry``
published as ``billing_country``, and a Cypher node's star read that name on a table that has none."""

# Requirements: REQ-641, REQ-2034

from __future__ import annotations

from provisa.compiler.sql_rewrite import lower_to_catalog_physical
from provisa.compiler.sql_types import CompilationContext, TableMeta


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables["customer360__accounts"] = TableMeta(
        table_id=1,
        field_name="customer360__accounts",
        type_name="Customer360_Accounts",
        source_id="salesforce",
        catalog_name="salesforce",
        schema_name="salesforce",
        table_name="Account",
        display_name="accounts",
        domain_id="customer360",
        source_type="salesforce",
    )
    ctx.physical_to_sql[(1, "Id")] = "id"
    ctx.physical_to_sql[(1, "BillingCountry")] = "billing_country"
    return ctx


def test_a_published_column_of_an_aliased_table_is_lowered_to_its_physical_name():
    lowered = lower_to_catalog_physical(
        'SELECT "accounts"."billing_country" FROM "customer360"."accounts" AS "accounts"', _ctx()
    )
    assert '"accounts"."BillingCountry" AS "billing_country"' in lowered
    assert '"salesforce"."salesforce"."Account" AS "accounts"' in lowered


def test_a_node_star_read_of_an_aliased_table_is_lowered_inside_its_wrapper():
    """What a Cypher node over the table is by the time it is lowered: its star expanded under the
    published names, beside the wrapper's own physical renames."""
    lowered = lower_to_catalog_physical(
        'SELECT a."Id" FROM (SELECT "accounts"."billing_country", "BillingCountry" AS billing_country '
        'FROM "customer360"."accounts" AS "accounts") AS a',
        _ctx(),
    )
    assert '"accounts"."billing_country"' not in lowered
    assert '"accounts"."BillingCountry"' in lowered


def test_an_aliased_api_table_keeps_the_sql_names_it_is_materialized_under():
    """REQ-301: an API-backed table's rows are held under the SQL names (a hot VALUES CTE, an
    engine-cache table), so lowering its published columns to the field's own spelling asked the
    relation for a column it does not have (Binder Error: Values list "n" does not have a column
    named "photoUrls", run 42527446894)."""
    ctx = CompilationContext()
    ctx.tables["pet_store__pet_by_status"] = TableMeta(
        table_id=2,
        field_name="pet_store__pet_by_status",
        type_name="PetStore_PetByStatus",
        source_id="petstore-api",
        catalog_name="petstore_api",
        schema_name="default",
        table_name="find_pets_by_status",
        display_name="pet_by_status",
        domain_id="pet_store",
        source_type="openapi",
    )
    ctx.physical_to_sql[(2, "photoUrls")] = "photo_urls"
    lowered = lower_to_catalog_physical(
        'SELECT "n"."photo_urls" FROM "pet_store"."pet_by_status" AS "n"', ctx
    )
    assert '"n"."photo_urls"' in lowered
    assert "photoUrls" not in lowered
