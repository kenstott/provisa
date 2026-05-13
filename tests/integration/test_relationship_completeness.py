# Copyright (c) 2026 Kenneth Stott
# Canary: c3d4e5f6-a7b8-9012-cdef-123456789012
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests: every defined relationship must be reachable in the admin SDL.

Two invariants:
  1. Column integrity — every relationship's source_column and target_column must
     exist in the respective table's registered columns (table_columns PG table).
     Broken column references cause _can_see_relationship to return False, which
     silently drops the relationship from the SDL with no error.
  2. Schema completeness — every relationship whose columns pass the integrity
     check must produce a field on the source GQL type in the generated schema.
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio
import trino as _trino_mod

from provisa.compiler.introspect import introspect_table_columns
from provisa.compiler.naming import domain_gql_alias, rel_field_name, source_to_catalog
from provisa.compiler.schema_gen import (
    SchemaInput,
    _assign_names,
    _build_visible_tables,
    _can_see_relationship,
    generate_schema,
)
from provisa.core.repositories import (
    domain as domain_repo,
    relationship as rel_repo,
    role as role_repo,
    table as table_repo,
)

pytestmark = [pytest.mark.integration]


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def _rel_completeness_data(pg_pool):
    """Load tables + relationships + roles + domains, and introspect Trino column types."""
    async with pg_pool.acquire() as conn:
        tables = await table_repo.list_all(conn)
        rels = await rel_repo.list_all(conn)
        roles = await role_repo.list_all(conn)
        domains = await domain_repo.list_all(conn)
        naming_rules = [
            dict(r) for r in await conn.fetch("SELECT pattern, replacement FROM naming_rules")
        ]

    _trino = _trino_mod.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="test",
    )
    column_types: dict = {}
    try:
        for table in tables:
            catalog = source_to_catalog(table["source_id"])
            try:
                cols = introspect_table_columns(
                    _trino, catalog, table["schema_name"], table["table_name"]
                )
                column_types[table["id"]] = cols
            except Exception:
                column_types[table["id"]] = []
    finally:
        _trino.close()

    return {
        "tables": tables,
        "rels": rels,
        "roles": {r["id"]: r for r in roles},
        "domains": domains,
        "naming_rules": naming_rules,
        "column_types": column_types,
    }


@pytest.mark.asyncio(loop_scope="session")
class TestRelationshipCompleteness:
    """All defined DB relationships must resolve correctly in the admin SDL."""

    async def test_relationship_columns_exist_in_table_columns(self, _rel_completeness_data):
        """Every relationship source_column/target_column must exist in table_columns.

        A column mismatch causes _can_see_relationship to silently drop the
        relationship from the SDL.  This test catches wrong column names stored
        in the relationships table (e.g. snake_case vs camelCase for remote tables).
        """
        data = _rel_completeness_data
        table_id_to_name = {t["id"]: t["table_name"] for t in data["tables"]}
        table_columns_map: dict[int, set[str]] = {
            t["id"]: {c["column_name"] for c in t["columns"]} for t in data["tables"]
        }

        failures: list[str] = []
        for rel in data["rels"]:
            src_id = rel["source_table_id"]
            tgt_id = rel.get("target_table_id")
            src_col = rel.get("source_column") or ""
            tgt_col = rel.get("target_column") or ""

            if src_col and src_id in table_columns_map and src_col not in table_columns_map[src_id]:
                failures.append(
                    f"  {rel['id']}: source_column={src_col!r} "
                    f"not registered in table {table_id_to_name.get(src_id, src_id)!r}"
                )
            if (
                tgt_id
                and tgt_col
                and tgt_id in table_columns_map
                and tgt_col not in table_columns_map[tgt_id]
            ):
                failures.append(
                    f"  {rel['id']}: target_column={tgt_col!r} "
                    f"not registered in table {table_id_to_name.get(tgt_id, tgt_id)!r}"
                )

        assert not failures, (
            "Relationship column references missing from table_columns.\n"
            "Fix: update the relationship with the correct column name.\n" + "\n".join(failures)
        )

    async def test_all_relationships_visible_in_admin_schema(self, _rel_completeness_data):
        """Every relationship where both columns are registered must appear in the admin schema."""
        data = _rel_completeness_data
        if "admin" not in data["roles"]:
            pytest.skip("admin role not present in this environment")

        admin_role = data["roles"]["admin"]

        si = SchemaInput(
            tables=data["tables"],
            relationships=data["rels"],
            column_types=data["column_types"],
            naming_rules=data["naming_rules"],
            role=admin_role,
            domains=data["domains"],
        )

        visible_tables = _build_visible_tables(si)
        domain_alias_map = {
            d["id"]: domain_gql_alias(d["id"], d.get("graphql_alias"))
            for d in data["domains"]
            if domain_gql_alias(d["id"], d.get("graphql_alias"))
        }
        _assign_names(
            visible_tables,
            data["naming_rules"],
            domain_prefix=False,
            domain_alias_map=domain_alias_map,
        )
        table_lookup = {t.table_id: t for t in visible_tables}

        visible_rels = [r for r in data["rels"] if _can_see_relationship(r, table_lookup)]

        if not visible_rels:
            pytest.skip("No visible relationships in this environment")

        schema = generate_schema(si)

        missing_fields: list[str] = []
        for rel in visible_rels:
            src_info = table_lookup.get(rel["source_table_id"])
            tgt_info = table_lookup.get(rel.get("target_table_id"))
            if not src_info or not tgt_info:
                continue
            expected_field = rel.get("graphql_alias") or rel_field_name(
                tgt_info.field_name, rel["cardinality"]
            )
            src_gql_type = schema.type_map.get(src_info.type_name)
            if src_gql_type is None or expected_field not in (src_gql_type.fields or {}):
                missing_fields.append(
                    f"  {rel['id']}: expected field {expected_field!r} "
                    f"on type {src_info.type_name!r}"
                )

        assert not missing_fields, (
            "Visible relationships not found as fields in the admin schema:\n"
            + "\n".join(missing_fields)
        )
