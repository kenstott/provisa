# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1e4f0a-93c7-4d28-a5e1-7c02d9b8f314
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2188: governed metrics as a meta-domain model table, read the way data_products is.

The metrics table is registered in the meta domain by the same seed as every other catalog table,
described column by column, with its role list (``visible_to``) in the governance tier. A metric
names no owner and no domain of its own, so no relationship is seeded for it.
"""

# Requirements: REQ-2188

from __future__ import annotations

import pytest

from provisa.api._catalog_descriptions import COLUMN_DESCRIPTIONS, TABLE_DESCRIPTIONS
from provisa.api._meta_seed import META_RELATIONSHIPS
from provisa.api.app_loaders import _META_TABLES
from provisa.security.rights import GOVERNANCE_META_COLUMNS

METRIC_COLUMNS = {
    "name",
    "expression",
    "datatype",
    "description",
    "ai_context",
    "visible_to",
    "from_fact",
}


def test_metrics_is_a_meta_table_read_through_a_view():
    """Registered in the meta domain beside data_products. ops already has a "metrics" table, and
    the catalog descriptions are keyed by table name, so the meta table is metric_definitions."""
    assert "metric_definitions" in _META_TABLES
    assert "metrics" not in _META_TABLES  # ops owns "metrics" (OTel points)


def test_every_metric_column_is_described():
    assert "metric_definitions" in TABLE_DESCRIPTIONS
    assert set(COLUMN_DESCRIPTIONS["metric_definitions"]) == METRIC_COLUMNS


def test_the_role_list_is_a_governance_column_like_other_meta_tables():
    """A caller without view_governance does not see who may read a metric; the definition
    columns are discovery columns."""
    assert "visible_to" in GOVERNANCE_META_COLUMNS
    assert not (METRIC_COLUMNS - {"visible_to"}) & GOVERNANCE_META_COLUMNS


def test_a_metric_has_no_owner_or_domain_so_no_relationship_is_seeded_for_it():
    assert not [r for r in META_RELATIONSHIPS if {"metrics", "metric_definitions"} & {r[2], r[7]}]


@pytest.mark.asyncio
async def test_metrics_are_read_through_the_meta_model(control_plane):
    from sqlalchemy import insert, select

    from provisa.api.startup_seed import _seed_meta_domain
    from provisa.core.models import Metric
    from provisa.core.repositories import metric as metric_repo
    from sqlalchemy import text

    from provisa.core.schema_org import registered_tables, sources, table_columns

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await metric_repo.upsert(
                conn,
                Metric(
                    name="net_revenue",
                    expression="SUM(orders.amount) - SUM(orders.refunds)",
                    description="Revenue after refunds",
                    visible_to=["analyst"],
                ),
            )
            await conn.execute_core(insert(sources).values(id="provisa-admin", type="postgresql"))
            await _seed_meta_domain(conn, org_id="default")

            rows = (
                await conn.execute_core(text("SELECT name, visible_to FROM metric_definitions"))
            ).fetchall()
            assert [r[0] for r in rows] == ["net_revenue"]

            tables = (
                await conn.execute_core(
                    select(registered_tables).where(
                        registered_tables.c.domain_id == "meta",
                        registered_tables.c.table_name == "metric_definitions",
                    )
                )
            ).fetchall()
            assert len(tables) == 1
            table = dict(tables[0]._mapping)
            assert table["source_id"] == "provisa-admin"
            assert table["description"] == TABLE_DESCRIPTIONS["metric_definitions"]

            cols = (
                await conn.execute_core(
                    select(table_columns).where(table_columns.c.table_id == table["id"])
                )
            ).fetchall()
            by_name = {c._mapping["column_name"]: dict(c._mapping) for c in cols}
            assert set(by_name) == METRIC_COLUMNS
            for name, col in by_name.items():
                assert col["description"] == COLUMN_DESCRIPTIONS["metric_definitions"][name]
    finally:
        await db.close()
