# Copyright (c) 2026 Kenneth Stott
# Canary: 8e2a37d5-91b6-4c0f-ae43-1d75c2b8f906
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every column the built-in meta and ops domains expose carries a description.

The seed writes NULL for a column with no curated text, and the REQ-609 ``stale_metadata`` report
then names it — which is how the deployment ended up reporting hundreds of gaps in Provisa's own
catalog that no data steward could close. This is what keeps that set empty: add a column to a
built-in view and the description must arrive with it.
"""

# Requirements: REQ-609, REQ-884, REQ-1386

from __future__ import annotations

import sqlglot

from provisa.api._catalog_descriptions import COLUMN_DESCRIPTIONS, TABLE_DESCRIPTIONS
from provisa.api._meta_views import (
    _META_TABLE_VIEWS,
    _OPS_LOG_TABLE_VIEWS,
    _OPS_REPORT_VIEWS,
)
from provisa.api.app_loaders import _META_TABLES
from provisa.core.org_registry_view import VIEW_COLUMNS as ORG_REGISTRY_COLUMNS
from provisa.core.org_registry_view import VIEW_NAME as ORG_REGISTRY_VIEW
from provisa.observability.ops_schema import OPS_TABLES


def _view_columns(ddl: str) -> list[str]:
    """Output column names of a CREATE VIEW statement, in select order."""
    parsed = sqlglot.parse_one(ddl, read="postgres")
    select = parsed.expression
    while select.key == "union":
        select = select.this
    return [e.output_name for e in select.expressions]


def _exposed_columns() -> dict[str, list[str]]:
    """Registered table name -> the columns that table registration exposes."""
    from provisa.api.startup_seed import _OPS_VIEWS
    from provisa.core import schema_org

    exposed: dict[str, list[str]] = {}
    for tbl in _META_TABLES:
        ddl = _META_TABLE_VIEWS.get(tbl)
        if ddl is None:
            # No curated view: the physical table registers every column it has.
            exposed[tbl] = [c.name for c in getattr(schema_org, tbl).columns]
        else:
            exposed[tbl] = _view_columns(ddl)
    for tbl, ddl in _OPS_LOG_TABLE_VIEWS.items():
        exposed[tbl] = _view_columns(ddl)
    for view_name, ddl in _OPS_REPORT_VIEWS.items():
        exposed[view_name] = _view_columns(ddl)
    for tbl, cols in OPS_TABLES.items():
        exposed[tbl] = [c for c, _t, _pk in cols]
    for view_name, cols, _ddl in _OPS_VIEWS:
        exposed[view_name] = [c for c, _t, _pk in cols]
    # REQ-1301: registered by seed_org_registry_view, not by the meta seed's table list.
    exposed[ORG_REGISTRY_VIEW] = list(ORG_REGISTRY_COLUMNS)
    return exposed


def test_every_built_in_table_has_a_description():
    missing = sorted(t for t in _exposed_columns() if t not in TABLE_DESCRIPTIONS)
    assert missing == []


def test_every_built_in_column_has_a_description():
    missing = sorted(
        f"{tbl}.{col}"
        for tbl, cols in _exposed_columns().items()
        for col in cols
        if not COLUMN_DESCRIPTIONS.get(tbl, {}).get(col)
    )
    assert missing == []


def test_no_description_is_written_for_a_column_that_is_not_exposed():
    """A stale entry is a description nothing displays — and a rename that silently lost its text."""
    exposed = _exposed_columns()
    stale = sorted(
        f"{tbl}.{col}"
        for tbl, cols in COLUMN_DESCRIPTIONS.items()
        for col in cols
        if col not in exposed.get(tbl, [])
    )
    assert stale == []


class TestTheSeedCarriesTheDescriptions:
    """Curating the text is only half of it — the seed has to write it onto the catalog row."""

    def _payloads(self, seeder, **kw):
        import asyncio
        from unittest.mock import AsyncMock

        conn = AsyncMock()
        conn.upsert_returning = AsyncMock(return_value=7)
        # Reflect the columns the relation actually exposes: a view-backed registration (derived_tags)
        # has no surrogate id, so a fixed one-column stand-in would demand a description for a column
        # the seed never writes.
        exposed = _exposed_columns()

        async def _reflect(name, schema=None):
            del schema
            return [
                {"column_name": c, "data_type": "text", "is_primary_key": c == "id"}
                for c in exposed.get(name, ["id"])
            ]

        conn.reflect_columns = AsyncMock(side_effect=_reflect)
        asyncio.run(seeder(conn, **kw))
        return (
            [c.args[1] for c in conn.upsert_returning.await_args_list],
            [c.args[1] for c in conn.upsert.await_args_list],
            conn,
        )

    def test_every_seeded_table_and_column_row_carries_its_description(self):
        from provisa.api.startup_seed import _seed_meta_domain, _seed_ops_domain, _seed_ops_pg

        for seeder, kw in (
            (_seed_meta_domain, {"org_id": "default"}),
            (_seed_ops_domain, {"org_id": "default"}),
            (_seed_ops_pg, {}),
        ):
            tables, columns, _ = self._payloads(seeder, **kw)
            for payload in tables:
                assert payload["description"], payload["table_name"]
            for payload in columns:
                if "column_name" not in payload:
                    continue  # a relationship row, not a catalog column
                assert payload["description"], payload["column_name"]

    def test_a_description_edited_in_the_ui_survives_the_next_seed(self):
        """The seed re-runs on every start; without COALESCE it would overwrite a steward's text."""
        from provisa.api.startup_seed import _seed_ops_pg

        _, _, conn = self._payloads(_seed_ops_pg)
        for call in conn.upsert.await_args_list:
            assignments = call.kwargs["set_extra"]
            assert "description" in assignments
            assert "coalesce" in str(assignments["description"]).lower()
