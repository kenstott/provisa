# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8e1d6a-2f9c-4a7e-b5d0-8c3f6e1a9d27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1683: the import preview types the design from the sources it can reach."""

from types import SimpleNamespace

import pytest

from provisa.api.admin.import_typing import type_imported_columns
from provisa.core.models import Column, ProvisaConfig, Source, SourceType, Table
from provisa.import_shared.warnings import WarningCollector

_SCHEMA = {
    ("public", "albums"): [("id", "integer"), ("title", "text"), ("artist_id", "integer")],
    ("public", "artists"): [("id", "integer"), ("name", "character varying")],
}


class _Pool:
    def __init__(self, reachable=True):
        self.reachable = reachable
        self.added: list[str] = []
        self.closed = False

    async def add(self, source_id, *_a, **_k):
        if not self.reachable:
            raise ConnectionError("refused")
        self.added.append(source_id)

    async def execute(self, source_id, sql, params):
        for (schema, table), cols in _SCHEMA.items():
            if f"'{schema}'" in sql and f"'{table}'" in sql:
                return SimpleNamespace(rows=cols)
        return SimpleNamespace(rows=[])

    async def close_all(self):
        self.closed = True


def _config(tables):
    return ProvisaConfig(
        sources=[
            Source(
                id="default",
                type=SourceType.postgresql,
                host="h",
                port=1,
                database="d",
                username="u",
                password="p",
            ),
            Source(id="countries", type=SourceType.graphql_remote, path="https://x"),
        ],
        domains=[],
        tables=tables,
        roles=[],
    )


@pytest.mark.asyncio
async def test_untyped_columns_take_the_source_type():
    cfg = _config(
        [
            Table(
                source_id="default",
                domain_id="music",
                schema_name="public",
                table_name="albums",
                columns=[
                    Column(name="title", visible_to=["user"]),
                    Column(name="artist_id", visible_to=["user"]),
                ],
            ),
        ]
    )
    pool = _Pool()
    col = WarningCollector()
    await type_imported_columns(cfg, col, pool_factory=lambda: pool)
    types = {c.name: c.data_type for c in cfg.tables[0].columns}
    assert types == {"title": "text", "artist_id": "integer"}
    assert pool.closed and pool.added == ["default"]
    assert not col.warnings


@pytest.mark.asyncio
async def test_column_less_table_lands_every_column_visible_to_admin_only():
    cfg = _config(
        [
            Table(
                source_id="default",
                domain_id="music",
                schema_name="public",
                table_name="artists",
                columns=[],
            ),
        ]
    )
    await type_imported_columns(cfg, WarningCollector(), pool_factory=_Pool)
    assert [(c.name, c.data_type, c.visible_to) for c in cfg.tables[0].columns] == [
        ("id", "integer", ["org_admin"]),
        ("name", "text", ["org_admin"]),
    ]


@pytest.mark.asyncio
async def test_unreachable_source_warns_and_leaves_columns_untyped():
    cfg = _config(
        [
            Table(
                source_id="default",
                domain_id="music",
                schema_name="public",
                table_name="albums",
                columns=[Column(name="title", visible_to=["user"])],
            ),
        ]
    )
    col = WarningCollector()
    await type_imported_columns(cfg, col, pool_factory=lambda: _Pool(reachable=False))
    assert cfg.tables[0].columns[0].data_type is None
    assert any(w.category == "sources" and "not reachable" in w.message for w in col.warnings)


@pytest.mark.asyncio
async def test_column_missing_from_source_warns():
    cfg = _config(
        [
            Table(
                source_id="default",
                domain_id="music",
                schema_name="public",
                table_name="albums",
                columns=[Column(name="ghost", visible_to=["user"])],
            ),
        ]
    )
    col = WarningCollector()
    await type_imported_columns(cfg, col, pool_factory=_Pool)
    assert cfg.tables[0].columns[0].data_type is None
    assert any("ghost is not a column" in w.message for w in col.warnings)


@pytest.mark.asyncio
async def test_non_sql_sources_and_typed_tables_open_no_pool():
    cfg = _config(
        [
            Table(
                source_id="countries",
                domain_id="geo",
                schema_name="graphql",
                table_name="countries",
                columns=[Column(name="code", data_type="varchar", visible_to=["user"])],
            ),
        ]
    )
    opened = []
    await type_imported_columns(cfg, WarningCollector(), pool_factory=lambda: opened.append(1))
    assert opened == []
