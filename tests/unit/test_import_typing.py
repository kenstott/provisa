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

    def has(self, source_id):
        return source_id in self.added

    async def execute(self, source_id, sql, params):
        for (schema, table), cols in _SCHEMA.items():
            if params == [schema, table]:
                # PostgreSQL's catalog row: name, data_type, udt_name, format_type, typtype.
                return SimpleNamespace(rows=[(n, t, t, t, "b") for n, t in cols])
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


class _FunctionPool(_Pool):
    """A database whose catalog holds ``public.search_albums(search text)`` returning albums."""

    async def execute(self, source_id, sql, params):
        if "FROM pg_proc" in sql:
            if "'search_albums'" in sql:
                return SimpleNamespace(rows=[("public", "albums", ["search"], ["text"])])
            return SimpleNamespace(rows=[])
        return await super().execute(source_id, sql, params)


def _albums():
    return Table(
        source_id="default",
        domain_id="music",
        schema_name="public",
        table_name="albums",
        columns=[Column(name="id", data_type="integer", visible_to=["customer"])],
    )


def _pending(name):
    return {
        "name": name,
        "source_id": "default",
        "schema_name": "public",
        "kind": "query",
        "visible_to": ["customer"],
    }


@pytest.mark.asyncio
async def test_a_tracked_function_takes_its_signature_from_the_database():
    """REQ-205: the export names a tracked function and its roles; the preview reads its
    arguments and the table it returns from the database it reaches."""
    cfg = _config([_albums()])
    collector = WarningCollector()
    collector.pending_functions.append(_pending("search_albums"))
    await type_imported_columns(cfg, collector, pool_factory=_FunctionPool)
    (fn,) = cfg.functions
    assert (fn.name, fn.kind, fn.returns) == ("search_albums", "query", "default.public.albums")
    assert [(a.name, a.type) for a in fn.arguments] == [("search", "text")]
    assert fn.visible_to == ["customer"] and fn.domain_id == "music"
    assert not collector.warnings and not collector.pending_functions


@pytest.mark.asyncio
async def test_a_tracked_function_the_database_lacks_is_left_to_redo():
    cfg = _config([_albums()])
    collector = WarningCollector()
    collector.pending_functions.append(_pending("gone"))
    await type_imported_columns(cfg, collector, pool_factory=_FunctionPool)
    assert not cfg.functions
    assert [w.category for w in collector.warnings] == ["functions"]


@pytest.mark.asyncio
async def test_a_tracked_function_on_an_unreached_source_is_left_to_redo():
    cfg = _config([_albums()])
    collector = WarningCollector()
    collector.pending_functions.append(_pending("search_albums"))
    await type_imported_columns(cfg, collector, pool_factory=lambda: _FunctionPool(False))
    assert not cfg.functions
    assert {w.category for w in collector.warnings} == {"sources", "functions"}
