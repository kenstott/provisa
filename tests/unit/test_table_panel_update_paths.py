# Copyright (c) 2026 Kenneth Stott
# Canary: 0c7e5a92-4b1d-4f86-9e3a-2d8b6f1c7a45
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What Register Table sets that only some tables have is changed by updateTable too (REQ-2234,
EDITORS FIRST): a ServiceNow table's import set (REQ-1954), a files table's glob and source-file
column (REQ-788), and reading its columns from the live source (REQ-252). Each is saved with the
rights of editing a table, is checked as registration checks it, and is stored; reading the
columns adds those the source has that the table lacks, as registration with discovery does."""

# Requirements: REQ-2234, REQ-1954, REQ-788, REQ-252

from __future__ import annotations

import contextlib
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select, update

from provisa.core.schema_org import registered_tables, sources, table_columns
from tests.unit.gate_identity import serve_every_column
from tests.unit.test_landing_ttl_admin import _db, _table_input, _table_patches

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    """Every column is served here: which ones a caller may read is REQ-2152's own tests."""
    serve_every_column(monkeypatch)


async def _stored(db, *cols):
    async with db.acquire() as conn:
        return (
            await conn.execute_core(
                select(*(getattr(registered_tables.c, c) for c in cols)).where(
                    registered_tables.c.table_name == "orders"
                )
            )
        ).one()


@contextlib.asynccontextmanager
async def _no_vault():
    yield


def _servicenow_maps(maps):
    """The instance's active transform maps into the table, as (name, import set)."""
    return (
        patch(
            "provisa.federation.pgwire_replica.servicenow_transform_maps",
            new=AsyncMock(return_value=maps),
        ),
        patch("provisa.core.secrets_store.bound_to_request_org", _no_vault),
    )


def _steward_only():
    """A caller holding a governance right but not table_registration: a save may change only
    how columns are hidden (REQ-1944). Its save is judged against the table as stored, read as
    the editor reads it, which takes the deployment's configuration."""
    import provisa.api.app as appmod
    from provisa.core.models import ProvisaConfig

    config = ProvisaConfig.model_validate({"sources": [], "domains": [], "tables": [], "roles": []})
    return (
        patch(
            "provisa.api.admin.capabilities.has_capability",
            side_effect=lambda _info, right: right == "column_grant",
        ),
        patch.object(appmod.state, "config", config),
    )


async def _update(db, input_, *extra):
    from provisa.api.admin.schema_mutation import Mutation

    with ExitStack() as stack:
        for p in (*_table_patches(db), *extra):
            stack.enter_context(p)
        return await Mutation().update_table(MagicMock(), input_)


# --- the import set ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_import_set_is_stored_by_update_table(control_plane):
    async with _db(control_plane, source_type="servicenow", table_ttl=60) as db:
        result = await _update(
            db,
            _table_input(import_set="imp_order"),
            *_servicenow_maps([("Order Import", "imp_order")]),
        )
        assert result.success is True, result.message
        assert (await _stored(db, "import_set")).import_set == "imp_order"
        # Unbound again: its inserts go to the table directly.
        result = await _update(
            db, _table_input(import_set=None), *_servicenow_maps([("Order Import", "imp_order")])
        )
        assert result.success is True, result.message
        assert (await _stored(db, "import_set")).import_set is None


@pytest.mark.asyncio
async def test_an_import_set_no_active_map_writes_through_is_refused(control_plane):
    async with _db(control_plane, source_type="servicenow", table_ttl=60) as db:
        result = await _update(
            db,
            _table_input(import_set="imp_gone"),
            *_servicenow_maps([("Order Import", "imp_order")]),
        )
        assert result.success is False and result.code == "schema.import_set_unknown"
        assert (await _stored(db, "import_set")).import_set is None


@pytest.mark.asyncio
async def test_an_import_set_on_a_table_of_another_source_is_refused(control_plane):
    async with _db(control_plane, source_type="postgresql", table_ttl=60) as db:
        result = await _update(db, _table_input(import_set="imp_order"))
        assert result.success is False and result.code == "schema.import_set_not_servicenow"
        assert (await _stored(db, "import_set")).import_set is None


@pytest.mark.asyncio
async def test_an_import_set_takes_the_right_to_edit_the_table(control_plane):
    async with _db(control_plane, source_type="servicenow", table_ttl=60) as db:
        with pytest.raises(PermissionError, match="import_set"):
            await _update(
                db,
                _table_input(import_set="imp_order"),
                *_servicenow_maps([("Order Import", "imp_order")]),
                *_steward_only(),
            )
        assert (await _stored(db, "import_set")).import_set is None


# --- the file glob and source-file column -----------------------------------------------------


async def _files_source(db, tmp_path, *files):
    for name, text in files:
        (tmp_path / name).write_text(text)
    async with db.acquire() as conn:
        await conn.execute_core(
            update(sources).where(sources.c.id == "s").values(path=str(tmp_path / files[0][0]))
        )


@pytest.mark.asyncio
async def test_a_file_glob_and_source_file_column_are_stored_by_update_table(
    control_plane, tmp_path
):
    async with _db(control_plane, source_type="files", table_ttl=60) as db:
        await _files_source(db, tmp_path, ("a.csv", "id\n1\n"), ("b.csv", "id\n2\n"))
        result = await _update(
            db, _table_input(file_glob="*.csv", source_file_column="loaded_from")
        )
        assert result.success is True, result.message
        row = await _stored(db, "file_glob", "source_file_column")
        assert (row.file_glob, row.source_file_column) == ("*.csv", "loaded_from")


@pytest.mark.asyncio
async def test_a_file_glob_over_files_that_differ_is_refused_as_registration_refuses_it(
    control_plane, tmp_path
):
    async with _db(control_plane, source_type="files", table_ttl=60) as db:
        await _files_source(db, tmp_path, ("a.csv", "id\n1\n"), ("b.csv", "id,extra\n2,x\n"))
        result = await _update(db, _table_input(file_glob="*.csv"))
        assert result.success is False and result.code == "schema.file_columns_differ"
        assert (await _stored(db, "file_glob")).file_glob is None


@pytest.mark.asyncio
async def test_a_file_glob_takes_the_right_to_edit_the_table(control_plane, tmp_path):
    async with _db(control_plane, source_type="files", table_ttl=60) as db:
        await _files_source(db, tmp_path, ("a.csv", "id\n1\n"), ("b.csv", "id\n2\n"))
        with pytest.raises(PermissionError, match="file_glob"):
            await _update(db, _table_input(file_glob="*.csv"), *_steward_only())
        assert (await _stored(db, "file_glob")).file_glob is None


# --- reading the columns from the live source -------------------------------------------------


async def _update_discovering(db, input_, discovered, *extra):
    from provisa.api.admin.schema_mutation import Mutation

    reads = AsyncMock(return_value=discovered)
    with ExitStack() as stack:
        for p in _table_patches(db):
            # The columns are built for real here: discovery is what is under test.
            if getattr(p, "attribute", None) == "_build_columns_for_input":
                continue
            stack.enter_context(p)
        stack.enter_context(
            patch("provisa.api.admin._table_ops._discover_columns_for_registration", new=reads)
        )
        stack.enter_context(
            patch(
                "provisa.api.admin._table_ops._ensure_source_column_types",
                new=AsyncMock(return_value=None),
            )
        )
        for p in extra:
            stack.enter_context(p)
        return await Mutation().update_table(MagicMock(), input_), reads


async def _columns(db):
    async with db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(table_columns.c.column_name, table_columns.c.data_type).order_by(
                    table_columns.c.id
                )
            )
        ).fetchall()
    return [(r.column_name, r.data_type) for r in rows]


_LIVE = [{"name": "id", "type": "integer"}, {"name": "note", "type": "varchar"}]


@pytest.mark.asyncio
async def test_reading_the_columns_adds_those_the_source_has_that_the_table_lacks(control_plane):
    from provisa.api.admin.types import ColumnInput

    async with _db(control_plane, source_type="mongodb", table_ttl=60) as db:
        given = [ColumnInput(name="id", data_type="integer", visible_to=["analyst"])]
        result, reads = await _update_discovering(
            db, _table_input(columns=given, discover=True), _LIVE
        )
        assert result.success is True, result.message
        reads.assert_awaited_once_with("s", "orders")
        # The column it had stays as it is; the one the source has besides is added.
        assert await _columns(db) == [("id", "integer"), ("note", "varchar")]


@pytest.mark.asyncio
async def test_columns_are_read_only_when_asked(control_plane):
    from provisa.api.admin.types import ColumnInput

    async with _db(control_plane, source_type="mongodb", table_ttl=60) as db:
        # Registered with its column: a save that keeps it as stored does not ask the source.
        async with db.acquire() as conn:
            table_id = (await conn.execute_core(select(registered_tables.c.id))).scalar_one()
            await conn.execute_core(
                table_columns.insert().values(
                    table_id=table_id,
                    domain_id="",
                    column_name="id",
                    data_type="integer",
                    visible_to=["analyst"],
                )
            )
        given = [ColumnInput(name="id", data_type="integer", visible_to=["analyst"])]
        result, reads = await _update_discovering(db, _table_input(columns=given), _LIVE)
        assert result.success is True, result.message
        reads.assert_not_awaited()
        assert await _columns(db) == [("id", "integer")]


@pytest.mark.asyncio
async def test_a_source_that_cannot_be_read_saves_nothing(control_plane):
    from provisa.api.admin.types import ColumnInput

    async with _db(control_plane, source_type="mongodb", table_ttl=60) as db:
        from provisa.api.admin.schema_mutation import Mutation

        with ExitStack() as stack:
            for p in _table_patches(db):
                if getattr(p, "attribute", None) == "_build_columns_for_input":
                    continue
                stack.enter_context(p)
            stack.enter_context(
                patch(
                    "provisa.api.admin._table_ops._discover_columns_for_registration",
                    new=AsyncMock(side_effect=ConnectionError("connection refused")),
                )
            )
            given = [ColumnInput(name="id", data_type="integer", visible_to=["analyst"])]
            result = await Mutation().update_table(
                MagicMock(), _table_input(columns=given, discover=True, description="edited")
            )
        assert result.success is False and result.code == "schema.discovery_failed"
        assert (await _stored(db, "description")).description is None


@pytest.mark.asyncio
async def test_reading_the_columns_takes_the_right_to_edit_the_table(control_plane):
    async with _db(control_plane, source_type="mongodb", table_ttl=60) as db:
        with pytest.raises(PermissionError, match="discover"):
            await _update_discovering(db, _table_input(discover=True), _LIVE, *_steward_only())
        assert await _columns(db) == []
