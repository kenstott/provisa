# Copyright (c) 2026 Kenneth Stott
# Canary: cbd1bd57-2ad9-445f-a478-adb3419eb708
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code command's output columns are read from its function's declaration on every save
(REQ-2226); the governance the Columns tab sets on them (REQ-1159, REQ-1679, REQ-2059) is kept by
column name across that re-read. A column the declaration no longer has loses its governance; a
renamed one is a new column, with none."""

# Requirements: REQ-2226, REQ-1159, REQ-1679, REQ-2059

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import insert

import provisa.api.app as appmod
from provisa.api.admin import actions_router
from provisa.codesource import commands as commands_mod
from provisa.core import domain_policy
from provisa.core.database import Database
from provisa.core.repositories import function as function_repo
from provisa.core.schema_org import domains, sources

ROLES = {
    "everywhere": {
        "id": "everywhere",
        "capabilities": ["table_registration"],
        "domain_access": ["*"],
    },
}
MASK = {"to": None, "mask": {"type": "regex", "pattern": ".", "replace": "*"}}
GOVERNED = {
    "visible_to": ["everywhere"],
    "masks": [MASK],
    "alias": "full_address",
    "description": "The cleaned address",
}


@pytest.fixture
async def plane(monkeypatch, control_plane) -> Database:
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="addresses", type="python"))
        await conn.execute_core(insert(domains).values(id="sales"))
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "model_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "roles", ROLES, raising=False)
    monkeypatch.setattr(appmod.state, "source_types", {"addresses": "python"}, raising=False)
    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)

    async def _rebuild() -> None:
        return None

    monkeypatch.setattr(appmod, "_rebuild_schemas", _rebuild)
    return db


def _declares(monkeypatch, outputs: list[dict]) -> None:
    """The source's ``clean_address`` @query declares ``outputs`` as its output columns."""

    async def registration(_state, source_id, function):
        assert (source_id, function) == ("addresses", "clean_address")
        return "query", [{"name": "raw", "type": "text"}], [dict(c) for c in outputs]

    monkeypatch.setattr(commands_mod, "registration", registration)


def _request() -> Any:
    return types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(
                user_id="u1", roles=["everywhere"], acting_roles=["everywhere"]
            ),
            active_org_id="a",
            role="everywhere",
        )
    )


def _form(output_columns: list[dict]) -> actions_router.FunctionInput:
    """The command form's save: what it loaded, with the Columns tab's edits."""
    return actions_router.FunctionInput(
        name="clean_address",
        sourceId="addresses",
        functionName="clean_address",
        domainId="sales",
        outputColumns=output_columns,
    )


async def _stored_columns(db: Database) -> list[dict]:
    async with db.acquire() as conn:
        row = await function_repo.get_function(conn, "clean_address")
    assert row is not None
    return row["output_columns"]


def _governance(column: dict) -> dict:
    return {k: column.get(k) for k in GOVERNED}


async def _governed(plane, monkeypatch) -> list[dict]:
    """Register the command, then set governance on ``address`` through the Columns tab's save."""
    declared = [{"name": "address", "type": "text"}, {"name": "zip", "type": "text"}]
    _declares(monkeypatch, declared)
    await actions_router.create_function(_request(), _form([]))
    loaded = await _stored_columns(plane)
    edited = [{**c, **GOVERNED} if c["name"] == "address" else c for c in loaded]
    await actions_router.update_function(_request(), "clean_address", _form(edited))
    return await _stored_columns(plane)


async def test_the_columns_tabs_governance_is_saved_on_a_code_commands_output_column(
    plane, monkeypatch
):
    stored = await _governed(plane, monkeypatch)
    assert [c["name"] for c in stored] == ["address", "zip"]
    assert _governance(stored[0]) == GOVERNED


async def test_re_saving_a_code_command_keeps_its_output_columns_governance(plane, monkeypatch):
    loaded = await _governed(plane, monkeypatch)
    await actions_router.update_function(_request(), "clean_address", _form(loaded))
    stored = await _stored_columns(plane)
    assert [(c["name"], c["type"]) for c in stored] == [("address", "text"), ("zip", "text")]
    assert _governance(stored[0]) == GOVERNED
    assert not any(_governance(stored[1]).values())


async def test_a_column_the_declaration_drops_or_renames_loses_its_governance(plane, monkeypatch):
    loaded = await _governed(plane, monkeypatch)
    # The function now returns ``street`` (``address`` renamed) and a new ``city``; ``zip`` is gone.
    _declares(monkeypatch, [{"name": "street", "type": "text"}, {"name": "city", "type": "text"}])
    zip_governed = [
        {**c, "description": "Postal code"} if c["name"] == "zip" else c for c in loaded
    ]
    await actions_router.update_function(_request(), "clean_address", _form(zip_governed))
    stored = await _stored_columns(plane)
    assert stored == [{"name": "street", "type": "text"}, {"name": "city", "type": "text"}]


async def test_the_declared_type_wins_over_the_type_the_form_sent(plane, monkeypatch):
    loaded = await _governed(plane, monkeypatch)
    _declares(monkeypatch, [{"name": "address", "type": "varchar"}])
    await actions_router.update_function(_request(), "clean_address", _form(loaded))
    stored = await _stored_columns(plane)
    assert [(c["name"], c["type"]) for c in stored] == [("address", "varchar")]
    assert _governance(stored[0]) == GOVERNED


async def test_a_column_transform_keeps_its_output_columns_governance_the_same_way(monkeypatch):
    """REQ-2131: a ``@columns`` command's output columns are re-read from its declaration too."""
    from provisa.codesource import columns as columns_mod

    async def registration(_state, source_id, function):
        assert (source_id, function) == ("addresses", "clean_address")
        return [{"name": "raw", "type": "text"}], [
            {"name": "address", "type": "text"},
            {"name": "street", "type": "text"},
        ]

    monkeypatch.setattr(appmod.state, "source_types", {"addresses": "python"}, raising=False)
    monkeypatch.setattr(columns_mod, "registration", registration)
    body = _form(
        [
            {"name": "address", "type": "text", **GOVERNED},
            {"name": "zip", "type": "text", "description": "Postal code"},
        ]
    )
    await actions_router._as_column_transform(body)
    assert body.outputColumns == [
        {"name": "address", "type": "text", **GOVERNED},
        {"name": "street", "type": "text"},
    ]
