# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3b6f21-4a9c-4e07-b5d2-6c1f0e9a7b34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A config declares its calendars (REQ-2120).

A view in a config could name a calendar as its snapshot schedule, but the calendar itself could
only be made through the ``createCalendar`` mutation. A config's ``calendars:`` section declares
them; the load stores each before the tables, whose views name one; a ``views:`` entry takes the
snapshot-schedule keys a ``tables:`` entry does; and a calendar the config declares is refused
for exactly what the mutation refuses."""

# Requirements: REQ-2120, REQ-962

from __future__ import annotations

import contextlib
import datetime as dt

import pytest
from pydantic import ValidationError
from sqlalchemy import insert, select

import provisa.api.app  # noqa: F401  (says whether the deployment holds many organizations)
from provisa.api.admin.schema_mutation import Mutation
from provisa.api.admin.types import CalendarInput
from provisa.core import secrets_store
from provisa.core.config_loader import apply_config, parse_config_dict
from provisa.core.database import Database
from provisa.core.schema_org import calendars, metadata, registered_tables
from provisa.events.app_wiring import _load_calendar_registry


@pytest.fixture(autouse=True)
def no_vault(monkeypatch):
    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)


@pytest.fixture
async def db(control_plane) -> Database:
    plane = control_plane.org()
    # The built-in source views are registered on, which the startup seed creates.
    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(metadata.tables["sources"]).values(id="__derived__", type="postgresql")
        )
    return plane


_COLUMNS = [{"name": "id", "data_type": "integer", "visible_to": ["seller"]}]
_BANK = {
    "name": "bank",
    "version": "v1",
    "tz": "America/New_York",
    "day_close": "18:00",
    # YAML reads an unquoted date as a date.
    "holidays": [dt.date(2026, 12, 25)],
}


def _raw(*, calendars_: list[dict], calendar: str) -> dict:
    return {
        "sources": [
            {"id": "cfg", "type": "postgresql", "host": "h", "port": 5432, "database": "d"}
        ],
        "domains": [{"id": "sales"}],
        "roles": [
            {
                "id": "seller",
                "description": "Test role.",
                "capabilities": [],
                "domain_access": ["sales"],
            }
        ],
        "tables": [
            {
                "source_id": "cfg",
                "domain_id": "sales",
                "schema": "public",
                "table": "orders",
                "columns": _COLUMNS,
            }
        ],
        "calendars": calendars_,
        "views": [
            {
                "id": "daily-orders",
                "sql": "SELECT id FROM orders",
                "domain_id": "sales",
                "materialize": True,
                "calendar": calendar,
                "grain": "daily",
                "business_day_grain": True,
                "allowed_lateness": 3600,
                "columns": _COLUMNS,
            }
        ],
    }


async def _apply(db: Database, raw: dict) -> None:
    async with db.acquire() as conn:
        await apply_config(parse_config_dict(raw), conn)


async def test_a_declared_calendar_is_stored_and_the_view_naming_it_loads(db):
    await _apply(db, _raw(calendars_=[_BANK], calendar="bank"))

    async with db.acquire() as conn:
        stored = (await conn.execute_core(select(calendars))).fetchall()
        view = (
            await conn.execute_core(
                select(registered_tables).where(
                    registered_tables.c.table_name == "view_daily_orders"
                )
            )
        ).fetchone()
    assert [(r.name, r.version, r.tz, r.day_close, r.holidays) for r in stored] == [
        ("bank", "v1", "America/New_York", "18:00", ["2026-12-25"])
    ]
    assert view is not None
    assert (
        view.mv_calendar,
        view.mv_grain,
        view.mv_business_day_grain,
        view.mv_allowed_lateness,
    ) == ("bank", "daily", True, 3600)
    # The event loop's registry resolves the calendar the view names.
    cal = (await _load_calendar_registry(db)).get("bank")
    assert cal.day_close == dt.time(18, 0) and cal.holidays == frozenset({dt.date(2026, 12, 25)})


async def test_a_view_naming_an_undeclared_calendar_is_refused_by_name(db):
    await _apply(db, _raw(calendars_=[_BANK], calendar="ledger"))
    registry = await _load_calendar_registry(db)
    with pytest.raises(ValueError, match=r"unknown calendar 'ledger'; registered: \['bank'\]"):
        registry.get("ledger")


@pytest.mark.parametrize(
    "bad",
    [
        {"base_system": "lunar"},
        {"day_close": "25:00"},
        {"holidays": ["Christmas"]},
        {"retail_anchor": "2026-13-01"},
    ],
)
async def test_a_declared_calendar_is_refused_for_what_the_mutation_refuses(monkeypatch, bad):
    from tests.unit.gate_identity import grant

    info = grant(monkeypatch, "table_registration")[0]
    result = await Mutation().create_calendar(info, CalendarInput(name="bank", version="v1", **bad))
    assert result.success is False and result.code == "schema.invalid_calendar"

    with pytest.raises(ValidationError) as refused:
        parse_config_dict(
            _raw(calendars_=[{"name": "bank", "version": "v1", **bad}], calendar="bank")
        )
    assert result.message in str(refused.value)


def test_a_declared_calendar_needs_its_version():
    with pytest.raises(ValidationError, match="version"):
        parse_config_dict(_raw(calendars_=[{"name": "bank"}], calendar="bank"))
