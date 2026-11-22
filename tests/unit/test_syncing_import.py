# Copyright (c) 2026 Kenneth Stott
# Canary: 5b1d7e83-2c49-4f06-a8e3-9d0c6b2f7a14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2247, REQ-2122: a Syncing entry's scheduled Import applies the steward's standing choices.

A run nobody watches takes the catalog's value where the standing choice says so and the catalog
alone moved off the value last exchanged; it holds every other difference Import lists (no choice,
a hold, a value edited in Provisa since the last exchange, one never exchanged, a kind Provisa
cannot take yet) and records each run with what it applied and held. A direction that is off never
runs, and only an entry that imports on a schedule has a job armed.
"""

# Requirements: REQ-2247, REQ-2122

from __future__ import annotations

import pytest

from provisa.api.admin import syncing_import as si
from provisa.api.errors import ApiError
from provisa.api.metadata_export import sync_values as sv
from provisa.core.models import CatalogConnection, SyncDirectionOff
from provisa.core.repositories import catalog_exchange, catalog_standing_choice, catalog_sync_run
from tests.unit.test_syncing_admin_surface import (  # noqa: F401 - the shared fixtures
    ORG_ID,
    _catalog,
    _served,
    surface,
)
from tests.unit.test_syncing_review import (  # noqa: F401 - the shared fixtures
    GROUP,
    _Catalog,
    _catalog_values,
    _conn,
    _latest,
    _register_orders,
    _review,
    _stored,
    entry,
)

COL_ID = "wh.public.orders.id"


async def _choose(kind: str, choice: str, direction: str = "import") -> None:
    async with _conn() as conn:
        await catalog_standing_choice.set_choice(
            conn, "mz", direction, kind, choice, decided_by="alice"
        )


async def _exchanged(*values: tuple[str, str, object], direction: str = "import") -> None:
    async with _conn() as conn:
        await catalog_exchange.record(conn, "mz", direction, values, decided_by="alice")  # type: ignore[arg-type]


async def _runs() -> list[catalog_sync_run.SyncRun]:
    async with _conn() as conn:
        return await catalog_sync_run.runs(conn, "mz", "import")


async def _table_description():
    from provisa.core.schema_org import registered_tables

    return await _stored(registered_tables, "description", id=11)


@pytest.mark.asyncio
async def test_a_scheduled_import_takes_the_description_and_holds_the_owner(entry):  # noqa: F811
    """The REQ-2247 scenario: both were last exchanged as Provisa holds them, the catalog changed
    both; the standing choice takes descriptions from the catalog, owners have none."""
    await _register_orders()
    await _exchanged((sv.DESCRIPTION, GROUP, "Orders placed"), (sv.OWNER, GROUP, ("alice",)))
    await _choose(sv.DESCRIPTION, catalog_standing_choice.CATALOG)
    entry.catalog.values = _catalog_values(
        description__wh_public_orders="Orders, as revised in the catalog",
        owner__wh_public_orders=("bob",),
    )

    body = await si.import_job(ORG_ID, "mz")

    assert body is not None
    assert body["applied"] == [{"kind": sv.DESCRIPTION, "object": GROUP}]
    assert body["held"] == [{"kind": sv.OWNER, "object": GROUP, "reason": si.NO_CHOICE}]
    assert await _table_description() == "Orders, as revised in the catalog"
    last = (await _latest())[(sv.DESCRIPTION, GROUP)]
    assert (last.direction, last.value, last.decided_by) == (
        "import",
        "Orders, as revised in the catalog",
        si.STANDING_CHOICE,
    )
    # The owner was not exchanged, and waits in the list.
    assert (await _latest())[(sv.OWNER, GROUP)].value == ("alice",)
    assert [k["kind"] for k in (await _review())["kinds"]] == [sv.DESCRIPTION, sv.OWNER]


@pytest.mark.asyncio
async def test_the_run_is_recorded_with_its_outcome_and_what_it_applied_and_held(entry):  # noqa: F811
    await _register_orders()
    await _exchanged((sv.DESCRIPTION, GROUP, "Orders placed"), (sv.OWNER, GROUP, ("alice",)))
    await _choose(sv.DESCRIPTION, catalog_standing_choice.CATALOG)
    await _choose(sv.OWNER, catalog_standing_choice.HOLD)
    entry.catalog.values = _catalog_values(
        description__wh_public_orders="Revised", owner__wh_public_orders=("bob",)
    )

    await si.import_job(ORG_ID, "mz")

    (run,) = await _runs()
    assert (run.entry, run.direction, run.trigger, run.outcome, run.error) == (
        "mz",
        "import",
        catalog_sync_run.SCHEDULE,
        catalog_sync_run.OK,
        None,
    )
    assert run.applied == [{"kind": sv.DESCRIPTION, "object": GROUP}]
    assert run.held == [{"kind": sv.OWNER, "object": GROUP, "reason": si.HELD_BY_CHOICE}]
    assert run.started_at <= run.finished_at


@pytest.mark.asyncio
async def test_a_value_edited_in_provisa_since_the_last_exchange_is_not_overwritten(entry):  # noqa: F811
    """REQ-2122: Provisa's description moved off the value last exchanged, and so did the
    catalog's; the standing choice says take the catalog's, and the run holds it instead."""
    await _register_orders()
    await _exchanged((sv.DESCRIPTION, GROUP, "What both once held"))
    await _choose(sv.DESCRIPTION, catalog_standing_choice.CATALOG)
    entry.catalog.values = _catalog_values(description__wh_public_orders="The catalog's edit")

    body = await si.import_job(ORG_ID, "mz")

    assert body is not None
    assert body["applied"] == []
    assert body["held"] == [
        {"kind": sv.DESCRIPTION, "object": GROUP, "reason": si.EDITED_IN_PROVISA}
    ]
    assert await _table_description() is None, "Provisa's description is not written"
    assert (await _latest())[(sv.DESCRIPTION, GROUP)].value == "What both once held"


@pytest.mark.asyncio
async def test_a_value_never_exchanged_is_held(entry):  # noqa: F811
    """Which side moved is unknown, so the run cannot tell Provisa's value is not a local edit."""
    from provisa.core.schema_org import table_columns

    await _register_orders()
    await _choose(sv.COLUMN_DESCRIPTION, catalog_standing_choice.CATALOG)
    entry.catalog.values = _catalog_values(column_description__wh_public_orders_id="The id")

    body = await si.import_job(ORG_ID, "mz")

    assert body is not None
    assert body["held"] == [
        {"kind": sv.COLUMN_DESCRIPTION, "object": COL_ID, "reason": si.NEVER_EXCHANGED}
    ]
    assert await _stored(table_columns, "description", table_id=11, column_name="id") is None
    assert await _latest() == {}


@pytest.mark.asyncio
async def test_a_kind_provisa_cannot_take_yet_is_held_by_name(entry):  # noqa: F811
    await _register_orders()
    await _exchanged((sv.TAG, GROUP, ()))
    await _choose(sv.TAG, catalog_standing_choice.CATALOG)
    entry.catalog.values = _catalog_values(tag__wh_public_orders=("PII.Sensitive",))

    body = await si.import_job(ORG_ID, "mz")

    assert body is not None
    assert body["held"] == [{"kind": sv.TAG, "object": GROUP, "reason": si.NOT_IMPORTABLE}]
    assert (await _latest())[(sv.TAG, GROUP)].value == ()


@pytest.mark.asyncio
async def test_a_standing_choice_of_provisas_value_changes_nothing_on_import(entry):  # noqa: F811
    await _register_orders()
    await _exchanged((sv.DESCRIPTION, GROUP, "Orders placed"))
    await _choose(sv.DESCRIPTION, catalog_standing_choice.PROVISA)
    entry.catalog.values = _catalog_values(description__wh_public_orders="The catalog's edit")

    body = await si.import_job(ORG_ID, "mz")

    assert body is not None
    assert (body["applied"], body["held"]) == ([], [])
    assert body["left"] == [{"kind": sv.DESCRIPTION, "object": GROUP, "reason": si.LEFT_TO_EXPORT}]
    assert await _table_description() is None
    assert entry.catalog.published == 0, "Import never changes the catalog"


@pytest.mark.asyncio
async def test_a_difference_only_provisa_moved_is_not_imports_to_decide(entry):  # noqa: F811
    await _register_orders()
    await _exchanged((sv.DESCRIPTION, GROUP, "The catalog's text"), direction="export")
    await _choose(sv.DESCRIPTION, catalog_standing_choice.CATALOG)
    entry.catalog.values = _catalog_values(description__wh_public_orders="The catalog's text")

    body = await si.import_job(ORG_ID, "mz")

    assert body is not None
    assert (body["applied"], body["held"], body["left"]) == ([], [], [])
    assert await _table_description() is None


@pytest.mark.asyncio
async def test_a_direction_that_is_off_applies_nothing_and_records_no_run(entry):  # noqa: F811
    await _register_orders()
    await _exchanged((sv.DESCRIPTION, GROUP, "Orders placed"))
    await _choose(sv.DESCRIPTION, catalog_standing_choice.CATALOG)
    entry.settings["catalogs"] = [
        _catalog(provider="openmetadata", role="steward", direction="export")
    ]
    entry.catalog.values = _catalog_values(description__wh_public_orders="The catalog's edit")

    assert await si.import_job(ORG_ID, "mz") is None
    with pytest.raises(SyncDirectionOff):
        await si.run_import(ORG_ID, "mz", trigger=catalog_sync_run.MANUAL)
    assert await _table_description() is None
    assert (await _latest())[(sv.DESCRIPTION, GROUP)].value == "Orders placed"
    assert await _runs() == []


@pytest.mark.asyncio
async def test_a_refused_read_is_recorded_as_a_failed_run(entry, monkeypatch):  # noqa: F811
    import httpx

    import provisa.api.admin.syncing_router as mod

    class _Refused(_Catalog):
        async def read(self, snapshot):
            request = httpx.Request("GET", "https://catalog.example/api/v1/tables/name/x")
            raise httpx.HTTPStatusError(
                "403", request=request, response=httpx.Response(403, request=request)
            )

    monkeypatch.setattr(mod, "catalog_reader", lambda target: _Refused({}))
    with pytest.raises(ApiError):
        await si.import_job(ORG_ID, "mz")
    (run,) = await _runs()
    assert (run.outcome, run.applied, run.held) == (catalog_sync_run.FAILED, [], [])
    assert run.error is not None and "reading 'mz' failed" in run.error


# --- the schedule ------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_only_an_entry_that_imports_on_a_schedule_has_a_job_armed(surface):  # noqa: F811
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    scheduler = AsyncIOScheduler()
    surface.settings["catalogs"] = [
        _catalog(id="mz", import_cron="0 3 * * *"),
        _catalog(id="sends", direction="export"),
        _catalog(id="unscheduled"),
    ]
    await si.register_import_jobs(scheduler, ORG_ID)
    assert [j.id for j in scheduler.get_jobs()] == [si.import_job_id(ORG_ID, "mz")]
    (job,) = scheduler.get_jobs()
    assert (job.func, job.args) == (si.import_job, (ORG_ID, "mz"))

    # Saving the entry without its schedule disarms it.
    surface.settings["catalogs"] = [_catalog(id="mz")]
    await si.register_import_jobs(scheduler, ORG_ID)
    assert scheduler.get_jobs() == []


def test_an_entry_that_does_not_import_has_no_import_schedule():
    with pytest.raises(ValueError, match="does not import, so it has no import schedule"):
        CatalogConnection(**_catalog(direction="export", import_cron="0 3 * * *"))
    with pytest.raises(ValueError):
        CatalogConnection(**_catalog(import_cron="not a cron"))
    assert CatalogConnection(**_catalog(import_cron="0 3 * * *")).import_cron == "0 3 * * *"


# --- Run import now and the last run (the page) ------------------------------------------------


async def _run_now(org_request=None) -> dict:
    from provisa.api.admin.syncing_router import import_entry
    from tests.unit.test_syncing_admin_surface import _request

    return await import_entry(org_request or _request(), "mz")


@pytest.mark.asyncio
async def test_run_import_now_runs_once_and_records_a_run_started_by_hand(entry):  # noqa: F811
    await _register_orders()
    await _exchanged((sv.DESCRIPTION, GROUP, "Orders placed"))
    await _choose(sv.DESCRIPTION, catalog_standing_choice.CATALOG)
    entry.catalog.values = _catalog_values(description__wh_public_orders="Revised")

    body = await _run_now()

    assert body["applied"] == [{"kind": sv.DESCRIPTION, "object": GROUP}]
    (run,) = await _runs()
    assert (run.trigger, run.outcome) == (catalog_sync_run.MANUAL, catalog_sync_run.OK)
    assert body["last_import"]["trigger"] == catalog_sync_run.MANUAL
    assert body["last_import"]["applied"] == [{"kind": sv.DESCRIPTION, "object": GROUP}]
    assert await _table_description() == "Revised"


@pytest.mark.asyncio
async def test_run_import_now_is_refused_without_the_org_settings_right(entry, monkeypatch):  # noqa: F811
    import provisa.api.admin.capabilities as capmod

    monkeypatch.setattr(capmod, "_resolved_capabilities", lambda identity, state: set())
    with pytest.raises(ApiError) as exc:
        await _run_now()
    assert exc.value.status_code == 403
    assert await _runs() == []


@pytest.mark.asyncio
async def test_run_import_now_on_an_entry_that_does_not_import_is_refused(entry):  # noqa: F811
    entry.settings["catalogs"] = [
        _catalog(provider="openmetadata", role="steward", direction="export")
    ]
    with pytest.raises(ApiError) as exc:
        await _run_now()
    assert (exc.value.status_code, exc.value.code) == (400, "syncing.direction_off")
    assert await _runs() == []


@pytest.mark.asyncio
async def test_the_read_returns_each_entrys_last_import_run(entry):  # noqa: F811
    from tests.unit.test_syncing_admin_surface import _get

    (before,) = (await _get())["entries"]
    assert before["last_import"] is None

    await _register_orders()
    await _exchanged((sv.DESCRIPTION, GROUP, "Orders placed"), (sv.OWNER, GROUP, ("alice",)))
    await _choose(sv.DESCRIPTION, catalog_standing_choice.CATALOG)
    entry.catalog.values = _catalog_values(
        description__wh_public_orders="First", owner__wh_public_orders=("bob",)
    )
    await si.import_job(ORG_ID, "mz")
    entry.catalog.values = _catalog_values(
        description__wh_public_orders="Second", owner__wh_public_orders=("bob",)
    )
    await _run_now()

    (after,) = (await _get())["entries"]
    last = after["last_import"]
    assert (last["trigger"], last["outcome"], last["error"]) == (
        catalog_sync_run.MANUAL,
        catalog_sync_run.OK,
        None,
    )
    assert last["applied"] == [{"kind": sv.DESCRIPTION, "object": GROUP}]
    assert last["held"] == [{"kind": sv.OWNER, "object": GROUP, "reason": si.NO_CHOICE}]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("over", "code"),
    [
        ({"direction": "export"}, "syncing.import_cron_not_importing"),
        ({"import_cron": "not a cron"}, "syncing.import_cron_invalid"),
    ],
)
async def test_a_refused_import_schedule_names_the_field(surface, over, code):  # noqa: F811
    from tests.unit.test_syncing_admin_surface import _put

    with pytest.raises(ApiError) as exc:
        await _put([_catalog(**{"import_cron": "0 3 * * *", **over})])
    assert (exc.value.status_code, exc.value.code) == (400, code)
    assert exc.value.params["field"] == "import_cron"
    assert exc.value.params["entry"] == "mz"
