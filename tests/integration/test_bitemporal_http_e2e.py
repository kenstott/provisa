# Copyright (c) 2026 Kenneth Stott
# Canary: dd840c5b-3783-4d24-bb7b-8fa3589d1a90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1162 + REQ-1163: HTTP end-to-end for a bitemporal materialized view.

Drives the full live path through the real app: register a MATERIALIZED bitemporal view over GraphQL
(config persists to the DB) and release it, which builds it — the first version in its append log;
change its row and refresh, the second version; then query it through /data/sql — current-by-default
and with the X-Provisa-As-Of header, before, between and after the two versions. The materialize store is pointed
at an embedded DuckDB file so the in-process test app needs no external store (the isolated Postgres
is on an ephemeral port, not :5432)."""

import asyncio
import os
import tempfile
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.helpers import registered_id, release_mutation

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")
    # Materialize into an embedded DuckDB file — no external store needed for the in-process app.
    _store = os.path.join(tempfile.mkdtemp(prefix="bt_mat_"), "mat.duckdb")
    # Restored below, not removed: the session exports a store (tests/conftest.py) that every
    # server started after this module inherits.
    _session_store = os.environ.get("PROVISA_MATERIALIZE_URL")
    os.environ["PROVISA_MATERIALIZE_URL"] = f"duckdb:///{_store}"
    try:
        from provisa.api.app import create_app

        app = create_app()
        async with app.router.lifespan_context(app):
            transport = ASGITransport(app=app)
            async with AsyncClient(
                # REQ-273: the header carries the role; a body role must match it.
                transport=transport,
                base_url="http://test",
                headers={"X-Provisa-Role": "org_admin"},
            ) as c:
                yield c
    finally:
        if _session_store is None:
            os.environ.pop("PROVISA_MATERIALIZE_URL", None)
        else:
            os.environ["PROVISA_MATERIALIZE_URL"] = _session_store


async def _admin(client, query: str):
    r = await client.post("/admin/graphql", json={"query": query})
    assert r.status_code == 200, r.text
    return r.json()


async def _sql(client, sql: str, **headers):
    return await client.post(
        "/data/sql", json={"sql": sql, "role": "org_admin"}, headers=headers or None
    )


def _view_input(amount: int, recorded: str, schema: str = "views") -> str:
    """The view's input, with its one row's amount. It registers the view; with another amount
    and the schema the view is STORED under it changes it (same columns, so its history is
    kept). Registering a materialized view stores it under the schema it lands in, not the one
    written here, and an existing table is addressed by what is stored -- as the admin UI does,
    which sends back the schema name it read (provisa-ui/src/pages/tables/helpers.ts). Its
    recorded time is ``recorded`` (REQ-2109): each version is entered at its own."""
    visible = 'visibleTo: ["org_admin", "analyst", "developer"]'
    return f"""input: {{
        sourceId: "__derived__",
        domainId: "bt",
        schemaName: "{schema}",
        tableName: "bt_view",
        alias: "bt_view",
        viewSql: "SELECT 1 AS id, {amount} AS amount, TIMESTAMP '2026-01-01 00:00:00' AS booked_at, TIMESTAMP '{recorded}' AS entered_at",
        materialize: true,
        businessKey: ["id"],
        validTime: "booked_at",
        recordedTime: "entered_at",
        columns: [
            {{ name: "id", {visible} }}, {{ name: "amount", {visible} }},
            {{ name: "booked_at", {visible} }}, {{ name: "entered_at", {visible} }}
        ]
    }}"""


async def _refresh(client, mv) -> None:
    """The refresh a user asks for: through the admin mutation, as the org the request is bound
    to. Called directly it runs with no org bound, is refused by name, and -- because a refresh
    never raises -- only logs (main's suite 37995232420)."""
    from provisa.mv.refresh import refresh_failure

    refreshed = await _admin(
        client, 'mutation { refreshMv(mvId: "view-bt_view") { success message } }'
    )
    assert refreshed["data"]["refreshMv"]["success"], refreshed
    assert refresh_failure(mv) is None


def _rows_and_columns(response) -> dict:
    body = response.json()
    return {"data": body["data"], "columns": body["columns"]}


async def test_bitemporal_view_http_end_to_end(client):
    await _admin(
        client, 'mutation { createDomain(input: { id: "bt", description: "x" }) { success } }'
    )

    reg = await _admin(
        client,
        f"mutation {{ registerTable({_view_input(10, '2026-01-01 00:00:00')}) "
        "{ success message } }",
    )
    assert reg["data"]["registerTable"]["success"], reg
    # REQ-1921: registered through the admin, the view starts as draft; released, it is built.
    released = await _admin(
        client, release_mutation(registered_id(reg["data"]["registerTable"]["message"]))
    )
    assert released["data"]["setTableDraft"]["success"], released

    # (1) Persistence round-trip: the bitemporal config survives GraphQL -> DB -> read-back.
    tables = await _admin(
        client,
        "query { tables { tableName schemaName businessKey validTime recordedTime } }",
    )
    row = next(t for t in tables["data"]["tables"] if t["tableName"] == "bt_view")
    assert (row["businessKey"], row["validTime"], row["recordedTime"]) == (
        ["id"],
        "booked_at",
        "entered_at",
    )

    # Rebuild so the view's read is wired to the reconstruction. The append log already holds the
    # first version: releasing the view built it. The refresh here finds nothing changed.
    rb = await _admin(client, "mutation { rebuildSchemas { success } }")
    assert rb["data"]["rebuildSchemas"]["success"], rb
    from provisa.api.app import state

    mv = state.mv_registry.get("view-bt_view")
    assert mv is not None and mv.bitemporal is not None  # REQ-1162: spec survived the reload
    assert mv.bitemporal.mode == "delta"
    await _refresh(client, mv)

    # (2) Current-by-default read through the real endpoint reconstructs current state from the log.
    cur = await _sql(client, 'SELECT id, amount FROM "bt"."bt_view"')
    assert cur.status_code == 200, cur.text
    # The rows and their columns are what is under test; the body also carries the engine's
    # column types (REQ-1937), which have their own tests.
    assert _rows_and_columns(cur) == {
        "data": {"sql": [{"id": 1, "amount": 10}]},
        "columns": ["id", "amount"],
    }, cur.text

    # (3) X-Provisa-As-Of after the first version → the reconstructed current row (time-travel path).
    fut = await _sql(
        client,
        'SELECT id, amount FROM "bt"."bt_view"',
        **{"X-Provisa-As-Of": "2999-01-01T00:00:00"},
    )
    assert fut.status_code == 200, fut.text
    # The rows and their columns are what is under test; the body also carries the engine's
    # column types (REQ-1937), which have their own tests.
    assert _rows_and_columns(fut) == {
        "data": {"sql": [{"id": 1, "amount": 10}]},
        "columns": ["id", "amount"],
    }, fut.text

    # (4) X-Provisa-As-Of BEFORE the first version → no version was effective yet → empty.
    past = await _sql(
        client,
        'SELECT id, amount FROM "bt"."bt_view"',
        **{"X-Provisa-As-Of": "2000-01-01T00:00:00"},
    )
    assert past.status_code == 200, past.text
    # REQ-1436: the projection rides alongside the rows, and is still present when the result is
    # empty — that is the case it exists for.
    assert _rows_and_columns(past) == {"data": {"sql": []}, "columns": ["id", "amount"]}, past.text

    # (5) A malformed X-Provisa-As-Of is rejected at the endpoint before execution → HTTP 400.
    bad = await _sql(
        client, 'SELECT id FROM "bt"."bt_view"', **{"X-Provisa-As-Of": "not-a-timestamp"}
    )
    assert bad.status_code == 400, bad.text
    assert "as-of" in bad.text.lower() or "as_of" in bad.text.lower()

    # (6) A second version, recorded on 1 June: the view's row changes (same columns, so the log is
    # kept and appended to) and the view is refreshed. The log now holds two versions of id 1,
    # which is what bitemporal has to mean.
    between = "2026-03-01T00:00:00"
    # The view is addressed by the schema it is stored under, read back above.
    changed_input = _view_input(20, "2026-06-01 00:00:00", row["schemaName"])
    changed = await _admin(
        client, f"mutation {{ updateTable({changed_input}) {{ success message }} }}"
    )
    assert changed["data"]["updateTable"]["success"], changed
    mv = state.mv_registry.get("view-bt_view")
    assert mv is not None and mv.bitemporal is not None
    await _refresh(client, mv)

    select = 'SELECT id, amount FROM "bt"."bt_view"'
    # Between the two versions: the first.
    then = await _sql(client, select, **{"X-Provisa-As-Of": between})
    assert then.status_code == 200, then.text
    assert _rows_and_columns(then)["data"] == {"sql": [{"id": 1, "amount": 10}]}, then.text
    # After the second, and by default: the changed row.
    for headers in ({"X-Provisa-As-Of": "2999-01-01T00:00:00"}, {}):
        now = await _sql(client, select, **headers)
        assert now.status_code == 200, now.text
        assert _rows_and_columns(now)["data"] == {"sql": [{"id": 1, "amount": 20}]}, now.text
    # And before the first there is still nothing: the change rewrote no history.
    past = await _sql(client, select, **{"X-Provisa-As-Of": "2000-01-01T00:00:00"})
    assert _rows_and_columns(past)["data"] == {"sql": []}, past.text


def _corrected_input(amount: int, schema: str = "views") -> str:
    """A view whose one row is valid from 30 September, with its amount: the same row corrected
    keeps that time. It is the view's valid time and, until reads move valid time to as_of,
    its recorded time too: the clock its as_of reads on (REQ-2109)."""
    visible = 'visibleTo: ["org_admin", "analyst", "developer"]'
    return f"""input: {{
        sourceId: "__derived__",
        domainId: "btc",
        schemaName: "{schema}",
        tableName: "bt_corrected",
        alias: "bt_corrected",
        viewSql: "SELECT 1 AS id, {amount} AS amount, TIMESTAMP '2026-09-30 00:00:00' AS valid_from",
        materialize: true,
        businessKey: ["id"],
        validTime: "valid_from",
        recordedTime: "valid_from",
        columns: [
            {{ name: "id", {visible} }}, {{ name: "amount", {visible} }},
            {{ name: "valid_from", {visible} }}
        ]
    }}"""


async def test_a_correction_is_read_as_it_was_recorded(client):
    """REQ-1163 (as recorded): a correction recorded later changes the view's past; a read as
    recorded before it answers what was said then."""
    await _admin(
        client, 'mutation { createDomain(input: { id: "btc", description: "x" }) { success } }'
    )
    reg = await _admin(
        client, f"mutation {{ registerTable({_corrected_input(10)}) {{ success message }} }}"
    )
    assert reg["data"]["registerTable"]["success"], reg
    released = await _admin(
        client, release_mutation(registered_id(reg["data"]["registerTable"]["message"]))
    )
    assert released["data"]["setTableDraft"]["success"], released
    tables = await _admin(client, "query { tables { tableName schemaName } }")
    schema = next(t for t in tables["data"]["tables"] if t["tableName"] == "bt_corrected")[
        "schemaName"
    ]
    rb = await _admin(client, "mutation { rebuildSchemas { success } }")
    assert rb["data"]["rebuildSchemas"]["success"], rb
    from provisa.api.app import state
    from provisa.mv.refresh import refresh_failure

    mv = state.mv_registry.get("view-bt_corrected")
    assert mv is not None and mv.bitemporal is not None

    async def refresh() -> None:
        done = await _admin(
            client, 'mutation { refreshMv(mvId: "view-bt_corrected") { success message } }'
        )
        assert done["data"]["refreshMv"]["success"], done
        assert refresh_failure(mv) is None

    await refresh()
    await asyncio.sleep(0.5)
    recorded_before = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    await asyncio.sleep(0.5)
    corrected = await _admin(
        client,
        f"mutation {{ updateTable({_corrected_input(20, schema)}) {{ success message }} }}",
    )
    assert corrected["data"]["updateTable"]["success"], corrected
    await refresh()

    select = 'SELECT id, amount FROM "btc"."bt_corrected"'
    valid = {"X-Provisa-As-Of": "2026-09-30T12:00:00"}

    async def amounts(**headers) -> list[int]:
        resp = await _sql(client, select, **headers)
        assert resp.status_code == 200, resp.text
        return [r["amount"] for r in resp.json()["data"]["sql"]]

    # As recorded before the correction: the original.
    assert await amounts(**valid, **{"X-Provisa-As-Recorded": recorded_before}) == [10]
    # As recorded since, and as of alone: the correction.
    assert await amounts(**valid, **{"X-Provisa-As-Recorded": "2999-01-01T00:00:00"}) == [20]
    assert await amounts(**valid) == [20]
    # As recorded alone: the version in force as recorded then.
    assert await amounts(**{"X-Provisa-As-Recorded": recorded_before}) == [10]
    # One time, not a range.
    bad = await _sql(
        client, select, **{"X-Provisa-As-Recorded": "2026-10-01T00:00:00/2026-10-05T00:00:00"}
    )
    assert bad.status_code == 400, bad.text
    assert "data.invalid_as_recorded" in bad.text, bad.text
