# Copyright (c) 2026 Kenneth Stott
# Canary: 3cfa5db0-2230-42fe-aefa-6262040ecfed
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An upsert of the row a landed table already holds is no change, and fires no trigger
(REQ-1733, REQ-2054).

A table its source's CDC topic follows is built from the database; its listener starts where the
build stopped reading the topic, taken before the read. A change made while the build read the
database is in the build's copy and is then applied once more by key: the row it writes is the
row the table holds. That application changed nothing, so it is no change to tell the table's
views, subscriptions or triggers of. Every path a listener's batch is applied through reports
it, comparing the row as the store types it, before and after."""

# Requirements: REQ-1733, REQ-2054

from __future__ import annotations

import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import duckdb
import pytest

_COLUMNS = [("id", "bigint"), ("status", "text"), ("placed_at", "timestamp")]
_PLACED = "2026-10-17T09:30:00"


def _ev(operation: str, status: str, placed_at: str = _PLACED, id_: int = 1) -> SimpleNamespace:
    return SimpleNamespace(
        operation=operation, row={"id": id_, "status": status, "placed_at": placed_at}
    )


# A change the build copied, then the same change from the topic: unchanged. Then a real change,
# and a key the table did not hold.
_REPLAYED = [_ev("insert", "new"), _ev("update", "paid"), _ev("insert", "new", id_=2)]


def _assert_reported(counts: dict) -> None:
    assert counts["unchanged"] == [0]
    assert counts["upsert"] == 2 and counts["delete"] == 0


def test_the_duckdb_store_reports_an_upsert_of_the_row_it_holds():
    from provisa.federation.store_connection import apply_cdc_duckdb_native

    with tempfile.TemporaryDirectory() as d:
        con = duckdb.connect()
        con.execute(f"ATTACH '{Path(d) / 'store.duckdb'}' AS mat_store")
        try:

            def _apply(events):
                return apply_cdc_duckdb_native(
                    con,
                    catalog="mat_store",
                    schema="mat",
                    table="orders",
                    columns=_COLUMNS,
                    pk_columns=["id"],
                    events=events,
                    report_unchanged=True,
                )

            _apply([_ev("insert", "new")])  # the build's copy
            counts = _apply([_ev("insert", "new"), _ev("insert", "x", id_=2)])
            assert counts["unchanged"] == [0] and counts["upsert"] == 1
            counts = _apply([_ev("update", "paid")])
            assert counts["unchanged"] == [] and counts["upsert"] == 1
            rows = con.execute('SELECT id, status FROM mat_store.mat."orders" ORDER BY id')
            assert rows.fetchall() == [(1, "paid"), (2, "x")]
        finally:
            con.close()


def test_the_sqlalchemy_store_reports_an_upsert_of_the_row_it_holds(tmp_path):
    from sqlalchemy import create_engine

    from provisa.federation.materialize_exec import build_table, temporal_columns
    from provisa.federation.sqlalchemy_runtime import _apply_events

    engine = create_engine(f"sqlite:///{tmp_path / 'store.db'}")
    tbl = build_table("", "orders", _COLUMNS, ("id",), dialect_name="sqlite")
    temporal = temporal_columns(_COLUMNS)

    def _apply(events):
        with engine.begin() as conn:
            return _apply_events(conn, "", tbl, ["id"], events, temporal, report_unchanged=True)

    _apply([_ev("insert", "new")])
    _assert_reported(_apply(_REPLAYED))
    engine.dispose()


@pytest.mark.asyncio
async def test_the_store_write_face_reports_an_upsert_of_the_row_it_holds(control_plane):
    from sqlalchemy.schema import CreateSchema

    from provisa.federation.materialize_exec import apply_cdc, build_table
    from provisa.federation.store_writer import store_connection

    tbl = build_table("cdc_store", "orders", _COLUMNS, ("id",))
    async with store_connection(control_plane.url) as conn:
        await conn.execute_core(CreateSchema("cdc_store", if_not_exists=True))
        await apply_cdc(conn, tbl, ["id"], [_ev("insert", "new")])
        _assert_reported(await apply_cdc(conn, tbl, ["id"], _REPLAYED, report_unchanged=True))


def test_the_postgresql_engine_reports_an_upsert_of_the_row_it_holds(control_plane):
    import psycopg2

    from provisa.federation.pg_runtime import PgFederationRuntime

    runtime = PgFederationRuntime.__new__(PgFederationRuntime)
    con = psycopg2.connect(control_plane.pg.dsn(control_plane.name))
    con.autocommit = True
    try:
        cur = con.cursor()
        cur.execute('CREATE SCHEMA IF NOT EXISTS "cdc_engine"')

        def _apply(events):
            return runtime._apply_events(
                cur, "cdc_engine", "orders", _COLUMNS, ["id"], events, report_unchanged=True
            )

        _apply([_ev("insert", "new")])
        _assert_reported(_apply(_REPLAYED))
    finally:
        con.close()


class _Engine:
    def __init__(self, counts: dict) -> None:
        self.counts = counts

    async def apply_cdc_events(self, **kwargs) -> dict:
        return dict(self.counts)


async def _land(counts: dict, batch: list) -> tuple[list, list, list]:
    """What the listener tells of one landed batch: the ripples posted, the subscriptions told,
    and the changes the table's triggers are given."""
    from provisa.events import push_wiring

    rippled, told, triggered = [], [], []

    async def _consume(provider, land_fn, **kwargs):
        return await land_fn(batch)

    async def _ripple(state, node, **kwargs):
        rippled.append(kwargs["payload"])

    with (
        patch("provisa.subscriptions.cdc_landing.consume_cdc_into_store", _consume),
        patch("provisa.events.ripple.ripple", _ripple),
        patch("provisa.subscriptions.landed.landed", lambda *key: told.append(key)),
        patch(
            "provisa.scheduler.table_triggers.table_changed",
            lambda state, key, changes: triggered.append(changes),
        ),
    ):
        await push_wiring._run_listener(
            state=SimpleNamespace(),
            engine=_Engine(counts),
            provider=object(),
            watch_target="shop.public.orders",
            land_schema="org_a_replicas",
            land_table="shop_db__public__orders",
            columns=_COLUMNS,
            pk_columns=["id"],
            disconnect=threading.Event(),
            debounce_quiet=0.0,
            debounce_max_delay=5.0,
            node="shop_db/public.orders",
            log=SimpleNamespace(exception=lambda *a, **k: None, error=lambda *a, **k: None),
            key=("shop_db", "public", "orders"),
        )
    return rippled, told, triggered


@pytest.mark.asyncio
async def test_a_trigger_is_given_only_the_changes_that_changed_the_table():
    rippled, told, triggered = await _land(
        {"upsert": 2, "delete": 0, "unchanged": [0]}, list(_REPLAYED)
    )
    assert triggered == [[{"operation": e.operation, "row": e.row} for e in _REPLAYED[1:]]]
    assert rippled == [{"landed": {"upsert": 2, "delete": 0}}] and len(told) == 1


@pytest.mark.asyncio
async def test_a_batch_of_changes_the_table_already_held_tells_no_one():
    rippled, told, triggered = await _land(
        {"upsert": 0, "delete": 0, "unchanged": [0]}, [_ev("insert", "new")]
    )
    assert (rippled, told, triggered) == ([], [], [])
