# Copyright (c) 2026 Kenneth Stott
# Canary: a41c713d-7501-459b-9f4c-a63cd8c341d5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-2109: a bitemporal table updated in place (its business key is its primary key) keeps its
versions in a log Provisa captures on its recorded time. The captures and the reads are run here
against real rows (DuckDB) so their answers are checked, not their text."""

# Requirements: REQ-2109

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pytest

from provisa.mv import bitemporal_capture as capture
from provisa.mv.bitemporal import AsOf, append_sql, create_sql, request_as_of, view_read_sql
from provisa.mv.bitemporal_tables import versioned_table_rows
from provisa.mv.registry import MVRegistry

pytestmark = pytest.mark.asyncio

_COLUMNS = ["payment_id", "business_date", "updated_at", "amount"]
_LOG = '"store"."main"."payments__versions_7"'


def _row(primary_key: list[str]) -> dict:
    return {
        "id": 7,
        "source_id": "pg",
        "schema_name": "fin",
        "table_name": "payments",
        "view_sql": None,
        "region": None,
        "draft": False,
        "business_key": ["payment_id"],
        "valid_time": "business_date",
        "recorded_time": "updated_at",
        "columns": [{"column_name": c, "is_primary_key": c in primary_key} for c in _COLUMNS],
    }


async def test_only_a_table_whose_key_is_its_primary_key_keeps_a_captured_log():
    assert set(capture.captured_table_rows([_row(["payment_id"])])) == {7}
    assert capture.captured_table_rows([_row(["row_id"])]) == {}
    # and it is not read as a table whose rows are its versions
    assert versioned_table_rows([_row(["payment_id"])]) == {}


@pytest.fixture
def con():
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS pg")
    con.execute("ATTACH ':memory:' AS store")
    con.execute("CREATE SCHEMA pg.fin")
    con.execute(
        "CREATE TABLE pg.fin.payments (payment_id TEXT PRIMARY KEY, business_date DATE, "
        "updated_at TIMESTAMP, amount INT)"
    )
    yield con
    con.close()


def _mv(lateness: float = 0.0):
    table = capture.captured_table_rows([_row(["payment_id"])])[7]
    mv = capture.log_mv(
        _row(["payment_id"]),
        table,
        target_catalog="store",
        target_schema="main",
        catalog_qualified=True,
        refresh_interval=300,
    )
    mv.allowed_lateness = lateness
    return mv


class _Engine:
    def __init__(self, con):
        self.con = con

    async def execute_engine(self, sql, authorization=None):
        cur = self.con.execute(sql)
        return SimpleNamespace(column_names=[d[0] for d in cur.description or []])


@pytest.fixture
def engine(con, monkeypatch):
    from provisa.mv import governed_build, refresh

    async def _build_sql(mv, _engine):
        return mv.semantic_sql  # the governed build of the administrator's read, as written

    async def _store(_engine, sql, _auth):
        return con.execute(sql).fetchall()

    monkeypatch.setattr(governed_build, "view_build_sql", _build_sql)
    monkeypatch.setattr(refresh, "_store_statement", _store)
    return _Engine(con)


def _columns(con) -> list[str] | None:
    try:
        return [d[0] for d in con.execute(f"SELECT * FROM {_LOG} LIMIT 0").description]
    except duckdb.CatalogException:
        return None


async def _capture(con, engine, at: str, *, full: bool = False, lateness: float = 0.0) -> None:
    """One capture of the log, stamped ``at``, as the refresh runs it."""
    mv = _mv(lateness)
    existing = _columns(con)
    build = await capture.capture_build(engine, mv, None, existing, full=full)
    assert build.select_sql is not None
    now = f"TIMESTAMP '{at}'"
    if existing is None:
        con.execute(create_sql(_LOG, build.select_sql, mv.bitemporal, now))
        return
    for stmt in append_sql(
        _LOG,
        build.select_sql,
        mv.bitemporal,
        _COLUMNS,
        now,
        "duckdb",
        scope=build.scope_sql(mv.bitemporal, "duckdb"),
    ):
        con.execute(stmt)


def _read(con, as_of=None, as_recorded=None) -> dict[str, int]:
    travel = request_as_of(as_of, as_recorded)
    spec = _mv().bitemporal
    sql = view_read_sql(_LOG, spec, AsOf.from_key(travel) if travel else None)
    return dict(con.execute(f"SELECT payment_id, amount FROM ({sql}) t").fetchall())


async def test_a_capture_appends_what_changed_since_the_latest_recorded_time(con, engine):
    con.execute(
        "INSERT INTO pg.fin.payments VALUES ('p1', '2026-10-03', '2026-10-03 18:00:00', 100), "
        "('p2', '2026-10-04', '2026-10-04 18:00:00', 50)"
    )
    await _capture(con, engine, "2026-10-04 20:00:00")
    # corrected in place on 5 Oct
    con.execute(
        "UPDATE pg.fin.payments SET amount = 120, updated_at = '2026-10-05 09:00:00' "
        "WHERE payment_id = 'p1'"
    )
    await _capture(con, engine, "2026-10-05 10:00:00")
    await _capture(con, engine, "2026-10-05 11:00:00")  # nothing changed: nothing appended
    assert con.execute(f"SELECT COUNT(*) FROM {_LOG}").fetchone() == (3,)
    assert _read(con) == {"p1": 120, "p2": 50}
    assert _read(con, as_of="2026-10-03") == {"p1": 120}
    # as reported on 4 Oct: the correction was recorded on 5 Oct
    assert _read(con, as_of="2026-10-03", as_recorded="2026-10-04T23:00:00") == {"p1": 100}


async def test_a_late_commit_is_read_within_the_allowed_lateness(con, engine):
    con.execute(
        "INSERT INTO pg.fin.payments VALUES ('p1', '2026-10-03', '2026-10-05 09:00:00', 100)"
    )
    await _capture(con, engine, "2026-10-05 10:00:00")
    # committed after the capture, recorded an hour before the watermark
    con.execute(
        "INSERT INTO pg.fin.payments VALUES ('p2', '2026-10-04', '2026-10-05 08:00:00', 50)"
    )
    await _capture(con, engine, "2026-10-05 11:00:00")
    assert "p2" not in _read(con)
    await _capture(con, engine, "2026-10-05 12:00:00", lateness=7200)
    assert _read(con) == {"p1": 100, "p2": 50}


async def test_a_hard_delete_is_recorded_by_a_full_capture(con, engine):
    con.execute(
        "INSERT INTO pg.fin.payments VALUES ('p1', '2026-10-03', '2026-10-03 18:00:00', 100), "
        "('p2', '2026-10-04', '2026-10-04 18:00:00', 50)"
    )
    await _capture(con, engine, "2026-10-04 20:00:00")
    con.execute("DELETE FROM pg.fin.payments WHERE payment_id = 'p2'")
    await _capture(con, engine, "2026-10-05 10:00:00")
    assert _read(con) == {"p1": 100, "p2": 50}  # a scoped capture cannot see it
    await _capture(con, engine, "2026-10-05 11:00:00", full=True)
    assert _read(con) == {"p1": 100}
    assert _read(con, as_recorded="2026-10-05T10:30:00") == {"p1": 100, "p2": 50}


def _state(registry: MVRegistry) -> SimpleNamespace:
    return SimpleNamespace(
        tables=[_row(["payment_id"])],
        mv_registry=registry,
        federation_engine=SimpleNamespace(
            materialize_store_target=lambda _org: ("store", "main"),
            engine=SimpleNamespace(catalog_qualified=True),
        ),
        active_org_id="default",
    )


def _ctx() -> SimpleNamespace:
    meta = SimpleNamespace(table_id=7, catalog_name="pg", schema_name="fin", table_name="payments")
    return SimpleNamespace(tables={"payments": meta})


async def test_a_read_under_as_of_reads_the_log_in_the_tables_place(con, engine):
    con.execute(
        "INSERT INTO pg.fin.payments VALUES ('p1', '2026-10-03', '2026-10-03 18:00:00', 100)"
    )
    await _capture(con, engine, "2026-10-04 20:00:00")
    con.execute(
        "UPDATE pg.fin.payments SET amount = 120, updated_at = '2026-10-05 09:00:00' "
        "WHERE payment_id = 'p1'"
    )
    await _capture(con, engine, "2026-10-05 10:00:00")
    registry = MVRegistry()
    state = _state(registry)
    capture.register_logs(state, state.tables, 300)
    sql = 'SELECT p.amount FROM "pg"."fin"."payments" AS p WHERE p.amount > 0'
    # a plain read reads the table
    assert capture.read_logs(sql, state, _ctx(), None) == sql
    assert not capture.reads_a_log([7], state, None)
    travel = request_as_of(None, "2026-10-04T23:00:00")
    assert capture.reads_a_log([7], state, travel)
    read = capture.read_logs(sql, state, _ctx(), travel)
    # what governance put around the reference (its alias's predicate) still applies
    assert con.execute(read).fetchall() == [(100,)]


async def test_a_table_no_longer_updated_in_place_has_its_log_dropped():
    registry = MVRegistry()
    state = _state(registry)
    capture.register_logs(state, state.tables, 300)
    assert (
        registry.get("versions-7").refresh_interval,
        registry.get("versions-7").allowed_lateness,
    ) == (300, 0.0)
    # its interval and lateness are what its table's row holds for it
    row = {**_row(["payment_id"]), "mv_refresh_interval": 60, "mv_allowed_lateness": 900.0}
    capture.register_logs(state, [row], 300)
    assert (
        registry.get("versions-7").refresh_interval,
        registry.get("versions-7").allowed_lateness,
    ) == (60, 900.0)
    # a draft table keeps its log, uncaptured
    capture.register_logs(state, [{**row, "draft": True}], 300)
    assert registry.get("versions-7").enabled is False
    capture.register_logs(state, [_row(["row_id"])], 300)
    assert registry.get("versions-7") is None
