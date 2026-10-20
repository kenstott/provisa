# Copyright (c) 2026 Kenneth Stott
# Canary: 3f7c2a91-d84e-4b6a-a0c5-71e9b2d4f863
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-2109: a materialized view that groups the one versioned table it reads inherits its
versions (key: its GROUP BY outputs; valid time: its output that is the table's valid time;
recorded time: each refresh's append stamp), and each refresh rebuilds only the valid dates its
input changed since a watermark the log itself keeps. Reads of a bitemporal view travel on valid
time (REQ-1163 as amended 2026-10-20). Everything is run against real rows in DuckDB through
refresh_mv; the one pipeline is stood in for, the versioned table read as the pipeline reads it
(latest version of each payment)."""

# Requirements: REQ-1163, REQ-2109

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import duckdb
import pytest

from provisa.executor.result import QueryResult
from provisa.mv import refresh as refresh_mod
from provisa.mv.bitemporal import (
    MODE_DELTA,
    WATERMARK_COLUMN,
    AsOf,
    append_sql,
    create_sql,
    declared_spec,
    request_as_of,
    view_read_sql,
)
from provisa.mv.bitemporal_inherit import inherited_spec
from provisa.mv.bitemporal_tables import VersionedTable, version_read_sql
from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.refresh import _target_ref, refresh_failure, refresh_mv
from provisa.mv.registry import MVRegistry
from tests.helpers import build_through_a_stand_in_pipeline, hold_registered_tables

pytestmark = pytest.mark.unit

VIEW_SQL = (
    "SELECT store, business_date, SUM(amount) AS total FROM fin.payments "
    "GROUP BY store, business_date"
)
_PAYMENT_COLUMNS = ["row_id", "payment_id", "store", "business_date", "updated_at", "amount"]
_VERSIONED = VersionedTable(
    table_id=1, business_key=("payment_id",), valid_time="business_date", recorded_time="updated_at"
)


def _row(**over) -> dict:
    """A registered table row as a build loads it: fin.payments, versioned on payment_id."""
    row = {
        "id": 1,
        "source_id": "pg",
        "schema_name": "fin",
        "table_name": "payments",
        "domain_id": "fin",
        "alias": None,
        "view_sql": None,
        "business_key": ["payment_id"],
        "valid_time": "business_date",
        "recorded_time": "updated_at",
        "columns": [{"column_name": c, "is_primary_key": c == "row_id"} for c in _PAYMENT_COLUMNS],
    }
    row.update(over)
    return row


# ── inheritance ─────────────────────────────────────────────────────────────────────────────


def test_a_view_grouping_one_versioned_table_inherits_its_versions():
    spec = inherited_spec(VIEW_SQL, [_row()])
    assert spec is not None
    assert (spec.key, spec.mode, spec.valid_column) == (
        ("store", "business_date"),
        MODE_DELTA,
        "business_date",
    )
    assert spec.inherited is not None
    assert (spec.inherited.ref, spec.inherited.valid_time, spec.inherited.recorded_time) == (
        '"fin"."payments"',
        "business_date",
        "updated_at",
    )
    assert spec.watermark_column == WATERMARK_COLUMN
    assert spec.source_column is None  # recorded on its append stamp


def test_its_valid_column_is_the_output_naming_the_inputs_valid_time_unchanged():
    sql = (
        "SELECT store, business_date AS day, SUM(amount) AS total FROM payments "
        "GROUP BY store, business_date"
    )
    spec = inherited_spec(sql, [_row()])
    assert spec is not None and spec.valid_column == "day" and spec.key == ("store", "day")


@pytest.mark.parametrize(
    "sql, tables",
    [
        # no GROUP BY: no key to version on
        ("SELECT store, business_date, amount FROM fin.payments", [_row()]),
        # the valid time transformed: no output column is it unchanged
        (
            "SELECT store, DATE_TRUNC('month', business_date) AS m, SUM(amount) AS t "
            "FROM fin.payments GROUP BY store, DATE_TRUNC('month', business_date)",
            [_row()],
        ),
        # the valid time not output at all
        ("SELECT store, SUM(amount) AS t FROM fin.payments GROUP BY store", [_row()]),
        # two tables read
        (
            "SELECT p.store, p.business_date, SUM(p.amount) AS t FROM fin.payments p "
            "JOIN fin.stores s ON s.store = p.store GROUP BY p.store, p.business_date",
            [_row(), _row(id=2, table_name="stores", business_key=[], columns=[])],
        ),
        # its business key is its primary key: updated in place, not versioned rows
        (
            VIEW_SQL,
            [
                _row(
                    columns=[
                        {"column_name": c, "is_primary_key": c == "payment_id"}
                        for c in _PAYMENT_COLUMNS
                    ]
                )
            ],
        ),
        # declares nothing
        (VIEW_SQL, [_row(business_key=[], valid_time=None, recorded_time=None)]),
        # names a table the model does not hold
        (VIEW_SQL, [_row(table_name="refunds")]),
    ],
)
def test_a_view_otherwise_inherits_nothing(sql, tables):
    assert inherited_spec(sql, tables) is None


# ── a stand-in engine over DuckDB ───────────────────────────────────────────────────────────


class DuckEngine:
    engine = SimpleNamespace(catalog_qualified=True)
    dialect = "duckdb"
    native_store = "duckdb"

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.con = con
        self.fail_upserts = False

    def engine_physical(self, sql):
        return sql

    def address_replicas(self, sql):
        return sql

    async def execute_engine(self, sql: str, *_a, **_k) -> QueryResult:
        if self.fail_upserts and sql.startswith("INSERT") and WATERMARK_COLUMN in sql:
            raise RuntimeError("the store refused the append")
        cur = self.con.execute(sql)
        if cur.description is None:
            return QueryResult(rows=[], column_names=[])
        return QueryResult(rows=cur.fetchall(), column_names=[d[0] for d in cur.description])


_INITIAL = [
    (1, "p1", "A", "2026-10-01", "2026-10-01 18:00:00", 100),
    (2, "p2", "B", "2026-10-01", "2026-10-01 18:00:00", 50),
    (3, "p3", "A", "2026-10-02", "2026-10-02 18:00:00", 70),
]
S1, S2, S3, S4 = (
    "2026-10-03 00:00:00",
    "2026-10-04 00:00:00",
    "2026-10-05 00:00:00",
    "2026-10-06 00:00:00",
)


class World:
    """fin.payments_log holds every version; fin.payments reads it as the pipeline does (latest
    version per payment_id). The view groups fin.payments and inherits its versions."""

    def __init__(self, monkeypatch, allowed_lateness: float = 0.0):
        self.con = duckdb.connect(":memory:")
        self.con.execute("CREATE SCHEMA fin")
        self.con.execute(
            "CREATE TABLE fin.payments_log (row_id INT, payment_id TEXT, store TEXT, "
            "business_date DATE, updated_at TIMESTAMP, amount INT)"
        )
        self.add(*_INITIAL)
        latest = version_read_sql('"fin"."payments_log"', _VERSIONED, _PAYMENT_COLUMNS, None)
        self.con.execute(f"CREATE VIEW fin.payments AS {latest}")
        self.engine = DuckEngine(self.con)
        self.registry = MVRegistry()
        spec = inherited_spec(VIEW_SQL, [_row()])
        assert spec is not None
        self.mv = MVDefinition(
            id="by_store",
            source_tables=[],
            target_catalog="memory",
            target_schema="mvs",
            sql=VIEW_SQL,
            semantic_sql=VIEW_SQL,
            bitemporal=spec,
            allowed_lateness=allowed_lateness,
        )
        self.registry.register(self.mv)
        self.target = _target_ref(self.mv)
        self._stamps = iter(f"TIMESTAMP '{s}'" for s in (S1, S2, S3, S4))
        monkeypatch.setattr(refresh_mod, "_now_ts_literal", lambda: next(self._stamps))

        async def _execute_plan(plan):
            cur = self.con.execute(plan.physical_sql)
            return QueryResult(rows=cur.fetchall(), column_names=[d[0] for d in cur.description])

        import provisa.pgwire._pipeline as pipeline

        monkeypatch.setattr(pipeline, "_execute_plan", _execute_plan)

    def add(self, *rows) -> None:
        self.con.executemany("INSERT INTO fin.payments_log VALUES (?, ?, ?, ?, ?, ?)", rows)

    async def refresh(self, full: bool = False) -> None:
        await refresh_mv(self.engine, self.mv, self.registry, full=full)

    def log_rows(self, stamp: str | None = None) -> list[tuple]:
        where = f" WHERE sys_recorded_at = TIMESTAMP '{stamp}'" if stamp else ""
        return self.con.execute(
            "SELECT store, CAST(business_date AS VARCHAR), total, sys_op "
            f"FROM {self.target}{where} ORDER BY store, business_date"
        ).fetchall()

    def watermark(self):
        return self.con.execute(f"SELECT MAX({WATERMARK_COLUMN}) FROM {self.target}").fetchone()[0]

    def read(self, as_of: str | None = None, as_recorded: str | None = None) -> set[tuple]:
        key = request_as_of(as_of, as_recorded)
        travel = AsOf.from_key(key) if key else None
        sql = view_read_sql(self.target, self.mv.bitemporal, travel)
        return set(
            self.con.execute(
                f"SELECT store, CAST(business_date AS VARCHAR), total FROM ({sql}) t"
            ).fetchall()
        )


@pytest.fixture(autouse=True)
def _stand_in(monkeypatch):
    build_through_a_stand_in_pipeline(monkeypatch)
    hold_registered_tables(monkeypatch, "payments", schema="fin")


def _ts(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s)


# ── the refresh ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_the_first_build_is_full_and_carries_the_inputs_watermark(monkeypatch):
    w = World(monkeypatch)
    await w.refresh()
    assert w.log_rows() == [
        ("A", "2026-10-01", 100, "upsert"),
        ("A", "2026-10-02", 70, "upsert"),
        ("B", "2026-10-01", 50, "upsert"),
    ]
    assert w.watermark() == _ts("2026-10-02 18:00:00")


@pytest.mark.asyncio
async def test_a_refresh_appends_only_the_changed_dates_with_tombstones_confined(monkeypatch):
    w = World(monkeypatch)
    await w.refresh()
    # p3 corrected (same date), p4 new on 3 Oct
    w.add(
        (4, "p4", "B", "2026-10-03", "2026-10-03 18:00:00", 30),
        (5, "p3", "A", "2026-10-02", "2026-10-03 19:00:00", 75),
    )
    await w.refresh()
    # Only 2 and 3 Oct were rebuilt; 1 Oct's groups (absent from that build) are not tombstoned.
    assert w.log_rows(S2) == [("A", "2026-10-02", 75, "upsert"), ("B", "2026-10-03", 30, "upsert")]
    assert w.watermark() == _ts("2026-10-03 19:00:00")
    # p4 moves from store B to A on the same date: B's 3 Oct group is gone -- within the rebuilt
    # date, so tombstoned.
    w.add((6, "p4", "A", "2026-10-03", "2026-10-04 10:00:00", 30))
    await w.refresh()
    assert w.log_rows(S3) == [
        ("A", "2026-10-03", 30, "upsert"),
        ("B", "2026-10-03", None, "delete"),
    ]
    assert w.read() == {
        ("A", "2026-10-01", 100),
        ("B", "2026-10-01", 50),
        ("A", "2026-10-02", 75),
        ("A", "2026-10-03", 30),
    }


@pytest.mark.asyncio
async def test_with_no_input_change_nothing_is_appended(monkeypatch):
    w = World(monkeypatch)
    await w.refresh()
    before = w.log_rows()
    await w.refresh()
    assert w.log_rows() == before
    assert w.mv.status == MVStatus.FRESH and refresh_failure(w.mv) is None


@pytest.mark.asyncio
async def test_the_watermark_advances_only_with_an_append_that_commits(monkeypatch):
    w = World(monkeypatch)
    await w.refresh()
    w.add((4, "p4", "B", "2026-10-03", "2026-10-03 18:00:00", 30))
    w.engine.fail_upserts = True
    await w.refresh()
    assert refresh_failure(w.mv) is not None
    assert w.watermark() == _ts("2026-10-02 18:00:00")  # unchanged: the append failed
    w.engine.fail_upserts = False
    await w.refresh()  # the same change is read again
    assert ("B", "2026-10-03", 30) in w.read()
    assert w.watermark() == _ts("2026-10-03 18:00:00")


# ── edge cases ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_a_late_commit_within_the_allowed_lateness_is_read(monkeypatch):
    w = World(monkeypatch, allowed_lateness=3600)
    await w.refresh()
    # Committed after the last watermark was read, but carrying an earlier recorded time.
    w.add((7, "p7", "B", "2026-10-02", "2026-10-02 17:30:00", 20))
    await w.refresh()
    assert w.log_rows(S2) == [("B", "2026-10-02", 20, "upsert")]
    # Re-reading 2 Oct (still within the lateness) appends nothing: the delta compares values.
    count = len(w.log_rows())
    await w.refresh()
    assert len(w.log_rows()) == count


@pytest.mark.asyncio
async def test_a_late_commit_beyond_the_allowed_lateness_waits_for_a_full_refresh(monkeypatch):
    w = World(monkeypatch)  # allowed lateness 0
    await w.refresh()
    w.add((7, "p7", "B", "2026-10-02", "2026-10-02 17:30:00", 20))
    await w.refresh()
    assert ("B", "2026-10-02", 20) not in w.read()
    await w.refresh(full=True)
    assert ("B", "2026-10-02", 20) in w.read()


@pytest.mark.asyncio
async def test_a_row_moved_between_dates_leaves_the_old_date_stale_until_a_full_refresh(
    monkeypatch,
):
    w = World(monkeypatch)
    await w.refresh()
    # p1 corrected from 1 Oct to 2 Oct.
    w.add((8, "p1", "A", "2026-10-02", "2026-10-03 12:00:00", 100))
    await w.refresh()
    # 2 Oct is rebuilt; 1 Oct is not among the changed dates and keeps its stale aggregate.
    assert w.read() == {("A", "2026-10-01", 100), ("B", "2026-10-01", 50), ("A", "2026-10-02", 170)}
    await w.refresh(full=True)
    assert w.read() == {("B", "2026-10-01", 50), ("A", "2026-10-02", 170)}


@pytest.mark.asyncio
async def test_a_hard_delete_is_seen_only_by_a_full_refresh(monkeypatch):
    w = World(monkeypatch)
    await w.refresh()
    w.con.execute("DELETE FROM fin.payments_log WHERE payment_id = 'p2'")
    await w.refresh()
    assert ("B", "2026-10-01", 50) in w.read()  # a deleted row has no recorded time to read
    await w.refresh(full=True)
    assert w.read() == {("A", "2026-10-01", 100), ("A", "2026-10-02", 70)}
    # the scoped refresh appended nothing, so the full one is the second append
    assert ("B", "2026-10-01", None, "delete") in w.log_rows(S2)


# ── reads of an inherited view ──────────────────────────────────────────────────────────────


@pytest.fixture
async def history(monkeypatch):
    w = World(monkeypatch)
    await w.refresh()  # S1
    w.add(
        (4, "p4", "B", "2026-10-03", "2026-10-03 18:00:00", 30),
        (5, "p3", "A", "2026-10-02", "2026-10-03 19:00:00", 75),
    )
    await w.refresh()  # S2
    w.add((6, "p4", "A", "2026-10-03", "2026-10-04 10:00:00", 30))
    await w.refresh()  # S3
    return w


@pytest.mark.asyncio
async def test_an_inherited_views_plain_read_is_its_current_state(history):
    assert history.read() == {
        ("A", "2026-10-01", 100),
        ("B", "2026-10-01", 50),
        ("A", "2026-10-02", 75),
        ("A", "2026-10-03", 30),
    }


@pytest.mark.asyncio
async def test_an_inherited_views_as_of_reads_on_its_valid_time(history):
    assert history.read(as_of="2026-10-02") == {
        ("A", "2026-10-01", 100),
        ("B", "2026-10-01", 50),
        ("A", "2026-10-02", 75),
    }


@pytest.mark.asyncio
async def test_an_inherited_views_range_answers_every_version_valid_within_it(history):
    key = request_as_of("2026-10-02/2026-10-04", None)
    assert key is not None
    sql = view_read_sql(history.target, history.mv.bitemporal, AsOf.from_key(key))
    rows = history.con.execute(
        f"SELECT store, CAST(business_date AS VARCHAR), total FROM ({sql}) t "
        "ORDER BY business_date, recorded_at"
    ).fetchall()
    assert rows == [
        ("A", "2026-10-02", 70),
        ("A", "2026-10-02", 75),
        ("B", "2026-10-03", 30),
        ("A", "2026-10-03", 30),
    ]


@pytest.mark.asyncio
async def test_an_inherited_views_as_recorded_cuts_on_its_append_stamp(history):
    assert history.read(as_recorded="2026-10-03T00:00:00") == {
        ("A", "2026-10-01", 100),
        ("B", "2026-10-01", 50),
        ("A", "2026-10-02", 70),
    }
    assert history.read(as_of="2026-10-03", as_recorded="2026-10-04T00:00:00") == {
        ("A", "2026-10-01", 100),
        ("B", "2026-10-01", 50),
        ("A", "2026-10-02", 75),
        ("B", "2026-10-03", 30),
    }


# ── reads of a declared view ────────────────────────────────────────────────────────────────

_TRADES = "SELECT trade_id, qty, trade_date, updated_at FROM base"
_TRADE_COLS = ["trade_id", "qty", "trade_date", "updated_at"]


@pytest.fixture
def declared():
    """A declared view: business key trade_id, valid time trade_date, recorded time updated_at.
    t1 booked for 1 Oct (recorded 1 Oct), corrected (recorded 3 Oct); t2 booked for 2 Oct and
    later gone."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE base (trade_id TEXT, qty INT, trade_date DATE, updated_at TIMESTAMP)")
    spec = declared_spec(["trade_id"], "trade_date", "updated_at", _TRADE_COLS)
    steps = [
        (
            [
                ("t1", 10, "2026-10-01", "2026-10-01 09:00"),
                ("t2", 5, "2026-10-02", "2026-10-02 09:00"),
            ],
            S1,
        ),
        (
            [
                ("t1", 12, "2026-10-01", "2026-10-03 09:00"),
                ("t2", 5, "2026-10-02", "2026-10-02 09:00"),
            ],
            S2,
        ),
        ([("t1", 12, "2026-10-01", "2026-10-03 09:00")], S3),
    ]
    for i, (rows, stamp) in enumerate(steps):
        con.execute("DELETE FROM base")
        con.executemany("INSERT INTO base VALUES (?, ?, ?, ?)", rows)
        now = f"TIMESTAMP '{stamp}'"
        if i == 0:
            con.execute(create_sql("log", _TRADES, spec, now))
        else:
            for stmt in append_sql("log", _TRADES, spec, _TRADE_COLS, now, "duckdb"):
                con.execute(stmt)

    def read(as_of=None, as_recorded=None) -> set[tuple]:
        key = request_as_of(as_of, as_recorded)
        sql = view_read_sql("log", spec, AsOf.from_key(key) if key else None)
        return set(con.execute(f"SELECT trade_id, qty FROM ({sql}) t").fetchall())

    return read


def test_a_declared_views_plain_read_drops_a_key_gone_since(declared):
    assert declared() == {("t1", 12)}


def test_a_declared_views_as_of_reads_on_its_valid_time(declared):
    # t2's disappearance took effect when it was recorded (5 Oct), after 2 Oct.
    assert declared(as_of="2026-10-02") == {("t1", 12), ("t2", 5)}
    assert declared(as_of="2026-09-30") == set()


def test_a_declared_views_range_answers_every_version_valid_within_it(declared):
    assert declared(as_of="2026-10-01/2026-10-02") == {("t1", 10), ("t1", 12)}


def test_a_declared_views_as_recorded_cuts_on_its_recorded_time(declared):
    # Its recorded time is the declared updated_at: the correction was recorded on 3 Oct.
    assert declared(as_of="2026-10-02", as_recorded="2026-10-02T12:00:00") == {
        ("t1", 10),
        ("t2", 5),
    }
    assert declared(as_of="2026-10-02", as_recorded="2026-10-03T12:00:00") == {
        ("t1", 12),
        ("t2", 5),
    }
