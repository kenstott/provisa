# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1159: BEHAVIORAL validation — execute the generated bitemporal SQL against a real engine
(DuckDB) across the full scenario matrix and assert the reconstructed state is correct.

test_bitemporal.py checks the SQL *shape*; this file proves the SQL actually *works*: a sequence of
refreshes with inserts, updates, deletes, reappearances, NULLs and composite keys reconstructs the
right current and as-of state — and snapshot and delta modes agree.
"""

from __future__ import annotations

import duckdb
import pytest

from provisa.mv.bitemporal import (
    MODE_DELTA,
    MODE_SNAPSHOT,
    BitemporalSpec,
    append_sql,
    create_sql,
    reconstruct_as_of_sql,
)

COLS = ["id", "region", "amount"]
SELECT = "SELECT id, region, amount FROM base"
TARGET = "mv_t"


class Driver:
    """Drives a bitemporal MV over an in-memory DuckDB, mirroring refresh_mv's create-then-append."""

    def __init__(self, spec: BitemporalSpec):
        self.spec = spec
        self.con = duckdb.connect(":memory:")
        self.con.execute("CREATE TABLE base (id INTEGER, region VARCHAR, amount INTEGER)")
        self.exists = False

    def refresh(self, rows: list[tuple], ts: str) -> None:
        self.con.execute("DELETE FROM base")
        for r in rows:
            self.con.execute("INSERT INTO base VALUES (?, ?, ?)", r)
        now = f"TIMESTAMP '{ts}'"
        if not self.exists:
            self.con.execute(create_sql(TARGET, SELECT, self.spec, now))
            self.exists = True
        else:
            for stmt in append_sql(TARGET, SELECT, self.spec, COLS, now, "duckdb"):
                self.con.execute(stmt)

    def state(self, ts: str | None = None) -> set[tuple]:
        ts_sql = f"TIMESTAMP '{ts}'" if ts else None
        sql = reconstruct_as_of_sql(TARGET, self.spec, COLS, ts_sql)
        return set(self.con.execute(sql).fetchall())

    def version_count(self) -> int:
        row = self.con.execute(f"SELECT COUNT(*) FROM {TARGET}").fetchone()
        assert row is not None
        return row[0]


T1, T2, T3, T4 = (
    "2026-01-01 00:00:00",
    "2026-02-01 00:00:00",
    "2026-03-01 00:00:00",
    "2026-04-01 00:00:00",
)

# A shared story: load, then update id=1 + insert id=3, then delete id=2, then id=2 reappears.
STEP1 = [(1, "west", 10), (2, "east", 20)]
STEP2 = [(1, "west", 15), (2, "east", 20), (3, "north", 30)]
STEP3 = [(1, "west", 15), (3, "north", 30)]
STEP4 = [(1, "west", 15), (2, "east", 99), (3, "north", 30)]


def _run_story(mode: str) -> Driver:
    d = Driver(BitemporalSpec(key=("id",), mode=mode))
    d.refresh(STEP1, T1)
    d.refresh(STEP2, T2)
    d.refresh(STEP3, T3)
    d.refresh(STEP4, T4)
    return d


@pytest.mark.parametrize("mode", [MODE_SNAPSHOT, MODE_DELTA])
def test_current_state_after_full_story(mode):
    d = _run_story(mode)
    assert d.state() == {(1, "west", 15), (2, "east", 99), (3, "north", 30)}


@pytest.mark.parametrize("mode", [MODE_SNAPSHOT, MODE_DELTA])
def test_as_of_reconstructs_each_point_in_history(mode):
    d = _run_story(mode)
    assert d.state(T1) == {(1, "west", 10), (2, "east", 20)}
    assert d.state(T2) == {(1, "west", 15), (2, "east", 20), (3, "north", 30)}
    assert d.state(T3) == {(1, "west", 15), (3, "north", 30)}  # id=2 deleted
    assert d.state(T4) == {(1, "west", 15), (2, "east", 99), (3, "north", 30)}  # id=2 back


def test_snapshot_and_delta_agree_at_every_step():
    """The two write choices must be observationally equivalent on reads."""
    snap, delta = Driver(BitemporalSpec(key=("id",), mode=MODE_SNAPSHOT)), Driver(
        BitemporalSpec(key=("id",), mode=MODE_DELTA)
    )
    for rows, ts in [(STEP1, T1), (STEP2, T2), (STEP3, T3), (STEP4, T4)]:
        snap.refresh(rows, ts)
        delta.refresh(rows, ts)
        assert snap.state() == delta.state()
    for ts in (T1, T2, T3, T4):
        assert snap.state(ts) == delta.state(ts)


def test_delta_appends_only_changes_not_full_dataset():
    """Delta must be economical: an unchanged refresh adds no rows; a 1-row change adds ~1."""
    d = Driver(BitemporalSpec(key=("id",), mode=MODE_DELTA))
    d.refresh(STEP1, T1)  # 2 initial versions
    assert d.version_count() == 2
    d.refresh(STEP1, T2)  # identical → nothing appended
    assert d.version_count() == 2
    d.refresh([(1, "west", 11), (2, "east", 20)], T3)  # only id=1 changed → +1
    assert d.version_count() == 3


def test_delta_deletion_writes_a_tombstone_not_a_mutation():
    d = Driver(BitemporalSpec(key=("id",), mode=MODE_DELTA))
    d.refresh(STEP1, T1)
    d.refresh([(1, "west", 10)], T2)  # id=2 removed
    assert d.state() == {(1, "west", 10)}
    # history is intact: id=2 still visible as-of T1 (never mutated away)
    assert d.state(T1) == {(1, "west", 10), (2, "east", 20)}


def test_null_attribute_unchanged_is_not_a_new_version():
    """Null-safe equality: a NULL that stays NULL must not look like a change (no phantom version)."""
    d = Driver(BitemporalSpec(key=("id",), mode=MODE_DELTA))
    d.con.execute("DROP TABLE base")
    d.con.execute("CREATE TABLE base (id INTEGER, region VARCHAR, amount INTEGER)")
    d.refresh([(1, "west", None)], T1)
    d.refresh([(1, "west", None)], T2)  # identical incl. NULL → no new version
    assert d.version_count() == 1
    assert d.state() == {(1, "west", None)}
    d.refresh([(1, "west", 5)], T3)  # NULL → 5 IS a change
    assert d.version_count() == 2
    assert d.state() == {(1, "west", 5)}
    assert d.state(T1) == {(1, "west", None)}


def test_composite_business_key():
    spec = BitemporalSpec(key=("id", "region"), mode=MODE_DELTA)
    d = Driver(spec)
    # same id, different region = two distinct entities
    d.refresh([(1, "west", 10), (1, "east", 20)], T1)
    assert d.state() == {(1, "west", 10), (1, "east", 20)}
    d.refresh([(1, "west", 11), (1, "east", 20)], T2)  # only (1,west) changed
    assert d.state() == {(1, "west", 11), (1, "east", 20)}
    assert d.state(T1) == {(1, "west", 10), (1, "east", 20)}


@pytest.mark.parametrize("mode", [MODE_SNAPSHOT, MODE_DELTA])
def test_before_first_refresh_is_empty(mode):
    d = _run_story(mode)
    assert d.state("2025-01-01 00:00:00") == set()
