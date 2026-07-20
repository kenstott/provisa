# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1159 + REQ-844: bitemporal materialization with ICEBERG output.

Iceberg is the substrate where time travel matters most, and our approach is designed for it: the
maintenance is APPEND-ONLY, and an Iceberg append is exactly one new snapshot. This test writes each
refresh's engine-computed batch into a REAL Iceberg table (pyiceberg, local catalog — no network),
then reconstructs as-of history by reading that Iceberg table back through DuckDB's iceberg_scan with
the same reconstruct_as_of_sql used everywhere else. It proves two things on real Iceberg:

  1. one refresh == one Iceberg snapshot (append-only never rewrites — REQ-844's invariant), and
  2. as-of reconstruction over Iceberg storage returns the right historical state.
"""

from __future__ import annotations

import os

import duckdb
import pyarrow as pa
import pytest

from provisa.mv.bitemporal import (
    MODE_DELTA,
    MODE_SNAPSHOT,
    BitemporalSpec,
    append_sql,
    create_sql,
    reconstruct_as_of_sql,
)

from tests.unit.test_bitemporal_exec import STEP1, STEP2, STEP3, STEP4, T1, T2, T3, T4

pytest.importorskip("pyiceberg")
from pyiceberg.catalog.sql import SqlCatalog  # noqa: E402

COLS = ["id", "region", "amount"]
SELECT = "SELECT id, region, amount FROM base"
SCRATCH = "mv_scratch"


def _iceberg_schema(spec: BitemporalSpec) -> pa.Schema:
    fields = [
        ("id", pa.int32()),
        ("region", pa.string()),
        ("amount", pa.int32()),
        (spec.system_column, pa.timestamp("us")),
    ]
    if spec.is_delta:
        fields.append((spec.op_column, pa.string()))
    return pa.schema(fields)


class IcebergDriver:
    """Runs the real bitemporal SQL in a DuckDB scratch, then mirrors each refresh's appended batch
    into a REAL Iceberg table and reconstructs by reading that Iceberg table back."""

    def __init__(self, tmp: str, spec: BitemporalSpec):
        self.spec = spec
        self.con = duckdb.connect(":memory:")
        self.con.execute("INSTALL iceberg")
        self.con.execute("LOAD iceberg")
        self.con.execute("CREATE TABLE base (id INTEGER, region VARCHAR, amount INTEGER)")
        self.exists = False
        wh = os.path.join(tmp, "wh")
        os.makedirs(wh, exist_ok=True)
        self.cat = SqlCatalog(
            "t", uri=f"sqlite:///{os.path.join(tmp, 'cat.db')}", warehouse=f"file://{wh}"
        )
        self.cat.create_namespace("mv")
        self.schema = _iceberg_schema(spec)
        self.allcols = [f.name for f in self.schema]
        self.cat.create_table("mv.log", schema=self.schema)

    def refresh(self, rows: list[tuple], ts: str) -> None:
        self.con.execute("DELETE FROM base")
        for r in rows:
            self.con.execute("INSERT INTO base VALUES (?, ?, ?)", r)
        now = f"TIMESTAMP '{ts}'"
        if not self.exists:
            self.con.execute(create_sql(SCRATCH, SELECT, self.spec, now))
            self.exists = True
        else:
            for stmt in append_sql(SCRATCH, SELECT, self.spec, COLS, now, "duckdb"):
                self.con.execute(stmt)
        # This refresh's appended batch = the rows stamped with this system time. Append them to
        # the real Iceberg table — one append == one Iceberg snapshot.
        select = ", ".join(f'"{c}"' for c in self.allcols)
        batch = self.con.execute(
            f'SELECT {select} FROM {SCRATCH} WHERE "{self.spec.system_column}" = {now}'
        ).fetchall()
        if batch:
            data = {name: [row[i] for row in batch] for i, name in enumerate(self.allcols)}
            self.cat.load_table("mv.log").append(pa.table(data, schema=self.schema))

    def _scan(self) -> str:
        loc = self.cat.load_table("mv.log").metadata_location
        return f"iceberg_scan('{loc}')"

    def state(self, ts: str | None = None) -> set[tuple]:
        ts_sql = f"TIMESTAMP '{ts}'" if ts else None
        sql = reconstruct_as_of_sql(self._scan(), self.spec, COLS, ts_sql)
        return set(self.con.execute(sql).fetchall())

    def snapshot_count(self) -> int:
        return len(list(self.cat.load_table("mv.log").metadata.snapshots))


def _story(driver: IcebergDriver) -> IcebergDriver:
    for rows, ts in [(STEP1, T1), (STEP2, T2), (STEP3, T3), (STEP4, T4)]:
        driver.refresh(rows, ts)
    return driver


@pytest.mark.parametrize("mode", [MODE_SNAPSHOT, MODE_DELTA])
def test_iceberg_one_snapshot_per_refresh(tmp_path, mode):
    """Append-only: each refresh commits exactly one Iceberg snapshot, never rewrites (REQ-844)."""
    d = _story(IcebergDriver(str(tmp_path), BitemporalSpec(key=("id",), mode=mode)))
    assert d.snapshot_count() == 4


@pytest.mark.parametrize("mode", [MODE_SNAPSHOT, MODE_DELTA])
def test_iceberg_current_state(tmp_path, mode):
    d = _story(IcebergDriver(str(tmp_path), BitemporalSpec(key=("id",), mode=mode)))
    assert d.state() == {(1, "west", 15), (2, "east", 99), (3, "north", 30)}


@pytest.mark.parametrize("mode", [MODE_SNAPSHOT, MODE_DELTA])
def test_iceberg_as_of_history(tmp_path, mode):
    d = _story(IcebergDriver(str(tmp_path), BitemporalSpec(key=("id",), mode=mode)))
    assert d.state(T1) == {(1, "west", 10), (2, "east", 20)}
    assert d.state(T2) == {(1, "west", 15), (2, "east", 20), (3, "north", 30)}
    assert d.state(T3) == {(1, "west", 15), (3, "north", 30)}  # id=2 deleted
    assert d.state(T4) == {(1, "west", 15), (2, "east", 99), (3, "north", 30)}  # id=2 back
