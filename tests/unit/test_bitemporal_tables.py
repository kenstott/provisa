# Copyright (c) 2026 Kenneth Stott
# Canary: 0205aa0f-04ff-41ad-b2d8-aecd6c96e51e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-2109: a table whose business key is not its primary key keeps every version as a row; a
read of it filters and keeps the latest version per key, on valid time (as_of) and recorded time
(as_recorded). The read is run here against real rows (DuckDB) so its answers are checked, not
its text."""

# Requirements: REQ-2109

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pytest

from provisa.compiler.stage2 import GovernanceContext
from provisa.mv.bitemporal import request_as_of
from provisa.mv.bitemporal_tables import (
    VersionedTable,
    read_versions,
    version_read_sql,
    versioned_tables,
)

# A payment of 100 for 3 Oct, corrected to 120 on 5 Oct (a new row, same payment_id), and one of
# 50 for 4 Oct recorded on 4 Oct.
_ROWS = [
    (1, "p1", "2026-10-03", "2026-10-03 18:00:00", 100),
    (2, "p1", "2026-10-03", "2026-10-05 09:00:00", 120),
    (3, "p2", "2026-10-04", "2026-10-04 18:00:00", 50),
]
_COLUMNS = ["row_id", "payment_id", "business_date", "updated_at", "amount"]
_TABLE = VersionedTable(
    table_id=7, business_key=("payment_id",), valid_time="business_date", recorded_time="updated_at"
)


@pytest.fixture
def ledger():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA fin")
    con.execute(
        "CREATE TABLE fin.payments (row_id INT, payment_id TEXT, business_date DATE, "
        "updated_at TIMESTAMP, amount INT)"
    )
    con.executemany("INSERT INTO fin.payments VALUES (?, ?, ?, ?, ?)", _ROWS)
    yield con
    con.close()


def _read(con, as_of=None, as_recorded=None) -> dict[str, int]:
    travel = request_as_of(as_of, as_recorded)
    sql = version_read_sql('"fin"."payments"', _TABLE, _COLUMNS, travel)
    return {p: a for p, a in con.execute(f"SELECT payment_id, amount FROM ({sql}) t").fetchall()}


def test_a_plain_read_answers_the_latest_version_of_each_key(ledger):
    assert _read(ledger) == {"p1": 120, "p2": 50}


def test_as_of_filters_valid_time_and_keeps_the_latest_version(ledger):
    assert _read(ledger, as_of="2026-10-03") == {"p1": 120}


def test_as_recorded_answers_what_was_known_then(ledger):
    # as reported on 4 Oct: the correction recorded on 5 Oct is not yet known
    assert _read(ledger, as_of="2026-10-03", as_recorded="2026-10-04T23:00:00") == {"p1": 100}
    assert _read(ledger, as_recorded="2026-10-04T23:00:00") == {"p1": 100, "p2": 50}
    assert _read(ledger, as_recorded="2026-10-03T12:00:00") == {}


def test_a_range_answers_every_version_whose_valid_time_falls_in_it(ledger):
    travel = request_as_of("2026-10-03/2026-10-04", None)
    sql = version_read_sql('"fin"."payments"', _TABLE, _COLUMNS, travel)
    rows = ledger.execute(f"SELECT payment_id, amount FROM ({sql}) t ORDER BY amount").fetchall()
    assert rows == [("p1", 100), ("p1", 120)]


def _state(primary_key: list[str]) -> SimpleNamespace:
    return SimpleNamespace(
        tables=[
            {
                "id": 7,
                "view_sql": None,
                "business_key": ["payment_id"],
                "valid_time": "business_date",
                "recorded_time": "updated_at",
                "columns": [
                    {"column_name": c, "is_primary_key": c in primary_key} for c in _COLUMNS
                ],
            }
        ]
    )


def _gov(visible=None) -> GovernanceContext:
    return GovernanceContext(
        table_map={"fin.payments": 7, "payments": 7},
        all_columns={7: [(c, "text") for c in _COLUMNS]},
        visible_columns={7: visible},
    )


def test_only_a_table_whose_key_is_not_its_primary_key_is_read_as_versions():
    assert set(versioned_tables(_state(["row_id"]))) == {7}
    # its business key is its primary key: updated in place, its versions a captured log
    assert versioned_tables(_state(["payment_id"])) == {}


def test_every_reference_is_read_as_versions_under_its_alias(ledger):
    sql = "SELECT p.payment_id, SUM(p.amount) AS total FROM fin.payments AS p GROUP BY p.payment_id"
    out = read_versions(sql, _state(["row_id"]), _gov(), request_as_of("2026-10-03", None))
    assert "ROW_NUMBER" in out and "fin.payments" in out
    # the aggregate over it runs whole, as written, over the versions read
    assert dict(ledger.execute(out).fetchall()) == {"p1": 120}
    # a statement over no versioned table is left as it is
    assert read_versions("SELECT 1", _state(["row_id"]), _gov(), None) == "SELECT 1"


def test_the_read_answers_only_the_columns_the_reader_sees(ledger):
    visible = frozenset({"payment_id", "business_date", "updated_at", "amount"})
    out = read_versions("SELECT * FROM fin.payments", _state(["row_id"]), _gov(visible), None)
    assert [d[0] for d in ledger.execute(out).description] == [
        "payment_id",
        "business_date",
        "updated_at",
        "amount",
    ]
    hidden_time = frozenset({"payment_id", "business_date", "amount"})
    with pytest.raises(PermissionError, match="updated_at"):
        read_versions("SELECT * FROM fin.payments", _state(["row_id"]), _gov(hidden_time), None)
