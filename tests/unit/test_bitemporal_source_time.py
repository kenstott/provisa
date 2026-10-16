# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Time travel on the source's clock (REQ-1163 as amended) and over a range (REQ-2064).

A bitemporal view's versions are ordered by the source time it names -- else by when Provisa
recorded them -- and each is in force from its time to the next version's. A point answers the
version of each key in force then; a range answers every version in force within it, each with
its source time, the next version's and its recorded time. Run on DuckDB."""

# Requirements: REQ-1163, REQ-2064

from __future__ import annotations

import duckdb
import pytest

from provisa.mv.bitemporal import (
    MODE_DELTA,
    MODE_SNAPSHOT,
    AsOf,
    BitemporalSpec,
    parse_as_of,
    view_read_sql,
    with_read_columns,
)

# id 1 changed in the source on Jan 1 and Feb 1, recorded on Jan 5 and Feb 3; id 2 changed on Jan 2,
# recorded Jan 5, and was deleted, recorded Mar 1.
_DELTA = """INSERT INTO log VALUES
  (1, 'a', '2026-01-01', '2026-01-05', 'upsert'), (2, 'x', '2026-01-02', '2026-01-05', 'upsert'),
  (1, 'b', '2026-02-01', '2026-02-03', 'upsert'), (2, NULL, NULL, '2026-03-01', 'delete')"""
# The same history as whole snapshots: Jan 5 holds both, Feb 3 holds 1 changed and 2, Mar 1 only 1.
_SNAPSHOT = """INSERT INTO log VALUES
  (1, 'a', '2026-01-01', '2026-01-05'), (2, 'x', '2026-01-02', '2026-01-05'),
  (1, 'b', '2026-02-01', '2026-02-03'), (2, 'x', '2026-01-02', '2026-02-03'),
  (1, 'b', '2026-02-01', '2026-03-01')"""


def _log(mode: str):
    con = duckdb.connect()
    op = ", sys_op VARCHAR" if mode == MODE_DELTA else ""
    con.execute(
        f"CREATE TABLE log(id INT, v VARCHAR, updated_at TIMESTAMP, sys_recorded_at TIMESTAMP{op})"
    )
    con.execute(_DELTA if mode == MODE_DELTA else _SNAPSHOT)
    return con, BitemporalSpec(key=("id",), mode=mode, source_column="updated_at")


def _read(con, spec, as_of=None) -> list[tuple]:
    return con.execute(
        "SELECT id, v, CAST(source_from AS DATE), CAST(source_to AS DATE) "
        f"FROM ({view_read_sql('log', spec, as_of)}) t ORDER BY id, source_from"
    ).fetchall()


@pytest.mark.parametrize("mode", [MODE_DELTA, MODE_SNAPSHOT])
def test_a_plain_read_is_the_version_in_force_now(mode):
    con, spec = _log(mode)
    assert [r[:2] for r in _read(con, spec)] == [(1, "b")]  # 2 was deleted


@pytest.mark.parametrize("mode", [MODE_DELTA, MODE_SNAPSHOT])
def test_a_point_reads_on_the_sources_time_not_provisas(mode):
    con, spec = _log(mode)
    # Jan 3: in force in the source though Provisa recorded neither until Jan 5.
    assert [r[:2] for r in _read(con, spec, parse_as_of("2026-01-03"))] == [(1, "a"), (2, "x")]
    # Feb 2: 1 had changed in the source (Feb 1) a day before Provisa recorded it (Feb 3).
    assert [r[:2] for r in _read(con, spec, parse_as_of("2026-02-02"))] == [(1, "b"), (2, "x")]


@pytest.mark.parametrize("mode", [MODE_DELTA, MODE_SNAPSHOT])
def test_a_range_answers_every_version_in_force_within_it(mode):
    con, spec = _log(mode)
    rows = _read(con, spec, parse_as_of("2026-01-01/2026-03-15"))
    assert [r[:2] for r in rows] == [(1, "a"), (1, "b"), (2, "x")]
    assert [str(r[2]) for r in rows] == ["2026-01-01", "2026-02-01", "2026-01-02"]
    # a version's span ends at the next's source time; a delete ends it when it was recorded
    assert [None if r[3] is None else str(r[3]) for r in rows] == [
        "2026-02-01",
        None,
        "2026-03-01",
    ]


def test_a_range_that_ends_before_a_change_answers_the_version_before_it():
    con, spec = _log(MODE_DELTA)
    assert [r[:2] for r in _read(con, spec, parse_as_of("2026-01-01/2026-01-20"))] == [
        (1, "a"),
        (2, "x"),
    ]


def test_each_version_says_when_provisa_recorded_it():
    con, spec = _log(MODE_DELTA)
    rows = con.execute(
        f"SELECT id, v, CAST(recorded_at AS DATE) FROM ({view_read_sql('log', spec)}) t"
    ).fetchall()
    assert [(r[0], r[1], str(r[2])) for r in rows] == [(1, "b", "2026-02-03")]


def test_with_no_source_time_the_clock_is_when_provisa_recorded_each_version():
    con = duckdb.connect()
    con.execute("CREATE TABLE log(id INT, v VARCHAR, sys_recorded_at TIMESTAMP, sys_op VARCHAR)")
    con.execute(
        "INSERT INTO log VALUES (1, 'a', '2026-01-05', 'upsert'), (1, 'b', '2026-02-03', 'upsert')"
    )
    spec = BitemporalSpec(key=("id",), mode=MODE_DELTA)
    # Feb 2: the source changed Feb 1, but Provisa had recorded only the first version.
    assert [r[:2] for r in _read(con, spec, parse_as_of("2026-02-02"))] == [(1, "a")]


def test_an_as_of_is_a_point_or_a_range_and_nothing_else():
    assert parse_as_of("2026-01-01T00:00:00") == AsOf("TIMESTAMP '2026-01-01 00:00:00.000000'")
    window = parse_as_of("2026-01-01/2026-02-01T12:00:00")
    assert window.is_range and window.key() == (
        "TIMESTAMP '2026-01-01 00:00:00.000000'/TIMESTAMP '2026-02-01 12:00:00.000000'"
    )
    assert AsOf.from_key(window.key()) == window
    with pytest.raises(ValueError, match="is not after its start"):
        parse_as_of("2026-02-01/2026-01-01")
    for bad in ["2026-01-01/", "a/b", "2026-01-01/2026-02-01/2026-03-01"]:
        with pytest.raises(ValueError):
            parse_as_of(bad)
    with pytest.raises(ValueError, match="not a canonical as-of"):
        AsOf.from_key("TIMESTAMP '2026-01-01'; DROP TABLE x")


def test_a_bitemporal_view_answers_its_read_columns_granted_as_its_clock_column():
    cols = with_read_columns(
        [{"name": "id", "visible_to": ["a"]}, {"name": "updated_at", "visible_to": ["a", "b"]}],
        ["id"],
        "updated_at",
    )
    added = {c["name"]: c["visible_to"] for c in cols[2:]}
    assert added == {"source_from": ["a", "b"], "source_to": ["a", "b"], "recorded_at": ["a", "b"]}
    # named already: kept as given
    again = with_read_columns(cols, ["id"], "updated_at")
    assert [c["name"] for c in again] == [c["name"] for c in cols]


def test_a_config_view_gains_them_and_names_its_key():
    from provisa.core.config_loader import views_as_tables

    raw = {
        "views": [
            {
                "id": "customer_history",
                "domain_id": "d",
                "sql": "SELECT 1",
                "materialize": True,
                "bitemporal_mode": "delta",
                "bitemporal_key": ["id"],
                "bitemporal_source_time": "updated_at",
                "columns": [
                    {"name": "id", "visible_to": ["a"]},
                    {"name": "updated_at", "visible_to": ["a"]},
                ],
            }
        ]
    }
    [entry] = views_as_tables(raw)["tables"]
    assert entry["mv_bitemporal_source_time"] == "updated_at"
    assert [c["name"] for c in entry["columns"]][-3:] == ["source_from", "source_to", "recorded_at"]


def test_a_bitemporal_view_with_no_key_is_refused_by_name():
    from provisa.core.models import Table

    with pytest.raises(ValueError, match="view 'v': a bitemporal view names the key"):
        Table(
            source_id="__derived__",
            domain_id="d",
            schema_name="views",
            table_name="v",
            view_sql="SELECT 1",
            materialize=True,
            mv_bitemporal_mode="snapshot",
            columns=[],
        )


def test_a_flight_ticket_takes_a_range_as_the_header_does():
    from provisa.api.flight.server import _ticket_as_of

    assert (
        _ticket_as_of({"as_of": "2026-01-01/2026-02-01"})
        == parse_as_of("2026-01-01/2026-02-01").key()
    )
