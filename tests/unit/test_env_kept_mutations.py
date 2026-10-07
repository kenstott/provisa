# Copyright (c) 2026 Kenneth Stott
# Canary: a4ffed27-6cfd-467e-9da2-3879df972188
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Reversible environment's kept mutations (REQ-1942): what a mutation is kept as, and a table
read with its change log applied -- the latest version of each key, a deleted key left out."""

# Requirements: REQ-1942

from __future__ import annotations

import duckdb
import pytest
import sqlglot

from provisa.core.env_changes import overlay_sql
from provisa.pgwire.kept_mutations import _reads


def _log(con: duckdb.DuckDBPyConnection, versions: list[tuple]) -> None:
    con.execute('CREATE TABLE log ("__seq" BIGINT, "__op" VARCHAR, id INTEGER, region VARCHAR)')
    con.executemany("INSERT INTO log VALUES (?, ?, ?, ?)", versions)


def test_a_read_shows_the_latest_version_of_each_key_and_drops_deleted_keys():
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE orders AS SELECT * FROM (VALUES (1, 'east'), (2, 'west'), (3, 'north')) t(id, region)"
    )
    _log(
        con,
        [
            (1, "upsert", 1, "south"),
            (2, "upsert", 1, "centre"),  # the later version wins
            (3, "delete", 2, None),
            (4, "upsert", 9, "north"),
            (5, "upsert", 3, "x"),
            (6, "delete", 3, None),  # deleted after being changed
        ],
    )
    sql = overlay_sql("SELECT * FROM orders", "log", ["id"], ["id", "region"])
    rows = con.execute(f"SELECT * FROM ({sql}) s ORDER BY id").fetchall()
    assert rows == [(1, "centre"), (9, "north")]


def test_the_row_filter_reaches_the_kept_versions_too():
    con = duckdb.connect()
    con.execute("CREATE TABLE orders AS SELECT * FROM (VALUES (1, 'east')) t(id, region)")
    _log(con, [(1, "upsert", 9, "west"), (2, "upsert", 8, "east")])
    sql = overlay_sql(
        "SELECT * FROM orders WHERE region = 'east'",
        "log",
        ["id"],
        ["id", "region"],
        "l.region = 'east'",
    )
    assert con.execute(f"SELECT id FROM ({sql}) s ORDER BY id").fetchall() == [(1,), (8,)]


def _parsed(sql: str):
    return sqlglot.parse_one(sql, read="postgres")


def test_an_insert_is_kept_as_the_rows_it_supplies():
    read, op, given = _reads(
        _parsed("INSERT INTO sales.orders (id, region) VALUES (9, 'north')"),
        ["id", "region"],
        ["id"],
    )
    assert op == "upsert" and given == ["id", "region"]
    assert duckdb.connect().execute(read).fetchall() == [(9, "north")]


def test_an_insert_naming_no_key_is_refused():
    with pytest.raises(PermissionError, match="every key column"):
        _reads(_parsed("INSERT INTO sales.orders (region) VALUES ('x')"), ["id", "region"], ["id"])


def test_an_update_is_kept_as_every_row_it_reaches_with_its_values_set():
    read, op, given = _reads(
        _parsed("UPDATE sales.orders SET region = upper(region) WHERE id = 1"),
        ["id", "region"],
        ["id"],
    )
    assert op == "upsert" and given == ["id", "region"]
    assert read == 'SELECT "id", UPPER(region) AS "region" FROM sales.orders WHERE id = 1'


def test_a_delete_is_kept_as_the_keys_it_reaches():
    read, op, given = _reads(
        _parsed("DELETE FROM sales.orders WHERE region = 'west'"), ["id", "region"], ["id"]
    )
    assert (op, given) == ("delete", ["id"])
    assert read == "SELECT \"id\" FROM sales.orders WHERE region = 'west'"


def test_a_merge_is_kept_as_what_each_of_its_clauses_writes():
    """A matched row takes the first matched clause whose condition holds; an unmatched source
    row the first unmatched clause."""
    from provisa.pgwire.kept_mutations import _merge_steps

    con = duckdb.connect()
    con.execute(
        "CREATE TABLE orders AS SELECT * FROM (VALUES (1, 'east'), (2, 'west'), (3, 'north')) "
        "t(id, region)"
    )
    con.execute(
        "CREATE TABLE incoming AS SELECT * FROM (VALUES (1, NULL), (2, 'south'), (7, 'new')) "
        "t(id, region)"
    )
    steps = _merge_steps(
        _parsed(
            "MERGE INTO orders AS t USING incoming AS s ON t.id = s.id "
            "WHEN MATCHED AND s.region IS NULL THEN DELETE "
            "WHEN MATCHED THEN UPDATE SET region = s.region "
            "WHEN NOT MATCHED THEN INSERT (id, region) VALUES (s.id, s.region)"
        ),
        ["id", "region"],
        ["id"],
    )
    got = [(op, sorted(con.execute(read).fetchall())) for read, op, _given in steps]
    assert got == [
        ("delete", [(1,)]),
        ("upsert", [(2, "south")]),  # 1 took the earlier clause
        ("upsert", [(7, "new")]),
    ]


def test_a_merge_inserting_no_key_is_refused():
    from provisa.pgwire.kept_mutations import _merge_steps

    with pytest.raises(PermissionError, match="every key column"):
        _merge_steps(
            _parsed(
                "MERGE INTO orders AS t USING incoming AS s ON t.id = s.id "
                "WHEN NOT MATCHED THEN INSERT (region) VALUES (s.region)"
            ),
            ["id", "region"],
            ["id"],
        )


def test_a_kept_truncate_deletes_the_base_and_every_earlier_version():
    """TRUNCATE then INSERT reads as just the inserts; a version kept before the marker is gone."""
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE orders AS SELECT * FROM (VALUES (1, 'east'), (2, 'west')) t(id, region)"
    )
    _log(
        con,
        [
            (1, "upsert", 5, "early"),  # before the marker: gone with the base
            (2, "truncate", None, None),
            (3, "upsert", 9, "north"),
            (4, "upsert", 1, "after"),
        ],
    )
    sql = overlay_sql("SELECT * FROM orders", "log", ["id"], ["id", "region"])
    rows = con.execute(f"SELECT * FROM ({sql}) s ORDER BY id").fetchall()
    assert rows == [(1, "after"), (9, "north")]


def test_a_truncate_alone_leaves_nothing():
    con = duckdb.connect()
    con.execute("CREATE TABLE orders AS SELECT * FROM (VALUES (1, 'east')) t(id, region)")
    _log(con, [(1, "truncate", None, None)])
    sql = overlay_sql("SELECT * FROM orders", "log", ["id"], ["id", "region"])
    assert con.execute(f"SELECT * FROM ({sql}) s").fetchall() == []


def test_a_truncate_is_kept_as_one_marker_with_nothing_to_read():
    assert _reads(_parsed("TRUNCATE TABLE sales.orders"), ["id", "region"], ["id"]) == (
        "",
        "truncate",
        [],
    )
