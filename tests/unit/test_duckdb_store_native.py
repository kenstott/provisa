# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-989/REQ-990: the embedded DuckDB materialization store. DuckDB is single-writer per file, so
the store is landed through the ENGINE'S OWN connection (which holds it attached), not a second
connection. ``reconcile_duckdb_native`` converges the landing table (DDL only); ``land_duckdb_native``
lands rows through DuckDB's native columnar ``executemany`` (never per-row), with replace/append
shapes chosen from the change_signal. JSON columns take the source's serialized text directly."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import duckdb
import pytest

from provisa.federation.store_connection import land_duckdb_native, reconcile_duckdb_native

COLS = [("id", "bigint"), ("s", "text"), ("j", "json")]


@pytest.fixture
def store_con():
    d = tempfile.TemporaryDirectory()
    con = duckdb.connect()  # the engine's in-process connection
    con.execute(f"ATTACH '{Path(d.name) / 'store.duckdb'}' AS mat_store")
    try:
        yield con
    finally:
        con.close()
        d.cleanup()


def _count(con):
    return con.execute('SELECT count(*) FROM mat_store.mat."pets"').fetchone()[0]


def test_reconcile_creates_keeps_and_recreates_on_drift(store_con):
    assert (
        reconcile_duckdb_native(
            store_con, catalog="mat_store", schema="mat", table="pets", columns=COLS
        )
        == "created"
    )
    assert (
        reconcile_duckdb_native(
            store_con, catalog="mat_store", schema="mat", table="pets", columns=COLS
        )
        == "kept"
    )
    drift = COLS + [("extra", "integer")]
    assert (
        reconcile_duckdb_native(
            store_con, catalog="mat_store", schema="mat", table="pets", columns=drift
        )
        == "recreated"
    )


def test_land_replace_is_full_refresh(store_con):
    land_duckdb_native(
        store_con,
        catalog="mat_store",
        schema="mat",
        table="pets",
        columns=COLS,
        rows=[{"id": 1, "s": "a", "j": json.dumps({"k": 1})}, {"id": 2, "s": "b", "j": None}],
    )
    assert _count(store_con) == 2
    # a second land REPLACES contents (default ttl signal, no watermark)
    land_duckdb_native(
        store_con,
        catalog="mat_store",
        schema="mat",
        table="pets",
        columns=COLS,
        rows=[{"id": 9, "s": "z", "j": None}],
    )
    assert _count(store_con) == 1
    assert store_con.execute('SELECT id FROM mat_store.mat."pets"').fetchone()[0] == 9


def test_land_append_shape_amends(store_con):
    land_duckdb_native(
        store_con,
        catalog="mat_store",
        schema="mat",
        table="pets",
        columns=COLS,
        rows=[{"id": 1, "s": "a", "j": None}],
    )
    # a poll signal with a watermark APPENDS the delta rather than replacing
    land_duckdb_native(
        store_con,
        catalog="mat_store",
        schema="mat",
        table="pets",
        columns=COLS,
        rows=[{"id": 2, "s": "b", "j": None}],
        change_signal="poll",
        watermark_column="id",
    )
    assert _count(store_con) == 2


def test_json_column_lands_as_parsed_json(store_con):
    land_duckdb_native(
        store_con,
        catalog="mat_store",
        schema="mat",
        table="pets",
        columns=COLS,
        rows=[{"id": 1, "s": "a", "j": json.dumps({"k": 7})}],
    )
    val = store_con.execute(
        "SELECT json_extract_string(j, '$.k') FROM mat_store.mat.\"pets\""
    ).fetchone()[0]
    assert val == "7"


def test_land_no_rows_creates_empty_table(store_con):
    land_duckdb_native(
        store_con, catalog="mat_store", schema="mat", table="pets", columns=COLS, rows=[]
    )
    assert _count(store_con) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
