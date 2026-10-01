# Copyright (c) 2026 Kenneth Stott
# Canary: 68a16a5b-1707-4deb-91a2-77202f627c93
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


class _Ev:
    def __init__(self, operation, row):
        self.operation = operation
        self.row = row


def _cdc(con, events):
    from provisa.federation.store_connection import apply_cdc_duckdb_native

    return apply_cdc_duckdb_native(
        con,
        catalog="mat_store",
        schema="mat",
        table="pets",
        columns=[("id", "bigint"), ("s", "text")],
        pk_columns=["id"],
        events=events,
    )


def _rows(con):
    return con.execute('SELECT id, s FROM mat_store.mat."pets" ORDER BY id').fetchall()


def test_apply_cdc_leaves_each_key_at_its_last_event_in_stream_order(store_con):
    """REQ-1733: the set-based apply equals applying every event one at a time in order."""
    _cdc(store_con, [_Ev("insert", {"id": 1, "s": "a"}), _Ev("insert", {"id": 2, "s": "b"})])
    counts = _cdc(
        store_con,
        [
            _Ev("update", {"id": 1, "s": "a1"}),
            _Ev("update", {"id": 1, "s": "a2"}),  # last upsert wins
            _Ev("delete", {"id": 2}),
            _Ev("insert", {"id": 2, "s": "b2"}),  # delete then re-insert -> present
            _Ev("insert", {"id": 3, "s": "c"}),
            _Ev("delete", {"id": 3}),  # insert then delete -> absent
            _Ev("delete", {"id": 9}),  # tombstone for a key never landed
        ],
    )
    assert _rows(store_con) == [(1, "a2"), (2, "b2")]
    assert counts == {"upsert": 4, "delete": 3}


def test_apply_cdc_lands_a_large_batch_set_based(store_con):
    """Confirmed live: per-event DELETE+INSERT took over ten minutes for ~3M keyed rows."""
    import time

    _cdc(store_con, [_Ev("insert", {"id": i, "s": "old"}) for i in range(0, 200_000, 2)])
    t0 = time.monotonic()
    _cdc(store_con, [_Ev("insert", {"id": i, "s": "new"}) for i in range(200_000)])
    assert time.monotonic() - t0 < 60.0  # per-event statements: minutes
    assert store_con.execute(
        'SELECT count(*), count(DISTINCT id), min(s), max(s) FROM mat_store.mat."pets"'
    ).fetchone() == (200_000, 200_000, "new", "new")


def test_read_row_cache_reports_each_present_key_at_its_earliest_expiry(store_con):
    """REQ-1865: keys join as a registered frame (a 1M-key bound IN list took ~18s live); a key
    repeated in the cache (keyed on a non-PK join column) is fresh only while every row is."""
    from datetime import UTC, datetime

    from provisa.federation.store_connection import read_row_cache_duckdb_native

    store_con.execute("CREATE SCHEMA mat_store.rc")
    store_con.execute(
        'CREATE TABLE mat_store.rc.ev (event_id BIGINT, order_id BIGINT, "_row_expires_at" TIMESTAMP)'
    )
    early, late = datetime(2026, 1, 1), datetime(2026, 1, 2)
    store_con.execute(
        "INSERT INTO mat_store.rc.ev VALUES (1, 10, ?), (2, 10, ?), (3, 20, ?)", [late, early, late]
    )
    got = read_row_cache_duckdb_native(
        store_con,
        catalog="mat_store",
        schema="rc",
        table="ev",
        pk_columns=["order_id"],
        keys=[(10,), (20,), (30,)],
    )
    assert got == {(10,): early.replace(tzinfo=UTC), (20,): late.replace(tzinfo=UTC)}

    class _Recording:
        """Records each statement's bound-parameter count on the cursor read_row_cache opens."""

        def __init__(self, con):
            self._con = con
            self.bound: list[int] = []

        def cursor(self):
            cur = self._con.cursor()
            outer = self

            class _Cur:
                def __getattr__(self, name):
                    return getattr(cur, name)

                def execute(self, sql, params=None):
                    outer.bound.append(len(params or ()))
                    return cur.execute(sql, params) if params else cur.execute(sql)

            return _Cur()

    rec = _Recording(store_con)
    got = read_row_cache_duckdb_native(
        rec,
        catalog="mat_store",
        schema="rc",
        table="ev",
        pk_columns=["order_id"],
        keys=[(i,) for i in range(100, 100_100)] + [(10,)],
    )
    assert len(got) == 1 and got[(10,)] == early.replace(tzinfo=UTC)
    assert max(rec.bound) <= 3  # the 100k keys are never bound as an IN list


def test_upsert_arrow_replaces_carried_keys_and_keeps_the_rest(store_con):
    """REQ-1865: the row-cache land is columnar -- one DELETE of the carried keys, one INSERT."""
    import pyarrow as pa

    from provisa.federation.store_connection import upsert_arrow_duckdb_native

    cols = [("id", "bigint"), ("s", "text")]

    def _up(data):
        return upsert_arrow_duckdb_native(
            store_con,
            catalog="mat_store",
            schema="mat",
            table="pets",
            columns=cols,
            pk_columns=["id"],
            data=data,
        )

    assert _up(pa.table({"id": [1, 2, 3], "s": ["a", "b", "c"]})) == 3
    assert _up(pa.table({"s": ["b2", "d"], "id": [2, 4]})) == 2  # columns bind by name
    assert _rows(store_con) == [(1, "a"), (2, "b2"), (3, "c"), (4, "d")]


def test_land_row_cache_arrow_stamps_and_lands_through_the_broker(tmp_path):
    """The Arrow land keeps _land_row_cache's stamps (cached now, expires now + ttl, UTC) and
    lands NULL for a declared column the fetch did not return."""
    import asyncio
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace

    import pyarrow as pa

    from provisa.federation.materialize_broker import _SyncedStore
    from provisa.federation.query_residency import _land_row_cache_arrow

    db = str(tmp_path / "store.duckdb")
    broker = _SyncedStore(db)
    runtime = SimpleNamespace(
        ensure_materialize_attached=lambda: None,
        _store_broker=broker,
        _store_is_duckdb=lambda: True,  # a DuckDB engine on an embedded DuckDB store
    )
    backend = SimpleNamespace(dialect="duckdb", _runtime_for=lambda state: runtime)
    before = datetime.now(UTC).replace(tzinfo=None)
    asyncio.run(
        _land_row_cache_arrow(
            None,
            backend,
            None,
            "rc",
            "ev",
            None,
            ["event_id"],
            [("event_id", "bigint"), ("order_id", "bigint"), ("channel", "text")],
            pa.table({"event_id": [1, 2], "order_id": [10, 10]}),
            300,
        )
    )
    con = duckdb.connect(db, read_only=True)
    try:
        rows = con.execute(
            'SELECT event_id, order_id, channel, "_row_cached_at", "_row_expires_at" '
            "FROM rc.ev ORDER BY event_id"
        ).fetchall()
    finally:
        con.close()
    assert [r[:3] for r in rows] == [(1, 10, None), (2, 10, None)]
    for _, _, _, cached, expires in rows:
        assert before <= cached <= datetime.now(UTC).replace(tzinfo=None)
        assert expires - cached == timedelta(seconds=300)
