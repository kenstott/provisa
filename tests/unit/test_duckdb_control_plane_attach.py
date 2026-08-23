# Copyright (c) 2026 Kenneth Stott
# Canary: 1e5c7ac6-a382-434b-8bf2-b3d178dc22a3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-native-meta: DuckDBFederationRuntime exposes the tenant control-plane SQLite DB as the
``provisa_admin`` catalog so meta/ops queries (e.g. registered_tables) resolve on the native tier.

On Trino, ``provisa_admin`` is a real attached catalog backed by the Postgres control-plane DB.
On the native DuckDB tier there was no such catalog, causing "Catalog provisa_admin does not
exist" on every meta/ops entity query. The fix attaches the local SQLite tenant DB READ_ONLY
as the ``provisa_admin`` catalog and exposes every table it contains under the schema the
compiler emits (``org_<id>``), providing exact Trino parity without hardcoding a partial list."""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")

from provisa.federation.duckdb_runtime import DuckDBFederationRuntime  # noqa: E402


@pytest.fixture
def sqlite_cp_db(tmp_path: Path) -> Path:
    """A minimal SQLite 'tenant DB' with registered_tables and sources rows."""
    db = tmp_path / "tenant.db"
    con = sqlite3.connect(str(db))
    con.executescript(
        """
        CREATE TABLE registered_tables (
            id INTEGER PRIMARY KEY,
            source_id TEXT,
            schema_name TEXT,
            table_name TEXT
        );
        CREATE TABLE sources (
            id TEXT PRIMARY KEY,
            type TEXT
        );
        INSERT INTO registered_tables VALUES
            (1, 'provisa-admin', 'org_default', 'registered_tables'),
            (2, 'provisa-admin', 'org_default', 'sources');
        INSERT INTO sources VALUES
            ('provisa-admin', 'sqlite'),
            ('my-source', 'postgresql');
        """
    )
    con.commit()
    con.close()
    return db


# --- regression: the binder error that existed before the fix ---


def test_provisa_admin_catalog_missing_without_attach_raises():
    """Without attach_control_plane, DuckDB raises a binder error for provisa_admin.* queries."""
    rt = DuckDBFederationRuntime()
    try:
        with pytest.raises(duckdb.Error, match="provisa_admin"):
            rt._con.execute(
                'SELECT * FROM "provisa_admin"."org_default"."registered_tables"'
            ).fetchall()
    finally:
        rt.close()


# --- core fix ---


def test_attach_control_plane_makes_provisa_admin_queryable(sqlite_cp_db: Path):
    """After attach_control_plane, querying provisa_admin.org_default.registered_tables returns rows."""
    rt = DuckDBFederationRuntime()
    try:
        rt.attach_control_plane(str(sqlite_cp_db), "org_default")
        rows = rt._con.execute(
            'SELECT id, source_id, table_name FROM "provisa_admin"."org_default"."registered_tables"'
        ).fetchall()
        assert len(rows) == 2
        source_ids = {r[1] for r in rows}
        assert source_ids == {"provisa-admin"}
        table_names = {r[2] for r in rows}
        assert "registered_tables" in table_names
        assert "sources" in table_names
    finally:
        rt.close()


def test_attach_control_plane_exposes_all_tables_not_hardcoded_subset(sqlite_cp_db: Path):
    """Every table in the SQLite file is visible under provisa_admin — not a hardcoded partial list."""
    rt = DuckDBFederationRuntime()
    try:
        rt.attach_control_plane(str(sqlite_cp_db), "org_default")
        # Both registered_tables and sources (extra table) must be queryable.
        r1 = rt._con.execute(
            'SELECT count(*) FROM "provisa_admin"."org_default"."sources"'
        ).fetchone()[0]
        assert r1 == 2  # provisa-admin + my-source
    finally:
        rt.close()


def test_attach_control_plane_is_idempotent(sqlite_cp_db: Path):
    """Calling attach_control_plane twice does not raise or duplicate views."""
    rt = DuckDBFederationRuntime()
    try:
        rt.attach_control_plane(str(sqlite_cp_db), "org_default")
        rt.attach_control_plane(str(sqlite_cp_db), "org_default")  # second call: no-op
        rows = rt._con.execute(
            'SELECT count(*) FROM "provisa_admin"."org_default"."registered_tables"'
        ).fetchone()
        assert rows[0] == 2
    finally:
        rt.close()


def test_attach_control_plane_noop_for_memory_db():
    """If the db_path is ':memory:' (test/in-memory tenant DB), the attach is a no-op — no error."""
    rt = DuckDBFederationRuntime()
    try:
        rt.attach_control_plane(":memory:", "org_default")
        # provisa_admin catalog is not created for in-memory paths — no error, just not accessible.
        with pytest.raises(duckdb.Error, match="provisa_admin"):
            rt._con.execute(
                'SELECT * FROM "provisa_admin"."org_default"."registered_tables"'
            ).fetchall()
    finally:
        rt.close()


def test_attach_control_plane_sees_writes_committed_after_the_first_attach(sqlite_cp_db: Path):
    """The attach is re-entrant: a row committed by the control plane after the first attach is
    visible to the next call. The engine reads a snapshot, so a stale snapshot would silently
    serve pre-registration metadata forever."""
    rt = DuckDBFederationRuntime()
    try:
        rt.attach_control_plane(str(sqlite_cp_db), "org_default")
        writer = sqlite3.connect(str(sqlite_cp_db))
        writer.execute(
            "INSERT INTO registered_tables VALUES (3, 'my-source', 'org_default', 'pets')"
        )
        writer.commit()
        writer.close()
        rt.attach_control_plane(str(sqlite_cp_db), "org_default")
        rows = rt._con.execute(
            'SELECT table_name FROM "provisa_admin"."org_default"."registered_tables"'
        ).fetchall()
        assert "pets" in {r[0] for r in rows}
    finally:
        rt.close()


def test_attach_control_plane_survives_a_concurrent_control_plane_writer(sqlite_cp_db: Path):
    """Regression: DuckDB's sqlite extension corrupts a WAL-mode SQLite file that another
    connection is writing and kills the process with SIGBUS. The control-plane file is written
    continuously by SQLAlchemy, so the engine must attach a snapshot, never the live file. Before
    the fix this test crashed the interpreter rather than failing."""
    wal = sqlite3.connect(str(sqlite_cp_db))
    wal.execute("PRAGMA journal_mode=WAL")
    wal.close()

    stop = threading.Event()
    writer_errors: list[str] = []

    def writer() -> None:
        con = sqlite3.connect(str(sqlite_cp_db))
        con.execute("PRAGMA busy_timeout=5000")
        i = 100
        try:
            while not stop.is_set():
                con.execute(
                    "INSERT INTO registered_tables VALUES (?, ?, 'org_default', ?)",
                    (i, "my-source", "x" * 400),
                )
                con.commit()
                i += 1
                # Commit steadily rather than as fast as the disk allows: the engine re-snapshots
                # on every commit it observes, so an unthrottled writer makes this test copy a
                # runaway database instead of exercising the concurrent-read hazard.
                time.sleep(0.005)
        except Exception as exc:  # surfaced below: corruption shows up as a writer failure too
            writer_errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            con.close()

    thread = threading.Thread(target=writer, daemon=True)
    thread.start()
    rt = DuckDBFederationRuntime()
    try:
        for _ in range(50):
            rt.attach_control_plane(str(sqlite_cp_db), "org_default")
            # information_schema.columns enumerates every attached catalog, so it re-reads the
            # attached SQLite database — this is the query that crashed against the live file.
            rt._con.execute("SELECT count(*) FROM information_schema.columns").fetchone()
            rt._con.execute(
                'SELECT id FROM "provisa_admin"."org_default"."registered_tables" LIMIT 1'
            ).fetchone()
    finally:
        stop.set()
        thread.join(timeout=10)
        rt.close()
    assert writer_errors == []


def test_control_plane_refresh_never_empties_a_concurrent_query(sqlite_cp_db: Path):
    """Regression: a query running while the control plane is re-snapshotted came back with ZERO
    ROWS instead of the real result. The rebuild DROPs the org schema CASCADE, DETACHes the snapshot
    alias and replaces the file; a query already bound to those views silently scanned nothing.

    Reproduced in the e2e suite as meta Cypher queries intermittently returning no rows on every
    worker while /data/graph-counts on the same backend reported the real counts. The reader here
    goes through run_sync (an execution path) rather than rt._con directly, because the gate that
    excludes readers from the rebuild is what the execution paths take."""
    stop = threading.Event()
    empties: list[str] = []
    errors: list[str] = []

    rt = DuckDBFederationRuntime()
    rt.attach_control_plane(str(sqlite_cp_db), "org_default")

    def writer() -> None:
        """Commits to the control plane — every commit makes the next attach re-snapshot."""
        con = sqlite3.connect(str(sqlite_cp_db))
        con.execute("PRAGMA busy_timeout=5000")
        i = 1000
        try:
            while not stop.is_set():
                con.execute("INSERT INTO sources VALUES (?, 'sqlite')", (f"src-{i}",))
                con.commit()
                i += 1
                time.sleep(0.002)
        except Exception as exc:
            errors.append(f"writer {type(exc).__name__}: {exc}")
        finally:
            con.close()

    def refresher() -> None:
        """The engine's per-query attach — this is what rebuilds the catalog under the reader."""
        try:
            while not stop.is_set():
                rt.attach_control_plane(str(sqlite_cp_db), "org_default")
        except Exception as exc:
            errors.append(f"refresher {type(exc).__name__}: {exc}")

    def reader() -> None:
        try:
            while not stop.is_set():
                rows = rt.run_sync(
                    'SELECT id FROM "provisa_admin"."org_default"."registered_tables"'
                ).rows
                # The fixture's two rows are committed before the run and never deleted, so any
                # result short of two means the reader saw the catalog mid-rebuild.
                if len(rows) < 2:
                    empties.append(f"{len(rows)} rows")
        except Exception as exc:
            errors.append(f"reader {type(exc).__name__}: {exc}")

    threads = [
        threading.Thread(target=writer, daemon=True),
        threading.Thread(target=refresher, daemon=True),
        threading.Thread(target=reader, daemon=True),
        threading.Thread(target=reader, daemon=True),
    ]
    try:
        for t in threads:
            t.start()
        time.sleep(5)
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=10)
        rt.close()
    assert errors == []
    assert empties == [], (
        f"{len(empties)} short reads during a control-plane rebuild: {empties[:5]}"
    )


def test_concurrent_queries_never_steal_each_others_results(sqlite_cp_db: Path):
    """Regression: two queries running at once on the shared DuckDB connection came back with ZERO
    ROWS. A duckdb connection holds ONE pending result — the second execute() replaces the first,
    and the first caller's fetchall() then returns an empty list rather than raising. run() and
    run_arrow() executed on the shared connection; only run_sync/run_arrow_stream took a cursor.

    Observed in the e2e suite as meta/admin queries intermittently returning no rows while the
    control plane demonstrably held the data (a count(*) against the attached snapshot returned no
    row at all, with no exception)."""
    rt = DuckDBFederationRuntime()
    rt.attach_control_plane(str(sqlite_cp_db), "org_default")

    stop = threading.Event()
    empties: list[str] = []
    errors: list[str] = []

    async def _query(sql: str) -> list:
        return (await rt.run(sql)).rows

    def hammer(sql: str, expected: int) -> None:
        import asyncio

        loop = asyncio.new_event_loop()
        try:
            while not stop.is_set():
                rows = loop.run_until_complete(_query(sql))
                if len(rows) != expected:
                    empties.append(f"{sql[:40]}: {len(rows)} rows")
        except Exception as exc:
            errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            loop.close()

    threads = [
        threading.Thread(
            target=hammer,
            args=('SELECT id FROM "provisa_admin"."org_default"."registered_tables"', 2),
            daemon=True,
        ),
        threading.Thread(
            target=hammer,
            args=('SELECT id FROM "provisa_admin"."org_default"."sources"', 2),
            daemon=True,
        ),
        threading.Thread(target=hammer, args=("SELECT * FROM range(500)", 500), daemon=True),
    ]
    try:
        for t in threads:
            t.start()
        time.sleep(3)
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=10)
        rt.close()
    assert errors == []
    assert empties == [], f"{len(empties)} wrong-length results under concurrency: {empties[:5]}"


def test_attach_control_plane_is_read_only(sqlite_cp_db: Path):
    """The SQLite ATTACH is READ_ONLY: DuckDB cannot write to the control-plane DB via provisa_admin."""
    rt = DuckDBFederationRuntime()
    try:
        rt.attach_control_plane(str(sqlite_cp_db), "org_default")
        # Writes through the view are not possible (view over a read-only attach).
        with pytest.raises(Exception):
            rt._con.execute(
                'INSERT INTO "provisa_admin"."org_default"."registered_tables" '
                "VALUES (99, 'x', 'org_default', 'x')"
            )
    finally:
        rt.close()
