# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a1d6c-92e4-4b07-a5c3-6e1d0b9f7a24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a Data Profiler samples a large table AT THE SOURCE (REQ-1934).

Two real Postgres servers (the test's own containers): the SOURCE (databases ``shop`` and ``crm``)
and the ENGINE (the pg federation engine, also control plane and store). On a server booted on the
pg engine, through the one governed pipeline:

* ``wide`` is read by its own source (a DIRECT route): the run samples blocks, TABLESAMPLE SYSTEM
  executed by the source, which reads only the sampled share of the table's rows;
* ``customers`` has a relationship into ``crm``, so its profile statement federates on the engine,
  which reads the source through postgres_fdw: the engine cannot block-sample a foreign table, so
  the run reads key ranges of its integer primary key, each answered from the source's index.

What the source read is measured by the source itself: ``pg_stat_user_tables`` tuple counters,
before and after each run. The row count every run takes first is answered by an index-only scan
(the tables are wide and vacuumed; asserted), so the counters' change is the profile's own read.
"""

# Requirements: REQ-1934

from __future__ import annotations

import contextlib
import os
import subprocess
import tempfile
import time
from pathlib import Path

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

_ROLE = "org_admin"
_ROWS = 100_000
_PAD = 600  # wide rows: the row count is cheaper from the primary key's index than the heap
_BUDGET = 15_000  # cells: 3 profiled columns x 100k rows -> a 5% sample
_TARGET = _BUDGET / (3 * _ROWS)


class _Databases:
    """A SOURCE Postgres (``shop``, ``crm``) and an ENGINE Postgres (``provisa``)."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        self.source_port, self.engine_port = lease_ports(2)
        self._source = f"provisa-itest-sampsrc-{os.getpid()}"
        self._engine = f"provisa-itest-sampeng-{os.getpid()}"

    def url(self, port: int, database: str, driver: str = "") -> str:
        return f"postgresql{driver}://provisa:provisa@127.0.0.1:{port}/{database}"

    def start(self) -> None:
        import psycopg

        env = ["-e", "POSTGRES_USER=provisa", "-e", "POSTGRES_PASSWORD=provisa"]
        publish = [
            f"127.0.0.1:{self.source_port}:{self.source_port}",
            f"127.0.0.1:{self.engine_port}:{self.engine_port}",
        ]
        subprocess.run(
            ["docker", "run", "-d", "--rm", "--name", self._source, *env]
            + ["-e", "POSTGRES_DB=shop", "-p", publish[0], "-p", publish[1], "postgres:16"]
            + ["-c", f"port={self.source_port}"],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["docker", "run", "-d", "--rm", "--name", self._engine, *env]
            + ["-e", "POSTGRES_DB=provisa", "--network", f"container:{self._source}"]
            + ["postgres:16", "-c", f"port={self.engine_port}"],
            check=True,
            capture_output=True,
        )
        for port, database in ((self.source_port, "shop"), (self.engine_port, "provisa")):
            # The image restarts the server once after initdb: ready means it answers repeatedly.
            deadline, answered = time.monotonic() + 120, 0
            while answered < 3:
                try:
                    psycopg.connect(self.url(port, database), connect_timeout=2).close()
                    answered += 1
                except psycopg.OperationalError:
                    answered = 0
                    if time.monotonic() > deadline:
                        raise
                time.sleep(1)
        with psycopg.connect(self.url(self.source_port, "shop"), autocommit=True) as conn:
            conn.execute("CREATE DATABASE crm")
            for table, second in (("wide", "region"), ("customers", "name")):
                conn.execute(
                    f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, {second} TEXT, pad TEXT)"
                )
                conn.execute(
                    f"INSERT INTO {table} SELECT g, 'r' || (g % 7), repeat('x', {_PAD}) "
                    f"FROM generate_series(1, {_ROWS}) g"
                )
                conn.execute(f"VACUUM (ANALYZE) {table}")
        with psycopg.connect(self.url(self.source_port, "crm"), autocommit=True) as conn:
            conn.execute("CREATE TABLE visits (visit_id BIGINT PRIMARY KEY, customer_id BIGINT)")
            conn.execute(
                "INSERT INTO visits SELECT g, (g * 37) % 100000 + 1 FROM generate_series(1, 2000) g"
            )

    def explain_count(self, table: str) -> str:
        import psycopg

        with psycopg.connect(self.url(self.source_port, "shop"), autocommit=True) as conn:
            rows = conn.execute(f"EXPLAIN SELECT COUNT(*) FROM public.{table}").fetchall()
        return "\n".join(r[0] for r in rows)

    def reads(self, table: str) -> tuple[int, int]:
        """``(seq_tup_read, idx_tup_fetch)`` of ``shop.public.<table>`` -- the heap tuples the
        source has read for it by sequential (and sample) scans and through indexes."""
        import psycopg

        with psycopg.connect(self.url(self.source_port, "shop"), autocommit=True) as conn:
            conn.execute("SELECT pg_stat_clear_snapshot()")
            row = conn.execute(
                "SELECT seq_tup_read, COALESCE(idx_tup_fetch, 0) FROM pg_stat_user_tables "
                "WHERE relname = %s",
                (table,),
            ).fetchone()
        assert row is not None, table
        return int(row[0]), int(row[1])

    def settled_reads(self, table: str) -> tuple[int, int]:
        """The counters once every backend that read the table has reported: a backend reports
        when it goes idle, at most ten seconds after its last report."""
        last, stable_since = self.reads(table), time.monotonic()
        deadline = time.monotonic() + 60
        while time.monotonic() - stable_since < 12:
            assert time.monotonic() < deadline, f"{table}: counters never settled"
            time.sleep(1)
            now = self.reads(table)
            if now != last:
                last, stable_since = now, time.monotonic()
        return last

    def stop(self) -> None:
        subprocess.run(
            ["docker", "rm", "-f", self._engine, self._source], check=True, capture_output=True
        )


def _source(sid: str, pg: _Databases, database: str) -> dict:
    return {
        "id": sid,
        "type": "postgresql",
        "host": "127.0.0.1",
        "port": pg.source_port,
        "database": database,
        "username": "provisa",
        "password": "provisa",
    }


def _col(name: str, data_type: str, **extra) -> dict:
    return {"name": name, "data_type": data_type, "visible_to": [_ROLE], **extra}


def _config(pg: _Databases) -> dict:
    member = {"profiler_source_id": "profiler", "schema": "public", "domain_id": "shop"}
    return {
        "federation_engine": "pg",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "shop", "description": "Shop"}],
        # org_admin is the reserved administrative role (REQ-1349): every org has it.
        "roles": [],
        "sources": [
            _source("src", pg, "shop"),
            _source("crm", pg, "crm"),
            {
                "id": "profiler",
                "type": "data_profiler",
                "mapping": {
                    "cron": "0 3 * * *",
                    "sample_above_cells": _BUDGET,
                    "low_cardinality_max": 100,
                },
            },
        ],
        "tables": [
            {
                **member,
                "source_id": "src",
                "table": "wide",
                "columns": [
                    _col("id", "bigint", is_primary_key=True),
                    _col("region", "varchar"),
                    _col("pad", "varchar"),
                ],
            },
            {
                **member,
                "source_id": "src",
                "table": "customers",
                "columns": [
                    _col("id", "bigint", is_primary_key=True),
                    _col("name", "varchar"),
                    _col("pad", "varchar"),
                ],
            },
            {
                "source_id": "crm",
                "table": "visits",
                "schema": "public",
                "domain_id": "shop",
                "columns": [
                    _col("visit_id", "bigint", is_primary_key=True),
                    _col("customer_id", "bigint"),
                ],
            },
        ],
        "relationships": [
            {
                "id": "customer-visits",
                "source_table_id": "customers",
                "target_table_id": "visits",
                "source_column": "id",
                "target_column": "customer_id",
                "cardinality": "one-to-many",
            }
        ],
    }


@contextlib.contextmanager
def _server(pg: _Databases, workdir: str):
    from tests.integration.isolated_server import IsolatedServer

    path = Path(workdir) / "config.yaml"
    path.write_text(yaml.safe_dump(_config(pg)))
    control_plane = pg.url(pg.engine_port, "provisa", "+psycopg")
    srv = IsolatedServer(
        "profiler_source_sampling",
        engine="pg",
        config=str(path),
        control_plane="postgres",
        env={
            "PLATFORM_DATABASE_URL": control_plane,
            "TENANT_DATABASE_URL": control_plane,
            "PROVISA_MATERIALIZE_URL": pg.url(pg.engine_port, "provisa"),
            "PROVISA_REDIS_EMBEDDED": "1",
        },
    )
    try:
        srv.start()
        yield srv
    finally:
        srv.stop_process()


def _call(srv, method: str, path: str) -> object:
    response = httpx.request(
        method,
        f"{srv.base_url}{path}",
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 60,
    )
    assert response.status_code == 200, response.text
    return response.json()


@pytest.fixture(scope="module")
def booted():
    pg = _Databases()
    pg.start()
    try:
        with tempfile.TemporaryDirectory() as workdir, _server(pg, workdir) as srv:
            catalog = _call(srv, "GET", "/admin/profilers/profiler/catalog")
            members = {m["member"]: m["memberId"] for m in catalog}  # type: ignore[union-attr]
            yield pg, srv, members
    finally:
        pg.stop()


def _profile(booted, table: str) -> tuple[dict, int, int]:
    """Run ``table``'s profile now; its runs row and the source's (seq, index) tuple reads."""
    pg, srv, members = booted
    # The premise of the measurement: the count every run takes reads the key's index only.
    plan = pg.explain_count(table)
    assert "Index Only Scan" in plan, plan
    seq0, idx0 = pg.settled_reads(table)
    _call(srv, "POST", f"/admin/tables/{members[table]}/profile-runs")
    seq1, idx1 = pg.settled_reads(table)
    runs = _call(srv, "GET", f"/admin/tables/{members[table]}/profile-runs")
    return runs[0], seq1 - seq0, idx1 - idx0  # type: ignore[index]


def test_a_directly_read_table_is_block_sampled_by_its_source(booted):
    run, seq, idx = _profile(booted, "wide")
    assert (run["status"], run["sample_method"], run["row_count"]) == (
        "succeeded",
        "block",
        _ROWS,
    ), run
    assert run["target_fraction"] == pytest.approx(_TARGET)
    # The source read the sampled blocks' rows only -- a twentieth of the table, not all of it.
    assert seq == run["profiled_rows"], (seq, run)
    assert idx == 0
    assert seq < _ROWS / 4
    # Pages of ~13 rows each, ~380 of them sampled: the realised share is near the target.
    assert 0.5 * _TARGET < run["sample_fraction"] < 1.5 * _TARGET, run
    assert '"percent": 5.0' in run["sample_attempts"], run


def test_a_table_federated_through_postgres_fdw_is_sampled_by_key_ranges(booted):
    run, seq, idx = _profile(booted, "customers")
    assert (run["status"], run["sample_method"], run["row_count"]) == (
        "succeeded",
        "key_range",
        _ROWS,
    ), run
    # No sequential read: every row the source read came through the primary key's index, and
    # only the rows in the ranges (the key extremes come from the index too).
    assert seq == 0, seq
    assert run["profiled_rows"] <= idx <= run["profiled_rows"] + 2, (idx, run)
    assert idx < _ROWS / 4
    # Keys 1..100000 are dense: the ranges hold the target share of the rows.
    assert run["sample_fraction"] == pytest.approx(_TARGET, rel=0.02), run
