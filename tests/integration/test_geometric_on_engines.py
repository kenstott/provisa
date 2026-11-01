# Copyright (c) 2026 Kenneth Stott
# Canary: e35a71c9-08b4-4d6e-8f23-b16d4c09a7f2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The geometric forms on each engine (REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED): first that the
recording is what postgres:17.11 answers today, then each statement of geometric_corpus as Provisa
writes it, against that recording, one test to a function."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import os
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import duckdb
import psycopg
import pytest

from tests.unit.pgwire.geometric_replay import BY_FUNCTION, problems
from tests.unit.pgwire.pg17_geometric_answers import RECORDED
from tests.unit.pgwire.record_pg17_geometric_answers import recorded, written

pytestmark = [pytest.mark.integration]

_IMAGE = "postgres:17.11"
_RESULTS = Path("results")


def _wait(dsn: str) -> None:
    deadline = time.monotonic() + 60
    while True:
        try:
            psycopg.connect(dsn).close()
            return
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


def test_the_recording_is_what_postgresql_17_answers():
    run = ["docker", "run", "-d", "-e", "POSTGRES_PASSWORD=pg17", "-p", "127.0.0.1::5432", _IMAGE]
    container = subprocess.run(run, check=True, capture_output=True, text=True).stdout.strip()
    try:
        port = (
            subprocess.run(
                ["docker", "port", container, "5432/tcp"],
                check=True,
                capture_output=True,
                text=True,
            )
            .stdout.split(":")[-1]
            .strip()
        )
        dsn = f"postgresql://postgres:pg17@127.0.0.1:{port}/postgres"
        _wait(dsn)
        answers = recorded(dsn)
    finally:
        subprocess.run(["docker", "rm", "-f", container], check=True, capture_output=True)
    _RESULTS.mkdir(exist_ok=True)
    (_RESULTS / "pg17_geometric_answers.py").write_text(written(answers))
    assert answers == RECORDED


def _check(function: str, dialect: str, run: Callable[[str], Any]) -> list[str]:
    wrong, refused = problems(function, dialect, run)
    print("REFUSED", dialect, function, len(refused))  # noqa: T201 -- the run's record
    assert not wrong, f"{len(wrong)} differ on {dialect}: " + "\n".join(wrong[:30])
    return refused


@pytest.mark.parametrize("function", sorted(BY_FUNCTION))
def test_geometric_forms_on_duckdb(function: str):
    connection = duckdb.connect()
    run = lambda sql: connection.execute(sql).fetchone()[0]  # noqa: E731
    assert _check(function, "duckdb", run) == []


@pytest.mark.requires_trino
@pytest.mark.parametrize("function", sorted(BY_FUNCTION))
def test_geometric_forms_on_trino(trino_conn, function: str):
    def run(sql: str) -> Any:
        cursor = trino_conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()[0][0]

    _check(function, "trino", run)


@pytest.mark.requires_clickhouse
@pytest.mark.parametrize("function", sorted(BY_FUNCTION))
def test_geometric_forms_on_clickhouse(function: str):
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    _check(function, "clickhouse", lambda sql: client.query(sql).result_rows[0][0])
