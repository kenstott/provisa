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
writes it, against that recording."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import os
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import psycopg
import pytest

from provisa.transpiler.transpile import transpile
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


def _check(run: Callable[[str], Any], dialect: str) -> None:
    wrong: dict[str, Any] = {}
    for sql, (_, want) in RECORDED.items():
        try:
            got = run(transpile(sql, dialect))
        except Exception as raised:  # noqa: BLE001 -- every statement is reported, not the first
            got = "ERROR " + str(raised)
            if (want or "").startswith("ERROR"):
                state = (want or "").split()[1]
                got = want if f"{state}:" in got else got
        text = None if got is None else str(got).replace("True", "t").replace("False", "f")
        if text != want:
            wrong[sql] = (text or "")[:200]
    assert not wrong, f"{len(wrong)} of {len(RECORDED)} {list(wrong.items())[:12]}"


@pytest.mark.requires_trino
def test_geometric_forms_on_trino(trino_conn):
    def run(sql: str) -> Any:
        cursor = trino_conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()[0][0]

    _check(run, "trino")


@pytest.mark.requires_clickhouse
def test_geometric_forms_on_clickhouse():
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    _check(lambda sql: client.query(sql).result_rows[0][0], "clickhouse")
