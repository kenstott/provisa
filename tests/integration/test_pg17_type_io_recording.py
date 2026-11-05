# Copyright (c) 2026 Kenneth Stott
# Canary: 91b3e7c2-0d46-4a1f-8e25-6fc0a7d39b12
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL 17 itself answers type_io_corpus (REQ-2171), and the answers replayed elsewhere are
its answers: postgres:17.11 starts here, each statement runs on it, and what it says is held against
pg17_type_io_answers.py. A difference writes the whole answer file under results/, which CI uploads."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import os
import subprocess
import time
import uuid
from pathlib import Path
from pprint import pformat

import psycopg
import pytest
from psycopg import pq

from tests.unit.pgwire.type_io_corpus import SESSION, SETUP, statements

pytestmark = [pytest.mark.integration]

_IMAGE = "postgres:17.11"
_READY_S = 120
_HEADER = '''# Copyright (c) 2026 Kenneth Stott
# Canary: 5f1a7c34-92e8-4b06-a3d1-07ce8f46b2d9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What postgres:17.11 answered for type_io_corpus: statement -> (type oid, the value's text), or
(None, "SQLSTATE: message") for what it refuses. Written by test_pg17_type_io_recording.py, never by
hand."""

# Requirements: REQ-2167, REQ-2171
# fmt: off

from __future__ import annotations

RECORDED: dict[str, tuple[int | None, str | None]] = '''


def _answer(conn: psycopg.Connection, sql: str) -> tuple[int | None, str | None]:
    res = conn.pgconn.exec_params(sql.encode(), [], result_format=0)
    if res.status != pq.ExecStatus.TUPLES_OK:
        state = (res.error_field(pq.DiagnosticField.SQLSTATE) or b"").decode()
        said = (res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY) or b"").decode()
        return None, f"{state}: {said}"
    value = res.get_value(0, 0)
    return res.ftype(0), None if value is None else value.decode()


@pytest.fixture(scope="module")
def postgres():
    name = f"pg17-type-io-{uuid.uuid4().hex[:8]}"
    subprocess.run(
        ["docker", "run", "-d", "--name", name, "-e", "POSTGRES_PASSWORD=pg17", "-p",
         "127.0.0.1::5432", _IMAGE],
        check=True, capture_output=True, text=True,
    )  # fmt: skip
    try:
        port = subprocess.run(
            ["docker", "port", name, "5432/tcp"], check=True, capture_output=True, text=True
        ).stdout.split(":")[-1].strip()  # fmt: skip
        deadline = time.monotonic() + _READY_S
        while True:
            try:
                conn = psycopg.connect(
                    f"postgresql://postgres:pg17@127.0.0.1:{port}/postgres", autocommit=True
                )
                break
            except psycopg.OperationalError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)
        with conn:
            yield conn
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)


def test_the_recorded_answers_are_postgresql_17s(postgres):
    for statement in (*SESSION, *SETUP):
        postgres.execute(statement)
    answers = {sql: _answer(postgres, sql) for sql in statements()}
    try:
        from tests.unit.pgwire.pg17_type_io_answers import RECORDED
    except ImportError:
        RECORDED = {}
    if answers != RECORDED:
        written = Path("results") / os.environ.get("LANE_SLUG", "") / "pg17_type_io_answers.py"
        written.parent.mkdir(parents=True, exist_ok=True)
        written.write_text(_HEADER + pformat(answers, width=100, sort_dicts=False) + "\n")
    assert answers == RECORDED, f"recorded answers differ: {written}"
