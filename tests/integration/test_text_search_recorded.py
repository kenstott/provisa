# Copyright (c) 2026 Kenneth Stott
# Canary: 4e1a7c93-5f28-4d06-b3c9-e80d62f7a315
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The recorded answers of the text search family are postgres:17.11's (REQ-2171): every statement
of text_search_corpus, asked of a real server. A difference prints the recording to write, between
RECORDING lines, base64 of gzip, for the run's log to give back."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import base64
import gzip
import subprocess
import time
import uuid

import psycopg
import pytest

from tests.unit.pgwire.pg17_text_search_answers import RECORDED
from tests.unit.pgwire.record_pg17_text_search import answers, written

pytestmark = [pytest.mark.integration]

_PORT = 55417
_LINE = 160


def _start(name: str) -> str:
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--name", name, "-e", "POSTGRES_PASSWORD=pg17"]
        + ["-p", f"127.0.0.1:{_PORT}:5432", "postgres:17.11"],
        check=True,
        capture_output=True,
    )
    dsn = f"postgresql://postgres:pg17@127.0.0.1:{_PORT}/postgres"
    # The image restarts the server once after initdb: ready means it answers repeatedly.
    deadline, answered = time.monotonic() + 90, 0
    while answered < 3:
        try:
            psycopg.connect(dsn, connect_timeout=2).close()
            answered += 1
        except psycopg.OperationalError:
            answered = 0
            if time.monotonic() > deadline:
                raise
        time.sleep(1)
    return dsn


def test_the_recorded_answers_are_postgresql_17s():
    name = f"pg17-text-search-{uuid.uuid4().hex[:8]}"
    dsn = _start(name)
    try:
        got = answers(dsn)
    finally:
        subprocess.run(["docker", "rm", "-f", name], check=True, capture_output=True)
    if got != RECORDED:
        blob = base64.b64encode(gzip.compress(written(got).encode(), 9)).decode()
        for start in range(0, len(blob), _LINE):
            print("RECORDING", blob[start : start + _LINE])  # noqa: T201 -- the run's log carries it
    differs = [sql for sql, answer in got.items() if RECORDED.get(sql) != answer]
    assert not differs, f"{len(differs)} of {len(got)} differ, the first {differs[:3]}"
