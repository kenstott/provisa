# Copyright (c) 2026 Kenneth Stott
# Canary: e35a71c9-08b4-4d6e-8f23-b16d4c09a7f2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The recording the geometric replays check against is what postgres:17.11 answers today (REQ-2171)."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import subprocess
import time
from pathlib import Path

import psycopg
import pytest

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
