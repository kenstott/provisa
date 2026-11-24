# Copyright (c) 2026 Kenneth Stott
# Canary: 9e4b7d21-3a6c-4f80-b5d2-1c8e6f0a7b39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: bytea LIKE on a live openGauss answers as PostgreSQL (REQ-2236, amended 2026-11-24).

openGauss matches bytea LIKE as multibyte text on a UTF-8 database and reads past a value ending
in an incomplete UTF-8 lead byte ('\\xdeadbeef' ends in 0xEF), so its answer varies from run to
run. The openGauss engine's transpile writes those calls as rawlike / rawnlike over raw casts.
Each statement is transpiled as the openGauss engine does and run many times on the pinned
openGauss image; every run must give PostgreSQL's answer. One test, so the container starts once.
"""

# Requirements: REQ-2236

from __future__ import annotations

import os
import time

import psycopg2
import pytest

from provisa.transpiler.transpile import transpile

pytestmark = [pytest.mark.integration, pytest.mark.requires_opengauss]

# Each statement and PostgreSQL's answer to it.
_CASES = [
    (r"""SELECT pg_catalog."like"('\xdeadbeef'::bytea, '\xdeadbeef'::bytea) AS v""", True),
    (r"""SELECT pg_catalog."notlike"('\xdeadbeef'::bytea, '\xdeadbeef'::bytea) AS v""", False),
    (r"""SELECT pg_catalog."bytealike"('\xdeadbeef'::bytea, '\xdeadbeef'::bytea) AS v""", True),
    (r"""SELECT pg_catalog."byteanlike"('\xdeadbeef'::bytea, '\xdeadbeef'::bytea) AS v""", False),
    (r"""SELECT '\xdeadbeef'::bytea ~~ '\xdeadbeef'::bytea AS v""", True),
    (r"""SELECT '\xdeadbeef'::bytea !~~ '\xdeadbeef'::bytea AS v""", False),
    # A pattern: 0x25 is '%', matched byte-wise.
    (r"""SELECT pg_catalog."bytealike"('\xdeadbeef'::bytea, '\xde25'::bytea) AS v""", True),
    (r"""SELECT pg_catalog."bytealike"('\xdeadbeef'::bytea, '\xad25'::bytea) AS v""", False),
]
# How many times each statement runs: the engine's own form flipped within a handful of runs.
_RUNS = 50


def _connect():
    # The image's remote-login user gaussdb, with the GS_PASSWORD docker-compose.test.yml gives it.
    # Retried while the database initialises after the container starts.
    deadline = time.monotonic() + 300
    while True:
        try:
            return psycopg2.connect(
                host="localhost",
                port=int(os.environ["OPENGAUSS_PORT"]),
                dbname="postgres",
                user="gaussdb",
                password="Provisa_2026!",
            )
        except psycopg2.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(3)


def test_bytea_like_answers_as_postgres_on_opengauss() -> None:
    conn = _connect()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            # The rewrite's premise: openGauss casts a bytea to raw.
            cur.execute(r"SELECT CAST('\xdeadbeef'::bytea AS raw)::text")
            assert cur.fetchone() is not None
            wrong: list[str] = []
            for statement, expected in _CASES:
                written = transpile(statement, "postgres", generated=False, engine_kind="opengauss")
                assert "rawlike" in written.lower() or "rawnlike" in written.lower(), written
                answers = set()
                for _ in range(_RUNS):
                    cur.execute(written)
                    row = cur.fetchone()
                    assert row is not None
                    answers.add(row[0])
                if answers != {expected}:
                    wrong.append(f"{written}: {sorted(answers)}, PostgreSQL {expected}")
            assert not wrong, "\n".join(wrong)
    finally:
        conn.close()
