# Copyright (c) 2026 Kenneth Stott
# Canary: 8b3e60f4-92d1-4c7a-a6e5-3f71d0c8b542
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Writes pg17_text_search_answers.py: each statement of text_search_corpus, answered by a real
PostgreSQL 17 under function_support's session settings (REQ-2171). Containers run only in CI, so
tests/integration/test_text_search_recorded.py runs this and prints the file when it differs.

    docker run -d --name pg17 -e POSTGRES_PASSWORD=pg17 -p 127.0.0.1::5432 postgres:17.11
    .venv/bin/python -m tests.unit.pgwire.record_pg17_text_search \\
        postgresql://postgres:pg17@127.0.0.1:<port>/postgres"""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import psycopg
from psycopg import pq

from tests.unit.pgwire.text_search_corpus import statements

_ANSWERS = Path(__file__).with_name("pg17_text_search_answers.py")
_SETTINGS = (
    "SET TimeZone TO 'UTC'",
    "SET DateStyle TO 'ISO, MDY'",
    "SET IntervalStyle TO 'postgres'",
    "SET extra_float_digits TO 1",
)
_HEADER = '''# Copyright (c) 2026 Kenneth Stott
# Canary: 61f7d2a8-c04e-4b39-8e1d-a95c37e0b6d4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What PostgreSQL 17 (postgres:17.11, UTF8, en_US.utf8) answered for text_search_corpus: statement
-> ("ok", type oid, rows) or ("err", SQLSTATE, message). Written by record_pg17_text_search.py,
never by hand."""

# Requirements: REQ-2167, REQ-2171
# fmt: off

from __future__ import annotations

from typing import Any

RECORDED: dict[str, tuple[Any, Any, Any]] = {
'''


def _answer(conn: Any, sql: str) -> tuple[Any, Any, Any]:
    res = conn.pgconn.exec_params(sql.encode(), [], result_format=0)
    if res.status == pq.ExecStatus.FATAL_ERROR:
        said = res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY) or b""
        return ("err", res.error_field(pq.DiagnosticField.SQLSTATE).decode(), said.decode())
    rows = [
        tuple(
            None if (v := res.get_value(r, c)) is None else v.decode() for c in range(res.nfields)
        )
        for r in range(res.ntuples)
    ]
    return ("ok", res.ftype(0), rows)


def answers(dsn: str) -> dict[str, tuple[Any, Any, Any]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        for setting in _SETTINGS:
            conn.execute(setting)
        return {sql: _answer(conn, sql) for sql in statements()}


def written(recorded: dict[str, tuple[Any, Any, Any]]) -> str:
    lines = "".join(f"    {sql!r}: {answer!r},\n" for sql, answer in recorded.items())
    return f"{_HEADER}{lines}}}\n"


if __name__ == "__main__":
    _ANSWERS.write_text(written(answers(sys.argv[1])))
