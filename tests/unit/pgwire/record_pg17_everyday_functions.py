# Copyright (c) 2026 Kenneth Stott
# Canary: a9c4f1e7-30d2-4b86-93e5-6c1f7d20b4e8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Writes pg17_everyday_function_answers.py: each statement of everyday_function_statements.py,
answered by a real PostgreSQL 17 (REQ-2171).

    docker run -d --name pg17 -e POSTGRES_PASSWORD=pg17 -p 127.0.0.1::5432 postgres:17.11
    .venv/bin/python -m tests.unit.pgwire.record_pg17_everyday_functions \\
        postgresql://postgres:pg17@127.0.0.1:<port>/postgres"""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import sys
from pathlib import Path
from pprint import pformat

import psycopg

from tests.unit.pgwire.everyday_function_statements import PROBES, SETUP, STATEMENTS
from tests.unit.pgwire.record_pg17_function_rules import _answer

_ANSWERS = Path(__file__).with_name("pg17_everyday_function_answers.py")
_HEADER = '''# Copyright (c) 2026 Kenneth Stott
# Canary: 6d7e18b2-c45a-4f09-8e31-b2a4c97f0d15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What PostgreSQL 17 (postgres:17.11, UTF8, en_US.utf8, UTC) answered for
everyday_function_statements.py: statement -> ("ok", type oid, rows) or ("err", SQLSTATE, message).
Real values, each statement over the extended protocol with text results. Written by
record_pg17_everyday_functions.py, never by hand."""

# Requirements: REQ-2167, REQ-2171
# fmt: off

from __future__ import annotations

from typing import Any

RECORDED: dict[str, tuple[Any, Any, Any]] = '''


def main(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as connection:
        version = connection.execute("SHOW server_version").fetchone()
        assert version is not None and version[0].startswith("17."), version
        for statement in SETUP:
            connection.execute(statement)
        recorded = {
            sql: _answer(connection, sql)
            for statements in (*STATEMENTS.values(), *PROBES.values())
            for sql in statements
        }
    _ANSWERS.write_text(_HEADER + pformat(recorded, width=100, sort_dicts=False) + "\n")
    print(f"{len(recorded)} statements -> {_ANSWERS}")


if __name__ == "__main__":
    main(sys.argv[1])
