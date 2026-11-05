# Copyright (c) 2026 Kenneth Stott
# Canary: 3b9e71c4-d05a-4c28-a6f3-8e1b27d4f096
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Writes pg17_system_answers.py: each statement of system_corpus, answered by a real PostgreSQL 17
in its default state (REQ-2171). tests/integration/test_pg17_system_recording.py runs this on
postgres:17.11 in CI, since no PostgreSQL runs on a worker's machine."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import sys
from pathlib import Path
from pprint import pformat
from typing import Any

import psycopg

from tests.unit.pgwire.record_pg17_function_rules import _answer
from tests.unit.pgwire.system_corpus import statements

ANSWERS = Path(__file__).with_name("pg17_system_answers.py")
_HEADER = '''# Copyright (c) 2026 Kenneth Stott
# Canary: 7c0d42e8-b16f-4e93-a572-d9c38f1e6a04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What PostgreSQL 17 (postgres:17.11, in its default state) answered for system_corpus: statement
-> ("ok", type oid, rows) or ("err", SQLSTATE, message). Written by
record_pg17_system_answers.py, never by hand."""

# Requirements: REQ-2167, REQ-2171
# fmt: off

from __future__ import annotations

from typing import Any

RECORDED: dict[str, tuple[Any, Any, Any]] = '''


def recorded(dsn: str) -> dict[str, tuple[Any, Any, Any]]:
    with psycopg.connect(dsn, autocommit=True) as connection:
        version = connection.execute("SHOW server_version").fetchone()
        assert version is not None and version[0].startswith("17."), version
        return {sql: _answer(connection, sql) for sql in statements()}


def written(answers: dict[str, tuple[Any, Any, Any]]) -> str:
    return _HEADER + pformat(answers, width=100, sort_dicts=False) + "\n"


if __name__ == "__main__":
    ANSWERS.write_text(written(recorded(sys.argv[1])))
