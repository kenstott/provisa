# Copyright (c) 2026 Kenneth Stott
# Canary: 6f2b8d17-c041-4e93-b7a5-3d91e0c62f48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Records what PostgreSQL 17 answers for geometric_corpus (REQ-2171), as pg17_geometric_answers.py.
tests/integration/test_geometric_on_engines.py runs it against postgres:17.11 in CI, where containers
run, and leaves the file in results/."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from pprint import pformat

import psycopg
from psycopg import pq

from tests.unit.pgwire.geometric_corpus import statements

HEADER = '''"""What PostgreSQL 17 (postgres:17.11, UTC) answered for geometric_corpus: statement -> (type oid,
the value's text), or (0, "ERROR sqlstate"). Written by record_pg17_geometric_answers.py, never by
hand."""

# Requirements: REQ-2167, REQ-2171
# fmt: off

from __future__ import annotations

RECORDED: dict[str, tuple[int, str | None]] = '''


def recorded(dsn: str) -> dict[str, tuple[int, str | None]]:
    answers: dict[str, tuple[int, str | None]] = {}
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("SET TimeZone TO 'UTC'")
        for sql in statements():
            res = conn.pgconn.exec_params(sql.encode(), [], result_format=0)
            if res.status != pq.ExecStatus.TUPLES_OK:
                state = res.error_field(pq.DiagnosticField.SQLSTATE).decode()
                answers[sql] = (0, f"ERROR {state}")
                continue
            value = res.get_value(0, 0)
            answers[sql] = (res.ftype(0), None if value is None else value.decode())
    return answers


def written(answers: dict[str, tuple[int, str | None]]) -> str:
    return HEADER + pformat(answers, width=100, sort_dicts=False) + "\n"
