# Copyright (c) 2026 Kenneth Stott
# Canary: 71d0b8e2-c346-4fa9-a50e-9d2e47c1683b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Writes pg17_aggregate_answers.py: each statement of aggregate_corpus, answered by a real
PostgreSQL 17 (REQ-2171).

    docker run -d --name pg17 -e POSTGRES_PASSWORD=pg17 -p 127.0.0.1::5432 postgres:17.11
    .venv/bin/python -m tests.unit.pgwire.record_pg17_aggregate_answers \\
        postgresql://postgres:pg17@127.0.0.1:<port>/postgres"""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import sys
from pathlib import Path
from pprint import pformat
from typing import Any

import psycopg
from psycopg import pq

from tests.unit.pgwire.aggregate_corpus import index_statements, server_statements, statements

_ANSWERS = Path(__file__).with_name("pg17_aggregate_answers.py")
_HEADER = '''# Copyright (c) 2026 Kenneth Stott
# Canary: 3ba96e17-d025-48c1-b7f4-6c0e12d9a785
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What PostgreSQL 17 (postgres:17.11, UTF8, en_US.utf8, UTC) answered for aggregate_corpus:
statement -> ("ok", the value's text) or ("err", its message). Written by
record_pg17_aggregate_answers.py, never by hand."""

# Requirements: REQ-2167, REQ-2171
# fmt: off

from __future__ import annotations

from typing import Any

RECORDED: dict[str, tuple[str, Any]] = '''


def main(dsn: str) -> None:
    answers: dict[str, tuple[str, Any]] = {}
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("SET extra_float_digits TO 1")
        conn.execute("SET default_transaction_read_only TO on")
        for sql in (*statements(), *index_statements(), *server_statements()):
            res = conn.pgconn.exec_params(sql.encode(), [], result_format=0)
            if res.status != pq.ExecStatus.TUPLES_OK:
                field = res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY)
                answers[sql] = ("err", (field or b"").decode())
                continue
            value = res.get_value(0, 0)
            answers[sql] = ("ok", None if value is None else value.decode())
    _ANSWERS.write_text(_HEADER + pformat(answers, width=100, sort_dicts=False) + "\n")


if __name__ == "__main__":
    main(sys.argv[1])
