# Copyright (c) 2026 Kenneth Stott
# Canary: 0d9a4b72-6f13-4e88-a2c5-7b1e3f60d845
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Writes pg17_hash_answers.py: each statement of hash_corpus, answered by a real PostgreSQL 17
(REQ-2171). A "char" past 127 is left out: its answer is the server's platform's.

    docker run -d --name pg17 -e POSTGRES_PASSWORD=pg17 -p 127.0.0.1::5432 postgres:17.11
    .venv/bin/python -m tests.unit.pgwire.record_pg17_hash_answers \\
        postgresql://postgres:pg17@127.0.0.1:<port>/postgres"""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import sys
from pathlib import Path
from pprint import pformat

import psycopg
from psycopg import pq

from tests.unit.pgwire.hash_corpus import ENUM, statements

_ANSWERS = Path(__file__).with_name("pg17_hash_answers.py")
_HEADER = '''# Copyright (c) 2026 Kenneth Stott
# Canary: 4ac8e215-1b67-4d03-9f7e-c3d0a6b2f178
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What PostgreSQL 17 (postgres:17.11, UTF8, en_US.utf8, UTC) answered for hash_corpus: statement
-> (type oid, the value's text). Written by record_pg17_hash_answers.py, never by hand."""

# Requirements: REQ-2167, REQ-2171
# fmt: off

from __future__ import annotations

RECORDED: dict[str, tuple[int, str | None]] = '''


def main(dsn: str) -> None:
    answers: dict[str, tuple[int, str | None]] = {}
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("SET TimeZone TO 'UTC'")
        conn.execute(f"DROP TYPE IF EXISTS {ENUM}")
        conn.execute(f"CREATE TYPE {ENUM} AS ENUM ('a')")
        for sql in statements():
            res = conn.pgconn.exec_params(sql.encode(), [], result_format=0)
            if res.status != pq.ExecStatus.TUPLES_OK:
                raise SystemExit(f"{sql}: {res.error_message.decode()}")
            value = res.get_value(0, 0)
            answers[sql] = (res.ftype(0), None if value is None else value.decode())
    _ANSWERS.write_text(_HEADER + pformat(answers, width=100, sort_dicts=False) + "\n")


if __name__ == "__main__":
    main(sys.argv[1])
