# Copyright (c) 2026 Kenneth Stott
# Canary: 9b2e4f6a-1c8d-4e0b-9a3f-5d6c7e8f9a0b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prime the optional CockroachDB demo source: wait for the server, then create widgets(id, name)
+ 3 rows. Idempotent (DROP TABLE IF EXISTS first)."""

from __future__ import annotations

import os
import sys
import time

import psycopg2

PORT = int(os.environ.get("PROVISA_DEMO_COCKROACHDB_PORT", "26258"))
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]


def main() -> int:
    deadline = time.monotonic() + 60
    conn = None
    while True:
        try:
            conn = psycopg2.connect(
                host="localhost",
                port=PORT,
                user="root",
                password="",
                dbname="defaultdb",
                connect_timeout=5,
            )
            break
        except psycopg2.Error:
            if time.monotonic() > deadline:
                print(f"cockroachdb at localhost:{PORT} did not become ready", file=sys.stderr)
                return 1
            time.sleep(2)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS widgets")
            cur.execute("CREATE TABLE widgets (id INT PRIMARY KEY, name STRING)")
            cur.executemany("INSERT INTO widgets (id, name) VALUES (%s, %s)", _WIDGETS)
    finally:
        conn.close()
    print(f"cockroachdb demo source primed: {len(_WIDGETS)} widgets at localhost:{PORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
