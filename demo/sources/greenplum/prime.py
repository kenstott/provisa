# Copyright (c) 2026 Kenneth Stott
# Canary: 8b9c0d1e-2f3a-4b4c-9d5e-6f7a8b9c0d1e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prime the optional Greenplum demo source: wait for the master to warm up, then create
widgets(id, name) + 3 rows. Idempotent (DROP TABLE IF EXISTS first)."""

from __future__ import annotations

import os
import sys
import time

import psycopg2

PORT = int(os.environ.get("PROVISA_DEMO_GREENPLUM_PORT", "25432"))
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]


def main() -> int:
    deadline = time.monotonic() + 300
    conn = None
    while True:
        try:
            conn = psycopg2.connect(
                host="localhost",
                port=PORT,
                user="gpadmin",
                password="",
                dbname="postgres",
                connect_timeout=5,
            )
            break
        except psycopg2.Error:
            if time.monotonic() > deadline:
                print(f"greenplum at localhost:{PORT} did not become ready", file=sys.stderr)
                return 1
            time.sleep(5)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS widgets")
            cur.execute("CREATE TABLE widgets (id int PRIMARY KEY, name text)")
            cur.executemany("INSERT INTO widgets VALUES (%s, %s)", _WIDGETS)
    finally:
        conn.close()
    print(f"greenplum demo source primed: {len(_WIDGETS)} widgets at localhost:{PORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
