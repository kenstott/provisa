# Copyright (c) 2026 Kenneth Stott
# Canary: 6c3fb0a2-8b5f-4e6a-9a8f-9b3c4d1e2f70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prime the optional MariaDB demo source: wait for the server, then create widgets(id, name) + 3
rows. Idempotent (DROP TABLE IF EXISTS first)."""

from __future__ import annotations

import os
import sys
import time

import pymysql

PORT = int(os.environ.get("PROVISA_DEMO_MARIADB_PORT", "23310"))
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]


def main() -> int:
    deadline = time.monotonic() + 60
    conn = None
    while True:
        try:
            conn = pymysql.connect(
                host="localhost",
                port=PORT,
                user="root",
                password="provisa",
                database="provisa_demo",
                connect_timeout=5,
            )
            break
        except pymysql.Error:
            if time.monotonic() > deadline:
                print(f"mariadb at localhost:{PORT} did not become ready", file=sys.stderr)
                return 1
            time.sleep(2)
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS widgets")
            cur.execute("CREATE TABLE widgets (id INT, name VARCHAR(64))")
            cur.executemany("INSERT INTO widgets (id, name) VALUES (%s, %s)", _WIDGETS)
        conn.commit()
    finally:
        conn.close()
    print(f"mariadb demo source primed: {len(_WIDGETS)} widgets at localhost:{PORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
