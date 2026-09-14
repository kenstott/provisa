# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prime the optional HiveServer2 demo source: wait for HS2 to accept connections, then create
database `wh` + table `widgets(id, name)` + 3 rows via impyla (PLAIN auth, no credentials — stock
HS2's default). Idempotent (DROP TABLE IF EXISTS first)."""

from __future__ import annotations

import os
import sys
import time

PORT = int(os.environ.get("PROVISA_DEMO_HIVESERVER2_PORT", "23100"))
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]


def main() -> int:
    from impala.dbapi import connect

    deadline = time.monotonic() + 120
    conn = None
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            conn = connect(host="localhost", port=PORT, auth_mechanism="PLAIN")
            break
        except Exception as exc:  # noqa: BLE001 - impyla raises a mix of thrift/socket errors
            last_exc = exc
            time.sleep(3)
    if conn is None:
        print(
            f"hiveserver2 at localhost:{PORT} did not become ready: {last_exc!r}", file=sys.stderr
        )
        return 1
    try:
        cur = conn.cursor()
        try:
            cur.execute("CREATE DATABASE IF NOT EXISTS wh")
            cur.execute("DROP TABLE IF EXISTS wh.widgets")
            cur.execute("CREATE TABLE wh.widgets (id INT, name STRING)")
            for wid, name in _WIDGETS:
                cur.execute(f"INSERT INTO wh.widgets (id, name) VALUES ({wid}, '{name}')")
        finally:
            cur.close()
    finally:
        conn.close()
    print(f"hiveserver2 demo source primed: {len(_WIDGETS)} widgets at localhost:{PORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
