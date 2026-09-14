# Copyright (c) 2026 Kenneth Stott
# Canary: 7a8b9c0d-1e2f-4a3b-9c4d-5e6f7a8b9c0d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prime the optional Oracle demo source: wait for the server, then create widgets(id, name) + 3
rows. Oracle has no DROP TABLE IF EXISTS, so ORA-00942 (table absent) is swallowed like
tests/integration/test_oracle_source_e2e.py does."""

from __future__ import annotations

import os
import sys
import time

import oracledb

PORT = int(os.environ.get("PROVISA_DEMO_ORACLE_PORT", "21521"))
_SERVICE_NAME = "FREEPDB1"
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]


def _connect():
    return oracledb.connect(
        user="system",
        password="provisa",
        dsn=oracledb.makedsn("localhost", PORT, service_name=_SERVICE_NAME),
    )


def main() -> int:
    deadline = time.monotonic() + 240
    conn = None
    while True:
        try:
            conn = _connect()
            break
        except oracledb.Error:
            if time.monotonic() > deadline:
                print(f"oracle at localhost:{PORT} did not become ready", file=sys.stderr)
                return 1
            time.sleep(5)
    conn.autocommit = True
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE widgets';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN
                            RAISE;
                        END IF;
                END;
                """
            )
            cur.execute("CREATE TABLE widgets (id NUMBER PRIMARY KEY, name VARCHAR2(64))")
            cur.executemany("INSERT INTO widgets VALUES (:1, :2)", _WIDGETS)
        finally:
            cur.close()
    finally:
        conn.close()
    print(f"oracle demo source primed: {len(_WIDGETS)} widgets at localhost:{PORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
