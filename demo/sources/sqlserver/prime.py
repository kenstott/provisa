# Copyright (c) 2026 Kenneth Stott
# Canary: 6f7a8b9c-0d1e-4f2a-9b3c-4d5e6f7a8b9c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prime the optional SQL Server demo source: wait for the server, then create widgets(id, name)
+ 3 rows. Idempotent (drop if exists first). Requires pyodbc + unixODBC + "ODBC Driver 18 for SQL
Server" on the HOST running this script (the same host dependency the app's own driver needs to
connect at all) — importorskip mirrors tests/integration/test_sqlserver_source_e2e.py."""

from __future__ import annotations

import os
import sys
import time

import pyodbc

PORT = int(os.environ.get("PROVISA_DEMO_SQLSERVER_PORT", "21433"))
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]


def main() -> int:
    deadline = time.monotonic() + 90
    conn = None
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER=localhost,{PORT};"
        "UID=sa;PWD=Provisa_2026!;Encrypt=yes;TrustServerCertificate=yes"
    )
    while True:
        try:
            conn = pyodbc.connect(conn_str, autocommit=True)
            break
        except pyodbc.Error:
            if time.monotonic() > deadline:
                print(f"sqlserver at localhost:{PORT} did not become ready", file=sys.stderr)
                return 1
            time.sleep(3)
    try:
        cur = conn.cursor()
        cur.execute("IF OBJECT_ID('widgets', 'U') IS NOT NULL DROP TABLE widgets")
        cur.execute("CREATE TABLE widgets (id INT PRIMARY KEY, name NVARCHAR(64))")
        cur.executemany("INSERT INTO widgets (id, name) VALUES (?, ?)", _WIDGETS)
    finally:
        conn.close()
    print(f"sqlserver demo source primed: {len(_WIDGETS)} widgets at localhost:{PORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
