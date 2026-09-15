# Copyright (c) 2026 Kenneth Stott
# Canary: 7f4a1c9e-3b2d-4e8f-9a1c-2d5e6f7a8b9c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prime the optional SAP HANA Express demo source: wait for the tenant DB (HXE) to accept SQL
connections, then create WIDGETS(ID, NAME) + 3 rows. Idempotent (DROP TABLE IF EXISTS first).

HXE's tenant-DB service can take several minutes past "container healthy" to actually accept SQL
connections (SYSTEMDB comes up first, then the HXE tenant is started last) — the retry loop here
budgets for that, not just a TCP-connect wait.

Default port 39041, NOT the commonly-documented 39015: verified live against this exact HXE image
(saplabs/hanaexpress:latest, instance 90) — hdbindexserver binds the HXE tenant's SQL port at
39040 on 127.0.0.1 (container-internal only) and 39041 on 0.0.0.0 (externally reachable); 39015
was never listening at all. See compose.yml's own module comment for the two separate issues that
had to be untangled to get here (a runc/containerd sysctl rejection, and Apple Silicon/Docker
Desktop's VM never actually starting hdbindexserver regardless).
"""

from __future__ import annotations

import os
import sys
import time

from hdbcli import dbapi

PORT = int(os.environ.get("PROVISA_DEMO_SAPHANA_PORT", "39041"))
PASSWORD = os.environ.get("PROVISA_DEMO_SAPHANA_PASSWORD", "HXEHana1")
HOST = os.environ.get("PROVISA_DEMO_SAPHANA_HOST", "localhost")
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]


def main() -> int:
    deadline = (
        time.monotonic() + 900
    )  # HXE cold start: commonly 5-15 minutes past container-healthy
    conn = None
    last_err: Exception | None = None
    while True:
        try:
            conn = dbapi.connect(
                address=HOST,
                port=PORT,
                user="SYSTEM",
                password=PASSWORD,
                databaseName="HXE",  # type: ignore[call-arg]  # hdbcli's C-extension stub omits
                # this (and every other) MDC tenant-routing connect property even though the
                # runtime accepts it — SAP's own Python driver guide documents databaseName for
                # routing a SYSTEMDB connection to a named tenant.
            )
            break
        except Exception as exc:  # hdbcli raises its own Error type; broad on purpose during retry
            last_err = exc
            if time.monotonic() > deadline:
                print(
                    f"saphana at {HOST}:{PORT} did not become ready: {last_err}",
                    file=sys.stderr,
                )
                return 1
            time.sleep(10)
    try:
        cur = conn.cursor()
        cur.execute('DROP TABLE "SYSTEM"."WIDGETS"') if _table_exists(cur) else None
        cur.execute('CREATE TABLE "SYSTEM"."WIDGETS" (ID INTEGER, NAME NVARCHAR(64))')
        cur.executemany('INSERT INTO "SYSTEM"."WIDGETS" (ID, NAME) VALUES (?, ?)', _WIDGETS)
        conn.commit()
        cur.close()
    finally:
        conn.close()
    print(f"saphana demo source primed: {len(_WIDGETS)} widgets at {HOST}:{PORT}")
    return 0


def _table_exists(cur) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM TABLES WHERE SCHEMA_NAME = 'SYSTEM' AND TABLE_NAME = 'WIDGETS'"
    )
    return cur.fetchone()[0] > 0


if __name__ == "__main__":
    raise SystemExit(main())
