# Copyright (c) 2026 Kenneth Stott
# Canary: 5e6f7a8b-9c0d-4e1f-8a2b-3c4d5e6f7a8b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prime the optional ClickHouse demo source: wait for the server, then create widgets(id, name)
+ 3 rows. Idempotent (DROP TABLE IF EXISTS first)."""

from __future__ import annotations

import os
import sys
import time

import clickhouse_connect

PORT = int(os.environ.get("PROVISA_DEMO_CLICKHOUSE_PORT", "28123"))
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]


def main() -> int:
    deadline = time.monotonic() + 60
    client = None
    while True:
        try:
            client = clickhouse_connect.get_client(
                host="localhost", port=PORT, username="default", password="provisa"
            )
            client.command("SELECT 1")
            break
        except Exception:  # noqa: BLE001 - readiness probe, any error means "not yet"
            if time.monotonic() > deadline:
                print(f"clickhouse at localhost:{PORT} did not become ready", file=sys.stderr)
                return 1
            time.sleep(2)
    client.command("DROP TABLE IF EXISTS widgets")
    client.command("CREATE TABLE widgets (id Int64, name String) ENGINE = MergeTree ORDER BY id")
    values = ",".join(f"({i},'{n}')" for i, n in _WIDGETS)
    client.command(f"INSERT INTO widgets VALUES {values}")
    client.close()
    print(f"clickhouse demo source primed: {len(_WIDGETS)} widgets at localhost:{PORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
