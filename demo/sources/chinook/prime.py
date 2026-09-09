# Copyright (c) 2026 Kenneth Stott
# Canary: 8e3b5c1a-4f7d-4a2e-9b6c-2d1f7e0a5c38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prime the optional Chinook demo source: wait for Postgres, then load the seed the Hasura v2
sample tracks (tests/fixtures/hasura_v2_t1_seed.sql). Idempotent — the seed drops and recreates.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

import asyncpg

PORT = int(os.environ.get("PROVISA_DEMO_CHINOOK_PORT", "25433"))
SEED = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "hasura_v2_t1_seed.sql"


async def _prime() -> None:
    deadline = time.monotonic() + 60
    while True:
        try:
            conn = await asyncpg.connect(
                host="localhost", port=PORT, user="provisa", password="provisa", database="chinook"
            )
            break
        except OSError as exc:
            if time.monotonic() > deadline:
                raise SystemExit(f"chinook postgres not reachable on {PORT}: {exc}")
            await asyncio.sleep(1)
    try:
        await conn.execute(SEED.read_text())
        n = await conn.fetchval("SELECT count(*) FROM albums")
    finally:
        await conn.close()
    print(f"chinook primed on port {PORT}: {n} albums", flush=True)


if __name__ == "__main__":
    asyncio.run(_prime())
    sys.exit(0)
