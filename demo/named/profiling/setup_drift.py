# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The profiling demo's setup client (setup.yaml; scripts/run_demo_setup.py runs it between the
profiler's two runs): the CRM's next sync, which writes every US customer's postal code as ZIP+4.

It writes the demo's store directly, as its owner, so the runtime configuration grants no role a
write: the second profile run, and the drift it finds in postal_code, are built into the template.
A code already written as ZIP+4 is left as it is, so a second run of it changes nothing.

Environment: the store's address as config.yaml reads it (PROFILING_PG_HOST, PROFILING_PG_PORT,
PROFILING_PG_NS).
"""

from __future__ import annotations

import os

import psycopg

# The store where config.yaml reads it: its variables, else config.yaml's own defaults (a local
# start sets none; the bundle build sets the port it ran the store on).
HOST = os.environ.get("PROFILING_PG_HOST", "localhost")
PORT = os.environ.get("PROFILING_PG_PORT", "28732")
DATABASE = "profiling" + os.environ["PROFILING_PG_NS"]

ZIP_PLUS_4 = """
UPDATE customers
SET postal_code = postal_code || '-' || lpad((customer_id % 9999)::text, 4, '0')
WHERE country IN ('US', 'USA', 'United States', 'us', 'U.S.') AND postal_code !~ '-'
"""


def main() -> None:
    dsn = f"postgresql://postgres:postgres@{HOST}:{PORT}/{DATABASE}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        changed = conn.execute(ZIP_PLUS_4).rowcount
    print(f"setup_drift: {changed} US postal codes written as ZIP+4")


if __name__ == "__main__":
    main()
