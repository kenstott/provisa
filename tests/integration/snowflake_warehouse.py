# Copyright (c) 2026 Kenneth Stott
# Canary: ce9d13e0-47c7-4f9b-8030-5503d39c99e8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Leave the Snowflake warehouse a test uses suspended, as the test found it (REQ-254).

A Snowflake virtual warehouse resumes itself on the first statement that needs compute and bills
by the second until it suspends: on its own only after its AUTO_SUSPEND idle time, which an
account may set long or turn off. A test that runs a statement on a suspended warehouse has
resumed it, so the test suspends it again in teardown, however the test ends.

The state is read before the test runs anything, through a session that names no warehouse (so
the read itself resumes nothing). A warehouse found STARTED, RESUMING or RESIZING is in use by
someone else -- another session, a scheduled task -- and is left running as it was found: REQ-254
covers only the compute the test started.
"""

from __future__ import annotations

import os
from collections.abc import Generator
from contextlib import contextmanager

# States in which the warehouse was running before the test used it.
_ALREADY_RUNNING = ("STARTED", "RESUMING", "RESIZING")
# States in which a suspended warehouse no longer runs compute for new statements.
_SUSPENDED = ("SUSPENDED", "SUSPENDING")


def _connect():
    """A session with no warehouse: SHOW and ALTER WAREHOUSE need none, and none is resumed."""
    import snowflake.connector

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
    )


def _find(cur, warehouse: str) -> tuple[str, str]:
    """``(name as Snowflake spells it, state)`` of ``warehouse``."""
    pattern = warehouse.replace("'", "''")
    cur.execute(f"SHOW WAREHOUSES LIKE '{pattern}'")
    columns = [d[0].lower() for d in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    matches = [r for r in rows if str(r["name"]).upper() == warehouse.upper()]
    if len(matches) != 1:
        raise RuntimeError(
            f"Snowflake warehouse {warehouse!r}: {len(matches)} warehouses of that name are "
            "visible to the test's user, expected exactly one"
        )
    return str(matches[0]["name"]), str(matches[0]["state"]).upper()


def warehouse_state(warehouse: str) -> str:
    """``warehouse``'s state as Snowflake reports it (STARTED, SUSPENDED, ...)."""
    conn = _connect()
    try:
        cur = conn.cursor()
        try:
            return _find(cur, warehouse)[1]
        finally:
            cur.close()
    finally:
        conn.close()


def suspend_warehouse(warehouse: str) -> str:
    """Suspend ``warehouse`` if it is running, and return the state it is left in. A warehouse
    that is not suspended afterwards may be billing, and that raises."""
    conn = _connect()
    try:
        cur = conn.cursor()
        try:
            name, state = _find(cur, warehouse)
            if state not in _SUSPENDED:
                ident = name.replace('"', '""')
                cur.execute(f'ALTER WAREHOUSE "{ident}" SUSPEND')
                state = _find(cur, warehouse)[1]
            if state not in _SUSPENDED:
                raise RuntimeError(
                    f"Snowflake warehouse {name!r} is {state} after a suspend, not suspended: "
                    "it may be billing"
                )
            return state
        finally:
            cur.close()
    finally:
        conn.close()


@contextmanager
def warehouse_in_use(warehouse: str) -> Generator[None, None, None]:
    """Run the block on ``warehouse`` and suspend it again after, however the block ends --
    unless it was already running before the block (REQ-254: the test stops only the compute it
    started; a warehouse in use by someone else is left running as it was found).

    ``warehouse`` is the name the test connects with, so the warehouse suspended is the one the
    test actually ran on."""
    found = warehouse_state(warehouse)
    try:
        yield
    finally:
        if found in _ALREADY_RUNNING:
            # REQ-254: the test did not resume it; leave it running as it was found.
            print(
                f"Snowflake warehouse {warehouse} was {found} before the test: left running",
                flush=True,
            )
        else:
            suspend_warehouse(warehouse)


def drop_database(connection, name: str) -> None:
    """``DROP DATABASE "<name>"`` (its schemas and tables with it) through ``connection``, then
    read back that it is gone (REQ-254). ``name`` is exact: quoted, so case-sensitive."""
    cur = connection.cursor()
    try:
        cur.execute(f'DROP DATABASE IF EXISTS "{name}"')
        cur.execute(f"SHOW DATABASES LIKE '{name}'")
        # SHOW ... LIKE matches without case; the name column (the second) is exact.
        left = [row[1] for row in cur.fetchall() if row[1] == name]
    finally:
        cur.close()
    if left:
        raise RuntimeError(f"Snowflake database {name} still listed after its drop")
