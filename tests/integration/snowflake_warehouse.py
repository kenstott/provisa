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


def _row(cur, warehouse: str) -> dict:
    """``warehouse``'s SHOW WAREHOUSES row, its columns by lower-case name."""
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
    return matches[0]


def _find(cur, warehouse: str) -> tuple[str, str]:
    """``(name as Snowflake spells it, state)`` of ``warehouse``."""
    row = _row(cur, warehouse)
    return str(row["name"]), str(row["state"]).upper()


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


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
                cur.execute(f"ALTER WAREHOUSE {_ident(name)} SUSPEND")
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


def set_auto_suspend(warehouse: str, seconds: int) -> int:
    """Set ``warehouse`` to suspend itself after ``seconds`` idle, read the setting back, and
    return it (maintainer 2026-10-29: a CI run that dies before its teardown cannot leave the
    warehouse billing for longer than that). The ALTER resumes nothing."""
    conn = _connect()
    try:
        cur = conn.cursor()
        try:
            name = str(_row(cur, warehouse)["name"])
            cur.execute(f"ALTER WAREHOUSE {_ident(name)} SET AUTO_SUSPEND = {int(seconds)}")
            found = int(_row(cur, warehouse)["auto_suspend"])
            if found != int(seconds):
                raise RuntimeError(
                    f"Snowflake warehouse {name!r} suspends after {found}s idle after it was set "
                    f"to {seconds}s"
                )
            return found
        finally:
            cur.close()
    finally:
        conn.close()


def cap_monthly_credits(warehouse: str, monitor: str, credits: int) -> str:
    """Put ``warehouse`` under the resource monitor ``monitor``: at most ``credits`` credits a
    calendar month, then the warehouse is suspended at once (maintainer 2026-10-29: CI must not
    run up the warehouse bill). A warehouse already under ``monitor`` is left as it is. Creating
    a resource monitor and assigning one to a warehouse need the ACCOUNTADMIN role; a user
    without it gets Snowflake's own refusal, raised as it is. Returns the monitor's name."""
    conn = _connect()
    try:
        cur = conn.cursor()
        try:
            row = _row(cur, warehouse)
            if str(row["resource_monitor"]).upper() == monitor.upper():
                return monitor.upper()
            cur.execute(
                f"CREATE RESOURCE MONITOR IF NOT EXISTS {_ident(monitor.upper())} "
                f"WITH CREDIT_QUOTA = {int(credits)} FREQUENCY = MONTHLY "
                "START_TIMESTAMP = IMMEDIATELY TRIGGERS ON 100 PERCENT DO SUSPEND_IMMEDIATE"
            )
            cur.execute(
                f"ALTER WAREHOUSE {_ident(str(row['name']))} "
                f"SET RESOURCE_MONITOR = {_ident(monitor.upper())}"
            )
            found = str(_row(cur, warehouse)["resource_monitor"]).upper()
            if found != monitor.upper():
                raise RuntimeError(
                    f"Snowflake warehouse {row['name']!r} is under resource monitor {found!r} "
                    f"after it was assigned {monitor.upper()!r}"
                )
            return found
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
