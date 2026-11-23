# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2e9a41-3b5d-4f86-a0e1-9d4b6c2f8e17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An acquire scopes its connection to the org's search path and resets it on release, and a read
made on every request (REQ-2176's graph-session row) reads outside any transaction: each sent
alone, in autocommit, one round trip where BEGIN, the statement and COMMIT were three. The
connection is back in its transactional mode for every other statement."""

# Requirements: REQ-2176

from __future__ import annotations

import pytest
from sqlalchemy import event, func, select

from provisa.core.schema_org import graph_sessions

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


@pytest.fixture
def sent(control_plane):
    """The org handle and every statement sent on it, with whether autocommit was on for it."""
    db = control_plane.org(holds="state")
    statements: list[tuple[str, bool]] = []

    def seen(conn, _cursor, statement, _params, _context, _many):
        statements.append((statement, conn.connection.driver_connection.autocommit))

    event.listen(db.engine, "before_cursor_execute", seen)
    yield db, statements
    event.remove(db.engine, "before_cursor_execute", seen)


async def test_the_search_path_is_set_and_reset_alone(sent):
    db, statements = sent
    async with db.acquire() as conn:
        assert await conn.fetchval("SELECT current_schema()") == db.search_path
    assert (f'SET search_path TO "{db.search_path}"', True) in statements
    assert ("RESET search_path", True) in statements


async def test_a_read_alone_is_sent_in_autocommit_and_the_next_statement_is_not(sent):
    db, statements = sent
    async with db.acquire() as conn:
        rows = (await conn.read_alone(select(func.count()).select_from(graph_sessions))).all()
        assert rows == [(0,)]
        statements.clear()
        async with conn.transaction():
            assert await conn.fetchval("SELECT 1") == 1
            assert conn._sc.connection.driver_connection.info.transaction_status.name == "INTRANS"
    assert statements[0] == ("SELECT 1", False)


async def test_a_read_alone_inside_a_transaction_is_refused(sent):
    db, _statements = sent
    async with db.acquire() as conn:
        async with conn.transaction():
            with pytest.raises(AssertionError, match="outside a transaction"):
                await conn.read_alone(select(func.count()).select_from(graph_sessions))
