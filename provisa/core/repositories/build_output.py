# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The tables a model build replaces whole: derived state every worker of an org writes."""

# Requirements: REQ-1914

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy import Table, text


@asynccontextmanager
async def replacing_build_output(conn: Any, table: Table) -> AsyncGenerator[None]:
    """One transaction for a build's clearing and writing of ``table``, holding it against every
    other build of this org (the table is the org's: search_path).

    Every worker of an org rebuilds on a model change (REQ-1914), so builds replace these rows at
    once. The transaction alone would not serialize them: a build's DELETE reads a snapshot from
    before the other's commit and clears none of its new rows. Waiting here, before the DELETE,
    each build clears what the last one committed. Reads of the table do not wait."""
    async with conn.transaction():
        await conn.execute_core(text(f"LOCK TABLE {table.name} IN SHARE ROW EXCLUSIVE MODE"))
        yield
