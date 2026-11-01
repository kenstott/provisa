# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A seeded edge the code no longer declares does not outlive the seed that dropped it."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from provisa.api import startup_seed


@pytest.mark.asyncio
async def test_a_retired_seeded_edge_is_removed_and_a_modellers_is_not():
    conn = MagicMock()
    conn.execute_core = AsyncMock(return_value=MagicMock(scalar=lambda: None))
    conn.upsert = AsyncMock()
    await startup_seed._seed_meta_relationships(conn)
    statements = [c.args[0] for c in conn.execute_core.await_args_list]
    deletes = [s for s in statements if getattr(s, "is_delete", False)]
    assert len(deletes) == 1
    sql = str(deletes[0].compile(compile_kwargs={"literal_binds": True}))
    assert "'meta:%" in sql and "'ontology:%" in sql
    assert "NOT IN" in sql and "ontology:_term_refs:term" in sql
