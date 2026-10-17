# Copyright (c) 2026 Kenneth Stott
# Canary: f7d09523-4490-4af7-85ea-e3cbb3fd1b84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The tables a server governs with carry each column's embedding declaration (REQ-421, REQ-423).

``fetch_tables`` names the column fields it reads one by one, and did not name ``embedding`` or
``embedding_model``: a column stored as an embedding reached governance as a plain column, and
every ``cosine_similarity`` over it was refused as "not an embedding column of this statement".
"""

# Requirements: REQ-421, REQ-423

from __future__ import annotations

from collections import defaultdict

import pytest

from provisa.api.admin.db_queries import fetch_tables
from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.stage2 import build_governance_context

pytestmark = pytest.mark.asyncio


class _Conn:
    """Answers fetch_tables' two statements from one stored table with an embedding column."""

    async def fetch(self, sql: str, *args):
        del args
        if "FROM table_write_grants" in sql:  # REQ-2074: no write grants
            return []
        if "FROM registered_tables" in sql:
            row: defaultdict[str, object] = defaultdict(lambda: None)
            row.update(id=1, source_id="pg", domain_id="d", schema_name="public", table_name="docs")
            return [row]
        return [
            defaultdict(lambda: None, column_name="id", data_type="integer", embedding=False),
            defaultdict(
                lambda: None,
                column_name="vec",
                data_type="array(double)",
                embedding=True,
                embedding_model="mini",
            ),
        ]


async def test_each_column_is_fetched_with_its_embedding_declaration():
    (table,) = await fetch_tables(_Conn())  # type: ignore[arg-type]
    assert [(c["column_name"], c["embedding"], c["embedding_model"]) for c in table["columns"]] == [
        ("id", False, None),
        ("vec", True, "mini"),
    ]


async def test_governance_knows_the_fetched_embedding_column():
    (table,) = await fetch_tables(_Conn())  # type: ignore[arg-type]
    gov = build_governance_context(
        "admin",
        None,
        {},
        CompilationContext(),
        [table],
        {"id": "admin", "capabilities": [], "domain_access": ["*"]},
    )
    assert gov.embedding_columns == {1: {"vec": "mini"}}
