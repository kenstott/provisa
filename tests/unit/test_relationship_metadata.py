# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-020: relationship ownership, versioning, and re-review on join-field change."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.sql.dml import Update

from provisa.core.models import Cardinality, Relationship
from provisa.core.repositories.relationship import mark_relationships_for_review

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


class _Conn:
    def __init__(self, rows):
        self._rows = rows
        self.executed: list = []  # recorded UPDATE statements

    async def execute_core(self, stmt):
        if isinstance(stmt, Update):
            self.executed.append(stmt)
        result = MagicMock()
        result.fetchall.return_value = [MagicMock(_mapping=r) for r in self._rows]
        return result


def _rel(rid, src_tid, tgt_tid, src_col, tgt_col):
    return {
        "id": rid,
        "source_table_id": src_tid,
        "target_table_id": tgt_tid,
        "source_column": src_col,
        "target_column": tgt_col,
    }


async def test_model_defaults():
    rel = Relationship(
        id="r",
        source_table_id="a",
        target_table_id="b",
        source_column="x",
        target_column="y",
        cardinality=Cardinality.many_to_one,
    )
    assert rel.owner is None
    assert rel.version == 1
    assert rel.needs_review is False


async def test_model_accepts_owner():
    rel = Relationship(
        id="r",
        source_table_id="a",
        target_table_id="b",
        source_column="x",
        target_column="y",
        cardinality=Cardinality.many_to_one,
        owner="alice",
    )
    assert rel.owner == "alice"


class TestMarkForReview:
    async def test_flags_relationship_with_missing_source_column(self):
        # Table 1's column "customer_id" is gone → its relationship is flagged.
        conn = _Conn([_rel("r1", 1, 2, "customer_id", "id")])
        flagged = await mark_relationships_for_review(conn, 1, ["id", "amount"])
        assert flagged == ["r1"]
        sql = str(conn.executed[0].compile(compile_kwargs={"literal_binds": True}))
        assert "needs_review" in sql

    async def test_flags_relationship_with_missing_target_column(self):
        conn = _Conn([_rel("r1", 2, 1, "fk", "old_pk")])  # table 1 is the target
        flagged = await mark_relationships_for_review(conn, 1, ["new_pk"])
        assert flagged == ["r1"]

    async def test_no_flag_when_columns_present(self):
        conn = _Conn([_rel("r1", 1, 2, "customer_id", "id")])
        flagged = await mark_relationships_for_review(conn, 1, ["customer_id", "amount"])
        assert flagged == []
        assert conn.executed == []  # no UPDATE issued

    async def test_only_this_table_side_checked(self):
        # Relationship where table 1 is the SOURCE; its target_column belongs to table 2
        # and must not be checked against table 1's columns.
        conn = _Conn([_rel("r1", 1, 2, "customer_id", "some_target_col")])
        flagged = await mark_relationships_for_review(conn, 1, ["customer_id"])
        assert flagged == []
