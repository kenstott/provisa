# Copyright (c) 2026 Kenneth Stott
# Canary: f4e39701-4d3b-4123-93b6-72ca63b28fd7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The runtime a model build publishes, as far as deriving an offer's contracts reads it
(provisa.api.offer_derivation), over a real control plane: the tests' stand-in for a build."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import select

from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import registered_tables


class Built:
    """The runtime a build publishes, as far as deriving a contract reads it: every registered
    table with its stored columns (``draft`` ones left out), the viewer role, and what the viewer
    reads of each column -- ``masked`` {column: mask kind}, ``unread`` columns."""

    def __init__(self) -> None:
        self.masked: dict[str, str] = {}
        self.unread: set[str] = set()
        self.draft: set[str] = set()

    async def state(self, conn) -> Any:
        rows = (
            await conn.execute_core(
                select(
                    registered_tables.c.id,
                    registered_tables.c.table_name,
                    registered_tables.c.domain_id,
                )
            )
        ).fetchall()
        tables = [
            {
                "id": int(r.id),
                "domain_id": r.domain_id,
                "columns": await table_repo.load_columns(conn, int(r.id)),
            }
            for r in rows
            if r.table_name not in self.draft
        ]
        return SimpleNamespace(
            roles={"viewer": {"capabilities": []}},
            role_chains={"viewer": ["viewer"]},
            meta_roles={},
            meta_qualifiers={},
            tables=tables,
        )

    def view(self, *_args: Any, **kw: Any) -> list[dict]:
        (table,) = kw["tables"]
        return [
            {
                "columns": [
                    {
                        "name": c["column_name"],
                        "read": c["column_name"] not in self.unread,
                        "mask": {"type": self.masked[c["column_name"]]}
                        if c["column_name"] in self.masked
                        else None,
                    }
                    for c in table["columns"]
                ]
            }
        ]


def installed(monkeypatch: pytest.MonkeyPatch) -> Built:
    """A :class:`Built` the effective-rights view of each column is read from."""
    b = Built()
    monkeypatch.setattr("provisa.security.effective_rights.data_view_from_state", b.view)
    return b
