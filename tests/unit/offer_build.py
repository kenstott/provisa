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

from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import select

from provisa.compiler.names_table import NameRow, NamesTable
from provisa.core.repositories import data_product_offer as offers
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import registered_tables


def names_table(
    named: Mapping[int, tuple[str, str]], views: frozenset[int] = frozenset()
) -> NamesTable:
    """A build's names table naming each table id ``named`` holds, as (domain id, SQL name); an
    id in ``views`` is a view."""
    return NamesTable(
        rows=tuple(
            NameRow(
                object_kind="view" if tid in views else "table",
                object_id=str(tid),
                surface="sql",
                name=name,
                physical_name=name,
                label=name,
                domain_id=domain,
                table_id=tid,
            )
            for tid, (domain, name) in named.items()
        )
    )


def published(
    names: NamesTable | None = None,
    checked: Mapping[int, int] | None = None,
    views: Mapping[int, set[int]] | None = None,
) -> offers.Published:
    """The build a runtime has published, as a request acting on a product reads it: its names
    table, each checker's checked table, and the tables each view reads."""
    reads = dict(views or {})
    return offers.Published(
        names=names,
        checked=dict(checked or {}),
        view_reads=lambda ids: {v: set(reads[v]) for v in ids if v in reads},
    )


class Built:
    """The runtime a build publishes, as far as deriving a contract reads it: every registered
    table with its stored columns (``withdrawn`` ones left out), the viewer role, and what the viewer
    reads of each column -- ``masked`` {column: mask kind}, ``unread`` columns."""

    def __init__(self) -> None:
        self.masked: dict[str, str] = {}
        self.unread: set[str] = set()
        self.withdrawn: set[str] = set()
        # REQ-2125: each checker's checked table, as the build resolved and published it.
        self.checked: dict[int, int] = {}

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
            if r.table_name not in self.withdrawn
        ]
        return SimpleNamespace(
            roles={"viewer": {"capabilities": []}},
            role_chains={"viewer": ["viewer"]},
            meta_roles={},
            meta_qualifiers={},
            tables=tables,
            checked_targets=dict(self.checked),
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
