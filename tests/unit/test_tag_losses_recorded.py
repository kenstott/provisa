# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3f1a52-9e64-4b0c-a8d1-5c2e9f04b736
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2259 (amended 2026-11-21), REQ-2145: each confirmed tag loss is recorded -- its column,
tag, the term it left, who and when -- and Context Health's Tags lost card counts this period's.

Asserted here: a confirmed move writes one row per column and tag lost, confirmed by the change's
actor; a refused move and a carried move write none; the read counts only losses since the drift
period's cutoff, names those on columns the caller reads and counts the rest; and the read is
refused without glossary_read.
"""

# Requirements: REQ-2259, REQ-2145

from __future__ import annotations

import types
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from sqlalchemy import insert, select

from provisa.api.admin import context_views_router
from provisa.api.errors import ApiError
from provisa.core import model_change
from provisa.core.schema_org import glossary_tag_losses
from provisa.core.term_drift import period_cutoff
from tests.unit.test_pii_term_moves import _model, _move

from tests.unit.gate_identity import serve_every_column


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    serve_every_column(monkeypatch)


async def _losses(db) -> list[dict]:
    async with db.acquire() as conn:
        rows = (
            await conn.execute_core(select(glossary_tag_losses).order_by(glossary_tag_losses.c.id))
        ).fetchall()
    return [dict(r._mapping) for r in rows]


@pytest.mark.asyncio
async def test_a_confirmed_loss_writes_a_row_with_column_tag_term_who_and_when(
    control_plane, monkeypatch
):
    async with _model(control_plane, monkeypatch) as (db, ids):
        before = datetime.now(UTC)
        async with model_change.scope("move", actor=lambda: "alice"):
            await _move(ids, confirm_tag_loss=["pii"])
        rows = await _losses(db)
    assert len(rows) == 1
    row = rows[0]
    assert {k: row[k] for k in row if k not in ("id", "confirmed_at")} == {
        "table_id": ids["customers"],
        "table_ref": "__derived__.s.customers",
        "column_name": "home_address",
        "tag_id": "pii",
        "term_id": ids["address"],
        "term_name": "home address",
        "confirmed_by": "alice",
    }
    assert row["confirmed_at"] >= before - timedelta(seconds=5)


@pytest.mark.asyncio
async def test_a_refused_or_carried_move_records_no_loss(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as (db, ids):
        with pytest.raises(ApiError):
            await _move(ids)
        assert await _losses(db) == []
        await _move(ids, carry_tags=True)
        assert await _losses(db) == []


def _request(caller: str = "alice") -> Any:
    return types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id=caller, roles=[], acting_roles=[]),
            active_org_id="acme",
        )
    )


@pytest.mark.asyncio
async def test_the_count_covers_this_period_only(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as (db, ids):
        cutoff = period_cutoff(datetime.now(UTC))

        async def _pool():
            return db

        monkeypatch.setattr(context_views_router, "_pool", _pool)
        monkeypatch.setattr(
            "provisa.api.admin.context_views_router._require_glossary_read", lambda request: None
        )

        def loss(table: str, column: str, at: datetime) -> dict:
            return {
                "table_id": ids[table],
                "table_ref": f"__derived__.s.{table}",
                "column_name": column,
                "tag_id": "pii",
                "term_id": ids["address"],
                "term_name": "home_address",
                "confirmed_by": "bob",
                "confirmed_at": at,
            }

        async with db.acquire() as conn:
            await conn.execute_core(
                insert(glossary_tag_losses).values(
                    [
                        loss("customers", "home_address", cutoff - timedelta(seconds=1)),
                        loss("customers", "home_address", cutoff + timedelta(days=1)),
                        loss("accounts", "home_address", cutoff + timedelta(days=2)),
                    ]
                )
            )
        found = await context_views_router.tags_lost(cast(Any, _request()))
    assert found["cutoff"] == cutoff.isoformat()
    assert found["count"] == 2
    assert found["hidden"] == 0
    assert [(r["tableRef"], r["confirmedAt"]) for r in found["losses"]] == [
        ("__derived__.s.accounts", (cutoff + timedelta(days=2)).isoformat()),
        ("__derived__.s.customers", (cutoff + timedelta(days=1)).isoformat()),
    ]
    assert found["losses"][0] == {
        "tableId": ids["accounts"],
        "tableRef": "__derived__.s.accounts",
        "column": "home_address",
        "tag": "pii",
        "termId": ids["address"],
        "term": "home_address",
        "confirmedBy": "bob",
        "confirmedAt": (cutoff + timedelta(days=2)).isoformat(),
    }


def test_a_loss_on_an_unread_column_is_counted_not_named():
    at = datetime(2026, 11, 2, tzinfo=UTC)

    def row(table_id: int, column: str) -> dict:
        return {
            "table_id": table_id,
            "table_ref": f"s.public.t{table_id}",
            "column_name": column,
            "tag_id": "pii",
            "term_id": 7,
            "term_name": "Email",
            "confirmed_by": None,
            "confirmed_at": at,
        }

    named, hidden = context_views_router.losses_this_period(
        [row(1, "email"), row(2, "ssn")], lambda table_id, column: table_id == 1
    )
    assert [n["tableRef"] for n in named] == ["s.public.t1"]
    assert named[0]["confirmedBy"] is None
    assert hidden == 1


@pytest.mark.asyncio
async def test_the_read_is_refused_without_glossary_read(monkeypatch):
    from provisa.api.admin import capabilities

    monkeypatch.setattr(
        capabilities, "_resolved_capabilities", lambda _identity, _state: {"query_development"}
    )
    with pytest.raises(ApiError) as refused:
        await context_views_router.tags_lost(cast(Any, _request()))
    assert refused.value.status_code == 403
    assert refused.value.params == {"right": "glossary_read"}
