# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1921: a configuration file carries a withdrawn table or source as withdrawn, so a promotion
carries a move between regions in progress. Applying a file sets what it states, withdrawn or not;
the store writes it back out as it holds it; and a file that says ``draft`` is refused by name,
saying what to write instead."""

# Requirements: REQ-1921

from __future__ import annotations

import contextlib
from typing import Any

import pytest
from pydantic import ValidationError


_COLUMNS = [{"name": "id", "data_type": "integer", "visible_to": ["seller"]}]


def _raw(*, withdrawn: bool) -> dict[str, Any]:
    flag = {"withdrawn": True} if withdrawn else {}
    return {
        "sources": [
            {"id": "crm", "type": "postgresql", "host": "h", "port": 5432, "database": "d"},
            {
                "id": "erp",
                "type": "postgresql",
                "host": "h",
                "port": 5432,
                "database": "d",
                **flag,
            },
        ],
        "domains": [{"id": "sales"}],
        "roles": [
            {
                "id": "seller",
                "description": "Test role.",
                "capabilities": [],
                "domain_access": ["sales"],
            }
        ],
        "tables": [
            {
                "source_id": "crm",
                "domain_id": "sales",
                "schema": "public",
                "table": "orders",
                "columns": _COLUMNS,
            },
            {
                "source_id": "crm",
                "domain_id": "sales",
                "schema": "public",
                "table": "refunds",
                "columns": _COLUMNS,
                **flag,
            },
        ],
    }


@contextlib.asynccontextmanager
async def _store(control_plane, monkeypatch):
    from provisa.core import secrets_providers, secrets_store

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)

    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)
    async with control_plane.org().acquire() as conn:
        yield conn


async def _withdrawn(conn) -> tuple[set[str], frozenset[str]]:
    from provisa.api.admin.db_queries import fetch_tables
    from provisa.core.repositories import source as source_repo

    tables = {t["table_name"] for t in await fetch_tables(conn, withdrawn=True)}
    return tables, await source_repo.withdrawn_ids(conn)


async def test_a_file_carries_a_withdrawn_table_and_source_into_the_store_and_back_out(
    control_plane, monkeypatch
):
    from provisa.core.config_loader import apply_config, parse_config_dict
    from provisa.core.store_config import store_model

    async with _store(control_plane, monkeypatch) as conn:
        await apply_config(parse_config_dict(_raw(withdrawn=True)), conn)
        tables, sources = await _withdrawn(conn)
        # refunds itself; orders is out through nothing of its own (its source is crm).
        assert (tables, sources) == ({"refunds"}, frozenset({"erp"}))
        model = await store_model(conn)
    written_tables = {t["table"]: t.get("withdrawn", False) for t in model["tables"]}
    written_sources = {s["id"]: s.get("withdrawn", False) for s in model["sources"]}
    assert written_tables == {"orders": False, "refunds": True}
    assert written_sources == {"crm": False, "erp": True}


async def test_a_file_applied_again_without_it_puts_them_back(control_plane, monkeypatch):
    """The file states the model: a promotion that carries the move's end puts them back."""
    from provisa.core.config_loader import apply_config, parse_config_dict

    async with _store(control_plane, monkeypatch) as conn:
        await apply_config(parse_config_dict(_raw(withdrawn=True)), conn)
        await apply_config(parse_config_dict(_raw(withdrawn=False)), conn)
        assert await _withdrawn(conn) == (set(), frozenset())


def test_a_file_declaring_draft_is_refused_naming_what_to_write():
    from provisa.core.config_loader import parse_config_dict

    raw = _raw(withdrawn=False)
    raw["tables"][1]["draft"] = True
    with pytest.raises(ValidationError) as refused:
        parse_config_dict(raw)
    message = str(refused.value)
    assert "table 'refunds' declares draft, which is not a setting" in message
    assert "withdrawn: true" in message
