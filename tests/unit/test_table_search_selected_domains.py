# Copyright (c) 2026 Kenneth Stott
# Canary: 6b567a5b-1bf5-4929-b821-a44c53d6cdc6
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2253: table search works only within the domains selected in the domain filter.

Source ``wh`` holds ``customers`` (registered in sales), ``staff`` (registered in hr) and
``visits`` (not registered). With sales selected the model is sent and ranks only ``customers``
and ``visits``; a source whose tables may be registered only in hr is refused before it is read.
With no selection the search is what it was.
"""

# Requirements: REQ-2253

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

from provisa.api import app as app_module
from provisa.api.admin import glossary_router
from provisa.api.admin import table_search_router as router
from provisa.api.errors import ApiError
from provisa.discovery import table_search
from provisa.discovery.table_search import RankedTable, TableCandidate

if TYPE_CHECKING:
    from fastapi import Request

pytestmark = pytest.mark.asyncio

TABLES = ("customers", "staff", "visits")


@pytest.fixture
def sent(monkeypatch) -> list[list[str]]:
    """The tables each model call is sent."""
    state = SimpleNamespace(
        source_types={"wh": "postgresql", "hr_only": "postgresql"},
        source_allowed_domains={"hr_only": ["hr"]},
        tables=[
            {
                "source_id": "wh",
                "schema_name": "public",
                "table_name": "customers",
                "domain_id": "sales",
            },
            {"source_id": "wh", "schema_name": "public", "table_name": "staff", "domain_id": "hr"},
        ],
        model_db=object(),
    )
    monkeypatch.setattr(app_module, "state", state, raising=False)
    monkeypatch.setattr(glossary_router, "_authority", lambda request: None)
    monkeypatch.setattr(router, "require_capability_request", lambda request, cap: None)
    calls: list[list[str]] = []

    async def _live(source_id, schema_name, _state):
        if source_id != "wh":
            raise AssertionError(f"source {source_id!r} outside the selection was read")
        return [
            TableCandidate(name=t, comment=f"{t} records", columns=["id"], schema_name="public")
            for t in TABLES
        ]

    async def _cold(*_a):
        return None

    async def _no_keys(_db):
        return {}

    async def _rank(query, candidates, api_keys=None):
        calls.append([c.name for c in candidates])
        return [
            RankedTable(
                name=c.name,
                schema_name=c.schema_name,
                comment=c.comment,
                confidence=0.9,
                reasoning="fits",
            )
            for c in candidates
        ]

    monkeypatch.setattr(router, "_candidates_from_cache", _cold)
    monkeypatch.setattr(router, "_candidates_live", _live)
    monkeypatch.setattr("provisa.core.org_secrets.read_org_api_keys", _no_keys)
    monkeypatch.setattr(table_search, "llm_rank", _rank)
    return calls


def _request() -> "Request":
    return cast("Request", SimpleNamespace(state=SimpleNamespace(identity=None)))


async def _found(source: str, selected: list[str] | None) -> list[str]:
    answer = await router.search_source_tables(
        _request(), source, q="records", schema_name="public", domains=selected
    )
    return [c["table_name"] for c in answer["candidates"]]


async def test_a_table_registered_only_outside_the_selection_is_not_sent_or_found(sent):
    assert sorted(await _found("wh", ["sales"])) == ["customers", "visits"]
    assert [sorted(c) for c in sent] == [["customers", "visits"]]


async def test_with_no_selection_every_table_is_searched(sent):
    assert sorted(await _found("wh", None)) == sorted(TABLES)
    assert [sorted(c) for c in sent] == [sorted(TABLES)]


async def test_a_source_outside_the_selection_is_refused_before_it_is_read(sent):
    with pytest.raises(ApiError) as caught:
        await _found("hr_only", ["sales"])
    assert (caught.value.status_code, caught.value.code) == (403, "table_search.outside_selection")
    assert sent == []
