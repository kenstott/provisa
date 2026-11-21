# Copyright (c) 2026 Kenneth Stott
# Canary: c609a489-d231-4665-8a55-4cc86a79bd36
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2253: term finding works only within the domains selected in the domain filter.

Two domains, ``d`` and ``other``. Started with ``d`` selected, a scan reads no document or table of
``other``, works in no domain of it, proposes no merge into a term of it, and the documents and
scans offered are ``d``'s. Started with no selection, it is what it was. The term finding API
fixture (test_term_finding.py) runs each handler over a real control-plane store.
"""

# Requirements: REQ-2253
# ruff: noqa: F811 -- the api fixture imported here is redefined by each test that takes it

from __future__ import annotations

import pytest

from provisa.api.admin import term_finding_router as router
from provisa.api.errors import ApiError
from provisa.core.repositories import glossary as glossary_repo
from provisa.reference_library import library, store
from provisa.term_finding import queue
from tests.unit.test_term_finding import (  # noqa: F401 -- api is the shared fixture
    EMBEDDER,
    POLICY,
    _by_name,
    _request,
    _upload,
    api,
)

pytestmark = pytest.mark.asyncio


async def _scanned_within(api, body: dict, selected: list[str] | None) -> dict:
    started = await router.start_scan(_request(body), domains=selected)
    await api.launched.pop()
    return await router.get_scan(started["scan"]["id"], _request())


async def _refused(body: dict, selected: list[str]) -> ApiError:
    with pytest.raises(ApiError) as caught:
        await router.start_scan(_request(body), domains=selected)
    return caught.value


async def _kept_document(api, domain: str) -> int:
    async with api.db.acquire() as conn:
        return await library.add(
            conn,
            library.file_content("policy.md", POLICY),
            source_kind=store.FILE,
            domains=[domain],
            status=store.KEPT,
            embedder=EMBEDDER,
            added_by="admin",
        )


async def test_a_scan_within_the_selection_proposes_no_merge_into_a_term_outside_it(api):
    """``client`` is ``other``'s term; the document names a customer also called a client."""
    async with api.db.acquire() as conn:
        await glossary_repo.create_abstract_term(conn, "client", domains={"other"})
    within = _by_name(await _scanned_within(api, _upload(POLICY, domains=["d"]), ["d"]))
    customer = within["customer"]
    assert (customer["proposal"], customer["match"], customer["matches"]) == ("new", None, [])


async def test_with_no_selection_a_scan_still_matches_the_whole_glossary(api):
    async with api.db.acquire() as conn:
        await glossary_repo.create_abstract_term(conn, "client", domains={"other"})
    customer = _by_name(await _scanned_within(api, _upload(POLICY, domains=["d"]), None))[
        "customer"
    ]
    assert (customer["proposal"], customer["match"]["name"]) == ("merge", "client")


async def test_a_scan_in_a_domain_outside_the_selection_is_refused_and_writes_nothing(api):
    refused = await _refused(_upload(POLICY, domains=["other"]), ["d"])
    assert (refused.status_code, refused.code) == (403, "term_finding.outside_selection")
    assert api.launched == []
    async with api.db.acquire() as conn:
        assert await store.documents(conn) == []
        assert await queue.scans(conn) == []


async def test_a_document_outside_the_selection_is_neither_offered_nor_scanned(api):
    theirs = await _kept_document(api, "other")
    within = await router.list_documents(_request(), domains=["d"])
    assert within["documents"] == []
    everything = await router.list_documents(_request())
    assert [d["id"] for d in everything["documents"]] == [theirs]

    refused = await _refused({"document_id": theirs, "domains": ["d"]}, ["d"])
    assert (refused.status_code, refused.code) == (403, "term_finding.outside_selection")
    assert api.launched == []


async def test_a_table_outside_the_selection_is_refused_before_it_is_read(api, monkeypatch):
    from provisa.api.admin import term_finding_table as table

    async def _read(*_a, **_k):
        raise AssertionError("a table outside the selection was read")

    monkeypatch.setattr(table, "rows", _read)
    monkeypatch.setattr(table, "read", _read)
    terms = {"domain": "other", "table": "t", "term_column": "a", "definition_column": "b"}
    text = {"domain": "other", "table": "t", "id_column": "a", "text_column": "b"}
    for source in (terms, text):
        refused = await _refused({"table": source, "domains": ["d"]}, ["d"])
        assert (refused.status_code, refused.code) == (403, "term_finding.outside_selection")


async def test_the_scans_offered_are_the_selections(api):
    ours = await _scanned_within(api, _upload(POLICY, domains=["d"]), ["d"])
    theirs = await _scanned_within(api, _upload(POLICY + b"\nMore.", domains=["other"]), ["other"])
    within = await router.list_scans(_request(), domains=["d"])
    assert [s["id"] for s in within["scans"]] == [ours["scan"]["id"]]
    everything = await router.list_scans(_request())
    assert {s["id"] for s in everything["scans"]} == {ours["scan"]["id"], theirs["scan"]["id"]}
