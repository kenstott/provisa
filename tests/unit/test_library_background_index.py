# Copyright (c) 2026 Kenneth Stott
# Canary: 7e41c0d2-3b58-4f16-a92c-6d0e8b1f4a37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2185 (A TOP-LEVEL LIBRARY; ADD NOW, INDEX IN THE BACKGROUND): adding a document is one step;
the document exists at once and is read, indexed and scanned for terms in the background, its index
status a property of it. Citations and assistants use a document once it is ready. Everyone browses
what they may read, and a document's Cited by lists what cites it, drifted citations flagged."""

# Requirements: REQ-2185

from __future__ import annotations

import base64
import json
import re
import types
from datetime import timedelta
from typing import Any, cast

import pytest
from fastapi import Request
from sqlalchemy import insert

from provisa.api.admin import reference_library_router as router
from provisa.api.admin import term_finding_router as finding
from provisa.api.errors import ApiError
from provisa.core.schema_org import (
    domains,
    glossary_term_drafts,
    glossary_term_scans,
    glossary_terms,
    reference_documents,
)
from provisa.reference_library import library, retrieve, store, terms
from provisa.term_finding import model as term_model

SCHEMA = "org_default"
POLICY = b"# Client policy\n\nA Client is a party that holds an account with the bank.\n"
VOCABULARY = ("client", "account", "bank", "party", "payment", "settle")


def _vector(text: str) -> list[float]:
    words = re.findall(r"[a-z]+", text.lower())
    return [float(sum(w.startswith(v) for w in words)) + 0.01 for v in VOCABULARY]


async def _embed(texts: list[str]) -> list[list[float]]:
    return [_vector(t) for t in texts]


EMBEDDER = library.Embedder(model="test-embed", embed=_embed)


def _request() -> Request:
    return cast(
        "Request",
        types.SimpleNamespace(
            state=types.SimpleNamespace(
                identity=types.SimpleNamespace(user_id="alice", roles=[], acting_roles=[]),
                active_org_id="default",
            )
        ),
    )


def _upload(name: str, data: bytes, **extra: Any) -> dict:
    return {"file_name": name, "content_b64": base64.b64encode(data).decode(), **extra}


@pytest.fixture
def db(control_plane):
    with control_plane.engine().begin() as c:
        c.exec_driver_sql(f'SET LOCAL search_path TO "{SCHEMA}"')
        c.execute(insert(domains).values(id="sales", description=""))
        c.execute(insert(domains).values(id="finance", description=""))
    return control_plane.org()


@pytest.fixture
def api(db, monkeypatch):
    """The API with its background work held: each job runs when the test says, not on a worker."""
    jobs: list[Any] = []
    monkeypatch.setattr(router, "_pool", lambda: db)
    monkeypatch.setattr(finding, "_pool", lambda: db)
    monkeypatch.setattr(router, "_guard", lambda request: "default")
    monkeypatch.setattr(router, "_read_scope", lambda request: None)
    monkeypatch.setattr(router, "_acting_role", lambda request: "steward")
    monkeypatch.setattr(router, "_launch", jobs.append)

    async def embedder():
        return EMBEDDER

    monkeypatch.setattr(router, "_embedder", embedder)

    async def no_terms(pool, document, role, started_by):
        return None

    monkeypatch.setattr(finding, "find_terms_in", no_terms)
    yield types.SimpleNamespace(db=db, jobs=jobs)
    for job in jobs:  # a job the test left unrun is not run
        job.close()


async def _run_jobs(api) -> None:
    while api.jobs:
        await api.jobs.pop(0)


async def test_adding_is_one_step_and_the_document_exists_at_once(api):
    added = await router.add_document(_upload("policy.md", POLICY, domains=["sales"]), _request())

    assert added["index_status"] == "indexing"
    assert added["passages"] == 0
    assert added["title"] == "policy.md"
    assert added["status"] == "kept"
    listed = await router.list_documents(_request())
    assert [d["id"] for d in listed["documents"]] == [added["id"]]
    assert len(api.jobs) == 1


async def test_the_background_job_reads_indexes_and_makes_the_document_ready(api):
    added = await router.add_document(_upload("policy.md", POLICY, domains=["sales"]), _request())
    await _run_jobs(api)

    ready = await router.get_document(added["id"], _request())
    assert ready["index_status"] == "ready"
    assert ready["index_error"] is None
    assert ready["title"] == "Client policy"
    assert ready["passages"] == 1
    assert ready["embedding_model"] == "test-embed"
    assert [p["section"] for p in ready["passage_list"]] == ["Client policy"]


async def test_a_title_given_at_add_is_kept_when_the_document_is_read(api):
    added = await router.add_document(
        _upload("policy.md", POLICY, domains=["sales"], title="Our client rules"), _request()
    )
    await _run_jobs(api)

    ready = await router.get_document(added["id"], _request())
    assert ready["title"] == "Our client rules"


async def test_the_same_file_is_refused_at_add_while_the_first_is_still_indexing(api):
    added = await router.add_document(_upload("policy.md", POLICY, domains=["sales"]), _request())

    with pytest.raises(ApiError) as again:
        await router.add_document(_upload("copy.md", POLICY, domains=["sales"]), _request())
    assert again.value.status_code == 409
    assert again.value.params == {"document_id": added["id"]}


async def test_an_unknown_kind_of_file_is_refused_at_add_and_leaves_nothing(api):
    with pytest.raises(ApiError, match="deck.pptx"):
        await router.add_document(_upload("deck.pptx", b"x", domains=["*"]), _request())
    assert (await router.list_documents(_request()))["documents"] == []
    assert api.jobs == []


async def test_a_url_document_is_fetched_by_the_job_not_the_request(api, monkeypatch):
    fetched: list[str] = []

    async def fetch(url: str, org_id: str | None) -> library.Content:
        fetched.append(url)
        return library.Content(POLICY, "text/markdown", None, url)

    monkeypatch.setattr(library, "fetch", fetch)
    added = await router.add_document(
        {"url": "https://example.com/rules/policy.md", "domains": ["*"]}, _request()
    )

    assert fetched == []
    assert (added["index_status"], added["title"]) == ("indexing", "policy.md")
    await _run_jobs(api)
    assert fetched == ["https://example.com/rules/policy.md"]
    ready = await router.get_document(added["id"], _request())
    assert (ready["index_status"], ready["title"]) == ("ready", "Client policy")


async def test_a_failed_document_says_why_and_a_retry_indexes_it(api, monkeypatch):
    added = await router.add_document(_upload("policy.md", POLICY, domains=["sales"]), _request())

    async def no_model():
        raise ApiError(409, "reference_library.no_vector_model", "no vector model is enabled")

    good = router._embedder
    monkeypatch.setattr(router, "_embedder", no_model)
    await _run_jobs(api)

    failed = await router.get_document(added["id"], _request())
    assert failed["index_status"] == "failed"
    assert failed["index_error"] == "no vector model is enabled"

    monkeypatch.setattr(router, "_embedder", good)
    retried = await router.retry_document(added["id"], _request())
    assert (retried["index_status"], retried["index_error"]) == ("indexing", None)
    await _run_jobs(api)
    ready = await router.get_document(added["id"], _request())
    assert (ready["index_status"], ready["passages"]) == ("ready", 1)


async def test_only_a_failed_document_can_be_retried(api):
    added = await router.add_document(_upload("policy.md", POLICY, domains=["sales"]), _request())
    await _run_jobs(api)

    with pytest.raises(ApiError) as refused:
        await router.retry_document(added["id"], _request())
    assert refused.value.status_code == 409
    assert refused.value.code == "reference_library.not_failed"


async def test_a_url_serving_a_document_already_held_fails_naming_it(api, monkeypatch):
    first = await router.add_document(_upload("policy.md", POLICY, domains=["sales"]), _request())
    await _run_jobs(api)

    async def fetch(url: str, org_id: str | None) -> library.Content:
        return library.Content(POLICY, "text/markdown", None, url)

    monkeypatch.setattr(library, "fetch", fetch)
    added = await router.add_document(
        {"url": "https://example.com/same.md", "domains": ["*"]}, _request()
    )
    await _run_jobs(api)

    failed = await router.get_document(added["id"], _request())
    assert failed["index_status"] == "failed"
    assert f"as document {first['id']}" in failed["index_error"]


async def test_progress_is_recorded_batch_by_batch(api, monkeypatch):
    monkeypatch.setattr(library, "EMBED_BATCH", 1)
    seen: list[tuple[str | None, int | None, int | None]] = []
    record = store.record_index_progress

    async def spying(conn, document_id, *, step, done=None, total=None):
        seen.append((step, done, total))
        return await record(conn, document_id, step=step, done=done, total=total)

    monkeypatch.setattr(store, "record_index_progress", spying)
    long = b"# Rules\n\nThe first rule.\n\n## More\n\nThe second rule.\n"
    await router.add_document(_upload("rules.md", long, domains=["*"]), _request())
    await _run_jobs(api)

    assert seen[0] == ("reading", None, None)
    assert seen[-1] == ("indexing", 2, 2)


async def test_removing_a_document_while_it_indexes_stops_the_job_quietly(api, monkeypatch):
    added = await router.add_document(_upload("policy.md", POLICY, domains=["sales"]), _request())
    await router.remove_document(added["id"], _request())

    await _run_jobs(api)

    assert (await router.list_documents(_request()))["documents"] == []


async def test_a_job_left_by_a_restart_fails_saying_so_and_a_file_can_be_retried(api):
    added = await router.add_document(_upload("policy.md", POLICY, domains=["sales"]), _request())
    api.jobs.clear()  # the server stopped before the job ran

    async with api.db.acquire() as conn:
        assert await store.fail_indexing_left_behind(conn, "a-later-launch", "restarted") == 1

    failed = await router.get_document(added["id"], _request())
    assert (failed["index_status"], failed["index_error"]) == ("failed", "restarted")

    await router.retry_document(added["id"], _request())
    await _run_jobs(api)
    assert (await router.get_document(added["id"], _request()))["index_status"] == "ready"


async def test_a_stalled_job_fails_saying_so(api):
    added = await router.add_document(_upload("policy.md", POLICY, domains=["sales"]), _request())
    async with api.db.acquire() as conn:
        assert await store.fail_indexing_stalled(conn, timedelta(hours=1), "stalled") == 0
        assert await store.fail_indexing_stalled(conn, timedelta(seconds=-1), "stalled") == 1
    failed = await router.get_document(added["id"], _request())
    assert (failed["index_status"], failed["index_error"]) == ("failed", "stalled")


async def test_the_scan_watch_also_gives_up_on_left_behind_indexing(api):
    added = await router.add_document(_upload("policy.md", POLICY, domains=["sales"]), _request())

    await finding.give_up_on_left_behind(api.db, "a-later-launch")

    failed = await router.get_document(added["id"], _request())
    assert failed["index_status"] == "failed"
    assert "restarted" in failed["index_error"]


async def test_a_document_in_flight_is_not_drawn_on_nor_named_as_stale(api):
    await router.add_document(_upload("policy.md", POLICY, domains=["*"]), _request())

    async with api.db.acquire() as conn:
        assert await retrieve.retrieve(conn, "What is a client?", None, EMBEDDER) == []
        assert await library.needing_re_embedding(conn, EMBEDDER) == []


async def test_a_document_is_drawn_on_once_ready(api):
    added = await router.add_document(_upload("policy.md", POLICY, domains=["*"]), _request())
    await _run_jobs(api)

    async with api.db.acquire() as conn:
        found = await retrieve.retrieve(conn, "What is a client?", None, EMBEDDER)
    assert [c.document_id for c in found] == [added["id"]]


async def test_a_document_still_indexing_cannot_be_refreshed_replaced_or_re_embedded(api):
    added = await router.add_document(_upload("policy.md", POLICY, domains=["*"]), _request())

    for call in (
        router.refresh_document(added["id"], _request()),
        router.upload_version(added["id"], _upload("v2.md", b"# Two\n\nText."), _request()),
        router.reembed_document(added["id"], _request()),
    ):
        with pytest.raises(ApiError) as refused:
            await call
        assert refused.value.status_code == 409
        assert refused.value.code == "reference_library.not_ready"
        assert "still being indexed" in str(refused.value.detail)


async def test_citing_a_document_still_indexing_says_so(api):
    added = await router.add_document(_upload("policy.md", POLICY, domains=["*"]), _request())

    async with api.db.acquire() as conn:
        document = await store.document(conn, added["id"])
    assert document is not None
    with pytest.raises(library.NotReady, match="still being indexed"):
        library.require_ready(document)


async def test_after_indexing_terms_are_found_in_the_background(api, monkeypatch):
    found: list[tuple[int, str, str | None]] = []

    async def find_terms_in(pool, document, role, started_by):
        found.append((document.id, role, started_by))

    monkeypatch.setattr(finding, "find_terms_in", find_terms_in)
    added = await router.add_document(_upload("policy.md", POLICY, domains=["*"]), _request())
    await _run_jobs(api)

    assert found == [(added["id"], "steward", "alice")]


async def test_a_failed_document_finds_no_terms(api, monkeypatch):
    found: list[int] = []

    async def find_terms_in(pool, document, role, started_by):
        found.append(document.id)

    monkeypatch.setattr(finding, "find_terms_in", find_terms_in)

    async def no_model():
        raise ApiError(409, "reference_library.no_vector_model", "no vector model")

    monkeypatch.setattr(router, "_embedder", no_model)
    await router.add_document(_upload("policy.md", POLICY, domains=["*"]), _request())
    await _run_jobs(api)

    assert found == []


async def test_no_term_model_leaves_the_document_ready_and_says_why_no_terms(db, monkeypatch):
    async with db.acquire() as conn:
        document_id = await store.insert_pending(
            conn,
            title="policy.md",
            source_kind=store.FILE,
            source="policy.md",
            media_type="text/markdown",
            content_sha256="abc",
            pending=POLICY,
            status=store.KEPT,
            added_by="alice",
            domains=["*"],
            launch="this-launch",
        )
        document = await store.document(conn, document_id)
    assert document is not None

    async def refused(state, role):
        raise term_model.ModelRefused("no model is named: set glossary_term_finding")

    monkeypatch.setattr(term_model, "for_role", refused)

    await finding.find_terms_in(db, document, "steward", "alice")

    async with db.acquire() as conn:
        held = await store.document(conn, document_id)
    assert held is not None
    assert held.terms_note == "no model is named: set glossary_term_finding"


async def test_everyone_browses_only_the_documents_they_may_read(api, monkeypatch):
    sales = await router.add_document(
        _upload("s.md", b"# S\n\nSales.", domains=["sales"]), _request()
    )
    finance = await router.add_document(
        _upload("f.md", b"# F\n\nFinance.", domains=["finance"]), _request()
    )
    whole = await router.add_document(_upload("w.md", b"# W\n\nAll.", domains=["*"]), _request())
    monkeypatch.setattr(router, "_read_scope", lambda request: frozenset({"sales"}))

    listed = await router.list_documents(_request())
    assert {d["id"] for d in listed["documents"]} == {sales["id"], whole["id"]}

    with pytest.raises(ApiError) as hidden:
        await router.get_document(finance["id"], _request())
    assert hidden.value.status_code == 404
    with pytest.raises(ApiError) as uncited:
        await router.cited_by(finance["id"], _request())
    assert uncited.value.status_code == 404


@pytest.mark.parametrize(
    ("held", "admitted"),
    [({"glossary_read"}, True), ({"glossary_rw"}, False), (set(), False)],
)
def test_browsing_asks_the_glossarys_read_right(monkeypatch, held, admitted):
    """Reading the library is reading the vocabulary's sources: glossary_read, every seeded role's.
    Adding, refreshing and removing stay glossary_rw's."""
    import provisa.api.admin.capabilities as capmod

    monkeypatch.setattr(capmod, "_resolved_capabilities", lambda _identity, _state: held)
    if admitted:
        router._read_scope(_request())
        return
    with pytest.raises(ApiError) as refused:
        router._read_scope(_request())
    assert refused.value.status_code == 403


async def _term(conn, name: str) -> int:
    result = await conn.execute_core(
        insert(glossary_terms).values(name=name).returning(glossary_terms.c.id)
    )
    return int(result.fetchone()[0])


async def _cite(conn, document_id: int, term_id: int, ordinal: int, quote: str) -> None:
    result = await conn.execute_core(
        insert(glossary_term_scans)
        .values(
            document_id=document_id,
            document_title="d",
            document_source="d",
            domains="[]",
            status="done",
        )
        .returning(glossary_term_scans.c.id)
    )
    await conn.execute_core(
        insert(glossary_term_drafts).values(
            scan_id=int(result.fetchone()[0]),
            ordinal=0,
            name="x",
            category="concept",
            sense="",
            definition="d",
            definition_kind="explicit",
            proposal="new",
            payload=json.dumps(
                {
                    "passages": [
                        {"ordinal": ordinal, "section": "Rules", "page": None, "quote": quote}
                    ]
                }
            ),
            status="accepted",
            term_id=term_id,
        )
    )


async def test_cited_by_lists_the_terms_citing_and_flags_a_drifted_citation(api):
    body = b"# Rules\n\nA Client holds an account.\n\n## Later\n\nA payment settles daily.\n"
    added = await router.add_document(_upload("rules.md", body, domains=["*"]), _request())
    await _run_jobs(api)
    async with api.db.acquire() as conn:
        client = await _term(conn, "Client")
        payment = await _term(conn, "Payment")
        gone = await _term(conn, "Dormant")
        await _cite(conn, added["id"], client, 0, "A Client  holds an account.")  # as it was
        await _cite(conn, added["id"], payment, 0, "A payment settles daily.")  # moved, follows
        await _cite(conn, added["id"], gone, 0, "A dormant account is closed.")  # text gone

    cited = await router.cited_by(added["id"], _request())

    by_term = {c["term"]: c for c in cited["terms"]}
    assert set(by_term) == {"Client", "Payment", "Dormant"}
    assert by_term["Client"]["state"] == "current"
    assert by_term["Payment"]["state"] == "current"
    assert by_term["Payment"]["ordinal"] == 1
    assert by_term["Payment"]["section"] == "Later"
    assert by_term["Dormant"]["state"] == "drifted"
    assert by_term["Dormant"]["quote"] == "A dormant account is closed."
    assert cited["drifted"] == 1


async def test_cited_by_lists_only_terms_the_caller_may_read(api, monkeypatch):
    added = await router.add_document(_upload("p.md", POLICY, domains=["*"]), _request())
    await _run_jobs(api)
    async with api.db.acquire() as conn:
        term = await _term(conn, "Client")
        await _cite(conn, added["id"], term, 0, "A Client is a party")
    monkeypatch.setattr(router, "_readable_terms", _nothing)

    assert (await router.cited_by(added["id"], _request()))["terms"] == []


async def _nothing(conn, scope, term_ids):
    return set()


async def test_the_passage_quote_is_compared_ignoring_spacing(api):
    assert terms.same_text("A  Client\nholds", "A Client holds an account") is True
    assert terms.same_text("A dormant", "A Client holds an account") is False


async def test_finished_rows_keep_no_uploaded_bytes(api):
    added = await router.add_document(_upload("policy.md", POLICY, domains=["*"]), _request())
    async with api.db.acquire() as conn:
        assert await store.pending_content(conn, added["id"]) == POLICY
    await _run_jobs(api)
    async with api.db.acquire() as conn:
        assert await store.pending_content(conn, added["id"]) is None


async def test_a_document_written_before_the_column_counts_as_ready(api):
    """Documents already in a library before the index status existed are ready: their passages
    are there. The column's default says so."""
    async with api.db.acquire() as conn:
        result = await conn.execute_core(
            insert(reference_documents)
            .values(
                title="old",
                source_kind="file",
                source="old.txt",
                media_type="text/plain",
                content_sha256="old",
                status="kept",
                embedding_model="test-embed",
            )
            .returning(reference_documents.c.id)
        )
        document_id = int(result.fetchone()[0])
        held = await store.document(conn, document_id)
    assert held is not None
    assert held.index_status == store.READY
