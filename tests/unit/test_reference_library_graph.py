# Copyright (c) 2026 Kenneth Stott
# Canary: 40a81e22-7fb2-4acd-a7c1-2294eafa4b7a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2185 (GRAPH RETRIEVAL): governed retrieval that follows the glossary -- the walk's weights,
decay and cutoff, the passages each reached term offers, their place in the passage budget, what a
role may read, the terms' embeddings kept current, the Admin setting, and the bank secrecy demo's
question answered with the aggregation rule."""

# Requirements: REQ-2185

from __future__ import annotations

import json
import math
import re
import types
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest
import sqlalchemy as sa
import yaml
from sqlalchemy import insert, select, update

from provisa.api.admin import reference_library_router as router
from provisa.api.errors import ApiError
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.schema_org import (
    domains,
    glossary_term_drafts,
    glossary_term_scans,
    glossary_term_vectors,
    glossary_terms,
)
from provisa.reference_library import graph, library, retrieve, store, terms

if TYPE_CHECKING:
    from fastapi import Request

SCHEMA = "org_default"
ROOT = Path(__file__).resolve().parents[2]

# -- the walk -----------------------------------------------------------------------------------


def _priorities(reached: list[graph.Reached]) -> dict[int, float]:
    return {r.term_id: r.priority for r in reached}


def _close(a: float, b: float) -> bool:
    return math.isclose(a, b, rel_tol=1e-14)


def test_each_relationship_type_weighs_as_the_requirement_says():
    assert dict(graph.WEIGHTS) == {
        "SYNONYM_OF": 1.0,
        "PREFERRED_TERM_FOR": 1.0,
        "KIND_OF": 0.8,
        "PART_OF": 0.8,
        "VALID_VALUE_OF": 0.8,
        "DERIVED_FROM": 0.6,
        "REPLACES": 0.6,
        "RELATED_TO": 0.4,
    }
    assert graph.CUTOFF == 0.25
    for rel_type, weight in graph.WEIGHTS.items():
        reached = _priorities(graph.walk([1], [(1, 2, rel_type)], [1, 2]))
        assert _close(reached[1], 1.0) and _close(reached[2], weight)


@pytest.mark.parametrize("rel_type", ["TRANSLATION_OF", "ANTONYM_OF"])
def test_a_translation_or_an_antonym_conducts_nothing(rel_type):
    reached = graph.walk([1], [(1, 2, rel_type), (2, 3, "SYNONYM_OF")], [1, 2, 3])
    assert [r.term_id for r in reached] == [1]


def test_priority_is_the_product_along_the_path_and_falls_with_each_hop():
    edges = [
        (1, 2, "SYNONYM_OF"),
        (3, 2, "PART_OF"),  # walked against its direction: the walk goes either way
        (3, 4, "KIND_OF"),
        (4, 5, "RELATED_TO"),
    ]
    reached = graph.walk([1], edges, [1, 2, 3, 4, 5])
    priorities = _priorities(reached)
    assert _close(priorities[2], 1.0)
    assert _close(priorities[3], 0.8)
    assert _close(priorities[4], 0.64)
    assert _close(priorities[5], 0.256)  # 0.8 * 0.8 * 0.4, just above the cutoff
    assert [r.term_id for r in reached] == [1, 2, 3, 4, 5]
    assert [s.term_id for s in reached[-1].steps] == [2, 3, 4, 5]


def test_the_walk_stops_below_the_cutoff():
    # 0.6 * 0.4 = 0.24: below 0.25, so 3 is not reached, nor anything beyond it
    edges = [(1, 2, "DERIVED_FROM"), (2, 3, "RELATED_TO"), (3, 4, "SYNONYM_OF")]
    assert [r.term_id for r in graph.walk([1], edges, [1, 2, 3, 4])] == [1, 2]
    # two RELATED_TO hops: 0.16
    assert [
        r.term_id for r in graph.walk([1], [(1, 2, "RELATED_TO"), (2, 3, "RELATED_TO")], [1, 2, 3])
    ] == [1, 2]


def test_a_term_is_reached_by_its_best_path():
    # 4 by RELATED_TO from the seed is 0.4; by two KIND_OF hops it is 0.64
    edges = [(1, 4, "RELATED_TO"), (1, 2, "KIND_OF"), (2, 4, "KIND_OF")]
    (four,) = [r for r in graph.walk([1], edges, [1, 2, 4]) if r.term_id == 4]
    assert _close(four.priority, 0.64)
    assert [(s.term_id, s.rel_type) for s in four.steps] == [(2, "KIND_OF"), (4, "KIND_OF")]


def test_every_seed_starts_at_one_and_a_term_is_reached_once():
    edges = [(1, 3, "RELATED_TO"), (2, 3, "PART_OF")]
    reached = graph.walk([1, 2, 1], edges, [1, 2, 3])
    assert [r.term_id for r in reached] == [1, 2, 3]
    assert reached[2].seed == 2 and _close(reached[2].priority, 0.8)


def test_the_walk_keeps_to_the_terms_it_is_given():
    # 2 is not readable: neither reached nor walked through
    edges = [(1, 2, "SYNONYM_OF"), (2, 3, "SYNONYM_OF")]
    assert [r.term_id for r in graph.walk([1, 2], edges, [1, 3])] == [1]


def test_a_path_is_labelled_from_its_seed_over_each_relationship():
    edges = [(2, 1, "PART_OF"), (2, 3, "RELATED_TO")]
    reached = {r.term_id: r for r in graph.walk([1], edges, [1, 2, 3])}
    names = {1: "currency transaction report", 2: "multiple transactions", 3: "cash"}
    assert graph.label(reached[1], names) == "via currency transaction report"
    assert (
        graph.label(reached[2], names)
        == "via currency transaction report -> multiple transactions (part of)"
    )
    assert graph.label(reached[3], names) == (
        "via currency transaction report -> multiple transactions (part of) -> cash (related to)"
    )


def test_the_terms_a_question_names_are_found_as_whole_phrases():
    held = {
        1: terms.Term(1, "currency transaction report", None, "approved"),
        2: terms.Term(2, "cash transaction", None, "approved"),
        3: terms.Term(3, "deposit", None, "approved"),
        4: terms.Term(4, "port", None, "approved"),
    }
    question = "Which cash deposits need a Currency  Transaction Report?"
    assert retrieve.named_terms(question, held) == [3, 1]


# -- the library and the glossary in the control plane ------------------------------------------

VOCABULARY = (
    "cash",
    "deposit",
    "currency",
    "report",
    "transaction",
    "multiple",
    "branch",
    "business",
    "person",
    "financial",
    "client",
    "account",
    "dormant",
    "book",
)


def _vector(text: str) -> list[float]:
    words = re.findall(r"[a-z]+", text.lower())
    return [float(sum(w.startswith(v) for w in words)) + 0.01 for v in VOCABULARY]


calls: list[list[str]] = []


async def _embed(texts: list[str]) -> list[list[float]]:
    calls.append(list(texts))
    return [_vector(t) for t in texts]


EMBEDDER = library.Embedder(model="test-embed", embed=_embed)


@pytest.fixture
def db(control_plane):
    with control_plane.engine().begin() as c:
        c.exec_driver_sql(f'SET LOCAL search_path TO "{SCHEMA}"')
        for domain in ("sales", "finance", "banksecrecy"):
            c.execute(insert(domains).values(id=domain, description=""))
    calls.clear()
    return control_plane.org()


async def _add(conn, name: str, data: bytes, scope: list[str]) -> int:
    return await library.add(
        conn,
        library.file_content(name, data),
        source_kind=store.FILE,
        domains=scope,
        status=store.KEPT,
        embedder=EMBEDDER,
        added_by="alice",
    )


async def _term(
    conn, name: str, definition: str | None, scope: set[str], *, approved: bool = True
) -> int:
    result = await conn.execute_core(
        insert(glossary_terms)
        .values(name=name, definition=definition, approved=approved)
        .returning(glossary_terms.c.id)
    )
    term_id = int(result.fetchone()[0])
    await glossary_repo.set_declared_domains(conn, term_id, scope)
    return term_id


async def _cite(
    conn, term_id: int, document_id: int, ordinals: list[int], status: str = "merged"
) -> None:
    """``term_id`` cites passages ``ordinals`` of ``document_id``: a scan of the document found
    the term there, and the steward merged the draft into it (or ``status``)."""
    scan = await conn.execute_core(
        insert(glossary_term_scans)
        .values(
            document_id=document_id,
            document_title="scanned",
            document_source="scanned",
            domains="[]",
            status="done",
        )
        .returning(glossary_term_scans.c.id)
    )
    await conn.execute_core(
        insert(glossary_term_drafts).values(
            scan_id=int(scan.fetchone()[0]),
            ordinal=0,
            name="draft",
            category="concept",
            sense="",
            definition_kind="missing",
            proposal="merge",
            payload=json.dumps(
                {
                    "passages": [
                        {"ordinal": o, "section": None, "page": None, "quote": ""} for o in ordinals
                    ]
                }
            ),
            status=status,
            term_id=term_id if status in ("accepted", "merged") else None,
            match_term_id=term_id,
        )
    )


@pytest.fixture
def named_only(monkeypatch):
    """The walk seeded by the terms the question names and the one term nearest it: in a glossary
    of three terms, the three nearest are all of them."""
    monkeypatch.setattr(retrieve, "NEAREST_TERMS", 1)


def _keys(found: list[retrieve.Citation]) -> list[tuple[int, int]]:
    return [(c.document_id, c.ordinal) for c in found]


# A client policy and a payments standard: the glossary relates "client" to "account" (PART_OF)
# and "account" to "payment" (RELATED_TO).
POLICY = b"""# Client policy

A client is a party the bank serves.

## Onboarding

Each client is identified before business begins.

## Statements

Statements are sent every month.
"""

ACCOUNTS = b"""# Account rules

An account is opened for one person.

## Closing

Closing needs a signature.

## Dormancy

After two years without activity a holding is dormant.

## Fees

Fees are charged quarterly.
"""


async def _library(conn) -> dict[str, int]:
    """Two documents, the terms client > account > ledger, and their citations."""
    policy = await _add(conn, "policy.md", POLICY, ["*"])
    rules = await _add(conn, "accounts.md", ACCOUNTS, ["*"])
    client = await _term(conn, "client", "A party the bank serves.", {"sales"})
    account = await _term(conn, "account", "Where a client's money is held.", {"sales"})
    ledger = await _term(conn, "ledger", "The books.", {"sales"})
    await glossary_repo.add_edge(conn, account, client, "PART_OF")
    await glossary_repo.add_edge(conn, ledger, account, "RELATED_TO")
    # account cites all four passages of the account rules; ledger the policy's statements
    await _cite(conn, account, rules, [0, 1, 2, 3])
    await _cite(conn, ledger, policy, [2])
    return {
        "policy": policy,
        "rules": rules,
        "client": client,
        "account": account,
        "ledger": ledger,
    }


async def test_a_reached_term_offers_at_most_two_of_its_passages_nearest_the_question_first(db):
    async with db.acquire() as conn:
        ids = await _library(conn)
        question = "What does the client policy say about a person's account closing?"
        found = await retrieve.retrieve_with_neighbours(
            conn, question, None, EMBEDDER, k=1, budget=6, retrieval=retrieve.GRAPH
        )
        via_account = [c for c in found if c.term == "account"]
        assert len(via_account) == retrieve.PASSAGES_PER_TERM == 2
        # of the four passages "account" cites, the two nearest the question not already given
        given = set(_keys(found[:1]))
        cited = await store.scored_passages(
            conn, _vector(question), [(ids["rules"], o) for o in (0, 1, 2, 3)]
        )
        nearest = [
            (p.document_id, p.ordinal) for p in cited if (p.document_id, p.ordinal) not in given
        ]
        assert _keys(via_account) == nearest[:2]
        first, second = (c.score for c in via_account)
        assert first is not None and second is not None and first >= second
        assert via_account[0].term_definition == "Where a client's money is held."


async def test_graph_candidates_come_after_the_question_s_passages_and_before_neighbours(
    db, named_only
):
    async with db.acquire() as conn:
        ids = await _library(conn)
        question = "Who is a client?"
        hits = await retrieve.retrieve(conn, question, None, EMBEDDER, k=1)
        assert _keys(hits) == [(ids["policy"], 0)]

        found = await retrieve.retrieve_with_neighbours(
            conn, question, None, EMBEDDER, k=1, budget=8, retrieval=retrieve.GRAPH
        )
        assert found[0] == hits[0]
        kinds = [
            "hit"
            if c.reached == retrieve.NEAREST_QUESTION
            else "graph"
            if c.reached.startswith("via ")
            else "neighbour"
            for c in found
        ]
        # the question's passage, then the walk's in priority order, then the neighbours
        assert kinds == sorted(kinds, key=["hit", "graph", "neighbour"].index)
        graph_found = [c for c in found if c.reached.startswith("via ")]
        assert [c.term for c in graph_found] == ["account", "account", "ledger"]
        # 1.0 * 0.8 = 0.8 for account; 0.8 * 0.4 = 0.32 for ledger
        assert graph_found[2].reached == "via client -> account (part of) -> ledger (related to)"
        # every one of the library's seven passages, each once, within the budget of eight
        assert len(set(_keys(found))) == len(found) == 7
        # the neighbours are of the question's passage first, then of the walk's
        neighbours = [c for c in found if kinds[found.index(c)] == "neighbour"]
        assert neighbours[0].reached.endswith("passage 0 in 'Client policy'")

        # the same budget cuts: the walk never displaces the question's passages
        short = await retrieve.retrieve_with_neighbours(
            conn, question, None, EMBEDDER, k=1, budget=2, retrieval=retrieve.GRAPH
        )
        assert _keys(short) == _keys(found)[:2]
        same = await retrieve.retrieve_with_neighbours(
            conn, question, None, EMBEDDER, k=1, budget=1, retrieval=retrieve.GRAPH
        )
        assert same == hits


async def test_passage_retrieval_does_not_walk_the_glossary(db):
    async with db.acquire() as conn:
        await _library(conn)
        found = await retrieve.retrieve_with_neighbours(
            conn, "Who is a client?", None, EMBEDDER, k=1, budget=8
        )
        assert not any(c.reached.startswith("via ") for c in found)
        assert all(c.term is None and c.term_definition is None for c in found)
        with pytest.raises(ValueError, match="retrieval must be one of"):
            await retrieve.retrieve_with_neighbours(
                conn, "Who is a client?", None, EMBEDDER, retrieval="both"
            )


async def test_the_terms_citing_the_question_s_passages_seed_the_walk(db, monkeypatch):
    monkeypatch.setattr(retrieve, "NEAREST_TERMS", 0)
    async with db.acquire() as conn:
        ids = await _library(conn)
        # "dormant" names no term; the question's passage is the account rules' dormancy, which
        # account cites, so account seeds the walk and offers its other passages
        question = "When is a holding dormant?"
        (hit,) = await retrieve.retrieve(conn, question, None, EMBEDDER, k=1)
        assert (hit.document_id, hit.ordinal) == (ids["rules"], 2)
        found = await retrieve.retrieve_with_neighbours(
            conn, question, None, EMBEDDER, k=1, budget=10, retrieval=retrieve.GRAPH
        )
        labels = [c.reached for c in found if c.reached.startswith("via ")]
        assert labels[:2] == ["via account", "via account"]
        assert "via account -> client (part of)" not in labels  # client cites nothing


async def test_the_term_nearest_the_question_seeds_the_walk(db, named_only):
    async with db.acquire() as conn:
        ids = await _library(conn)
        # no term is named and the question's passage is cited by none: ledger ("The books.")
        # is the term nearest it
        question = "Which books?"
        assert retrieve.named_terms(question, await terms.in_service(conn)) == []
        found = await retrieve.retrieve_with_neighbours(
            conn, question, None, EMBEDDER, k=1, budget=10, retrieval=retrieve.GRAPH
        )
        # the question's passage is the policy's statements, which ledger cites: not given again
        assert (found[0].document_id, found[0].ordinal) == (ids["policy"], 2)
        via = [c for c in found if c.reached.startswith("via ")]
        # from ledger the walk reaches account (0.4), which offers its passages
        assert via and {c.reached for c in via} == {"via ledger -> account (related to)"}
        assert len(set(_keys(found))) == len(found)


async def test_a_citation_of_a_passage_no_longer_held_is_not_offered(db):
    async with db.acquire() as conn:
        ids = await _library(conn)
        # a new version of the account rules left no passage 9; the draft still names it
        await _cite(conn, ids["client"], ids["rules"], [9])
        found = await retrieve.retrieve_with_neighbours(
            conn, "Who is a client?", None, EMBEDDER, k=1, budget=20, retrieval=retrieve.GRAPH
        )
        assert (ids["rules"], 9) not in _keys(found)
        assert any(c.term == "account" for c in found)


async def test_a_draft_awaiting_review_cites_nothing(db):
    async with db.acquire() as conn:
        rules = await _add(conn, "accounts.md", ACCOUNTS, ["*"])
        account = await _term(conn, "account", None, {"sales"})
        await _cite(conn, account, rules, [1], status="draft")
        assert await terms.citations(conn, [rules]) == {}
        await _cite(conn, account, rules, [2], status="accepted")
        assert await terms.citations(conn, [rules]) == {account: [(rules, 2)]}


async def test_a_role_walks_only_the_terms_relationships_and_documents_it_may_read(db, named_only):
    async with db.acquire() as conn:
        ids = await _library(conn)
        secret = await _add(
            conn, "secret.md", b"# Secret\n\nA client account ledger secret.", ["finance"]
        )
        hidden = await _term(conn, "vault", "A finance-only term.", {"finance"})
        await glossary_repo.add_edge(conn, hidden, ids["client"], "SYNONYM_OF")
        await glossary_repo.add_edge(conn, hidden, ids["ledger"], "SYNONYM_OF")
        await _cite(conn, ids["account"], secret, [0])
        await _cite(conn, hidden, ids["rules"], [1])
        question = "Who is a client?"

        everyone = await retrieve.retrieve_with_neighbours(
            conn, question, None, EMBEDDER, k=1, budget=20, retrieval=retrieve.GRAPH
        )
        # unscoped, the finance term conducts: ledger is reached through it at 1.0
        assert any(c.term == "vault" for c in everyone)
        assert any(
            c.reached == "via client -> vault (synonym of) -> ledger (synonym of)" for c in everyone
        )
        assert secret in {c.document_id for c in everyone}

        sales = await retrieve.retrieve_with_neighbours(
            conn, question, frozenset({"sales"}), EMBEDDER, k=1, budget=20, retrieval=retrieve.GRAPH
        )
        assert sales
        # the finance term is neither a seed, nor reached, nor walked through
        assert not any(c.term == "vault" or "vault" in c.reached for c in sales)
        assert {c.reached for c in sales if c.term == "ledger"} == {
            "via client -> account (part of) -> ledger (related to)"
        }
        # nor is a passage of a document the role may not read offered, though account cites it
        assert secret not in {c.document_id for c in sales}

        finance = await retrieve.retrieve_with_neighbours(
            conn,
            question,
            frozenset({"finance"}),
            EMBEDDER,
            k=1,
            budget=20,
            retrieval=retrieve.GRAPH,
        )
        # the sales terms are out of reach for finance: only its own and the whole org's
        assert {c.term for c in finance if c.term} <= {"vault"}


# -- the terms' embeddings ----------------------------------------------------------------------


async def _held_vectors(conn) -> dict[int, tuple[str, str]]:
    rows = await conn.execute_core(
        select(
            glossary_term_vectors.c.term_id,
            glossary_term_vectors.c.embedding_model,
            glossary_term_vectors.c.digest,
        )
    )
    return {r[0]: (r[1], r[2]) for r in rows.fetchall()}


async def _term_indexes(conn) -> set[str]:
    rows = await conn.execute_core(
        sa.text(
            "SELECT indexname FROM pg_indexes WHERE schemaname = current_schema() "
            "AND tablename = 'glossary_term_vectors'"
        )
    )
    return {r[0] for r in rows.fetchall() if r[0].startswith("glossary_term_vectors_vector_")}


async def test_the_terms_are_embedded_by_name_and_definition_under_a_vector_index(db):
    async with db.acquire() as conn:
        client = await _term(conn, "client", "A party the bank serves.", {"sales"})
        bare = await _term(conn, "account", None, {"sales"})
        held = await terms.in_service(conn)
        assert await terms.refresh(conn, held, EMBEDDER) == 2
        assert calls[-1] == ["client\nA party the bank serves.", "account"]
        assert set(await _held_vectors(conn)) == {client, bare}
        assert await _term_indexes(conn) == {f"glossary_term_vectors_vector_{len(VOCABULARY)}"}
        # nothing changed: nothing embedded again
        assert await terms.refresh(conn, await terms.in_service(conn), EMBEDDER) == 0

        nearest = await terms.nearest_terms(conn, _vector("bank party"), [client, bare], 1)
        assert [t for t, _ in nearest] == [client]
        sql, params = terms.nearest_terms_sql(_vector("bank party"), [client, bare], 1)
        async with conn.transaction():
            await conn.execute_core(sa.text("SET LOCAL enable_seqscan = off"))
            explained = await conn.execute_core(sa.text(f"EXPLAIN {sql}").bindparams(**params))
            plan = "\n".join(r[0] for r in explained.fetchall())
        assert f"glossary_term_vectors_vector_{len(VOCABULARY)}" in plan


async def test_a_term_is_embedded_again_when_its_name_or_definition_changes(db):
    async with db.acquire() as conn:
        client = await _term(conn, "client", "A party the bank serves.", {"sales"})
        account = await _term(conn, "account", "Where money is held.", {"sales"})
        await terms.refresh(conn, await terms.in_service(conn), EMBEDDER)
        before = await _held_vectors(conn)

        await glossary_repo.set_definition(conn, client, "A customer of the bank.")
        assert await terms.refresh(conn, await terms.in_service(conn), EMBEDDER) == 1
        assert calls[-1] == ["client\nA customer of the bank."]
        await glossary_repo.rename_term(conn, account, "deposit account")
        assert await terms.refresh(conn, await terms.in_service(conn), EMBEDDER) == 1
        after = await _held_vectors(conn)
        assert after[client] != before[client] and after[account] != before[account]

        # retiring a term removes its embedding: it seeds nothing
        await conn.execute_core(
            update(glossary_terms).where(glossary_terms.c.id == account).values(retired=True)
        )
        assert await terms.refresh(conn, await terms.in_service(conn), EMBEDDER) == 0
        assert set(await _held_vectors(conn)) == {client}


def _other(text: str) -> list[float]:
    return [float(len(text)), float(text.count(" ")) + 0.5, 1.0]


async def _embed_other(texts: list[str]) -> list[list[float]]:
    return [_other(t) for t in texts]


async def test_every_term_is_embedded_again_when_the_model_changes(db):
    async with db.acquire() as conn:
        await _term(conn, "client", "A party the bank serves.", {"sales"})
        await _term(conn, "account", None, {"sales"})
        await terms.refresh(conn, await terms.in_service(conn), EMBEDDER)
        other = library.Embedder(model="other-embed", embed=_embed_other)
        assert await terms.refresh(conn, await terms.in_service(conn), other) == 2
        assert {m for m, _ in (await _held_vectors(conn)).values()} == {"other-embed"}
        # the earlier model's dimension has no embedding left: its index is dropped
        assert await _term_indexes(conn) == {"glossary_term_vectors_vector_3"}


async def test_graph_retrieval_refreshes_the_terms_before_it_seeds_from_them(db):
    async with db.acquire() as conn:
        ids = await _library(conn)
        await retrieve.retrieve_with_neighbours(
            conn, "Who is a client?", None, EMBEDDER, k=1, retrieval=retrieve.GRAPH
        )
        assert set(await _held_vectors(conn)) == {ids["client"], ids["account"], ids["ledger"]}
        await glossary_repo.set_definition(conn, ids["ledger"], "The bank's books of account.")
        calls.clear()
        await retrieve.retrieve_with_neighbours(
            conn, "Who is a client?", None, EMBEDDER, k=1, retrieval=retrieve.GRAPH
        )
        assert ["ledger\nThe bank's books of account."] in calls


# -- the Admin setting --------------------------------------------------------------------------


def _request() -> Request:
    fake = types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="alice", roles=[], acting_roles=[]),
            active_org_id="default",
        )
    )
    return cast("Request", fake)


@pytest.fixture
def api(db, monkeypatch):
    monkeypatch.setattr(router, "_pool", lambda: db)
    monkeypatch.setattr(router, "_guard", lambda request: "default")

    async def embedder():
        return EMBEDDER

    monkeypatch.setattr(router, "_embedder", embedder)
    return db


async def test_the_admin_api_shows_and_changes_the_retrieval_setting(api):
    assert await router.get_settings(_request()) == {"retrieval": "passage"}
    async with api.acquire() as conn:
        term = await _term(conn, "client", "A party.", {"sales"})
    assert await router.put_settings({"retrieval": "graph"}, _request()) == {"retrieval": "graph"}
    assert await router.get_settings(_request()) == {"retrieval": "graph"}
    # choosing graph embeds the glossary's terms there and then
    async with api.acquire() as conn:
        assert set(await _held_vectors(conn)) == {term}
    assert await router.put_settings({"retrieval": "passage"}, _request()) == {
        "retrieval": "passage"
    }
    assert await retrieve.retrieval_setting(api) == "passage"
    for bad, says in (
        ({"retrieval": "both"}, "retrieval must be one of passage, graph"),
        ({"retrieval": "graph", "k": 3}, "'k' is not a library setting"),
    ):
        with pytest.raises(ApiError) as refused:
            await router.put_settings(bad, _request())
        assert refused.value.status_code == 400 and says in str(refused.value)


async def test_the_assistant_s_tool_retrieves_as_the_org_s_setting_says(api, monkeypatch):
    from provisa.api.mcp import tools

    async def embedder(state):
        return EMBEDDER

    monkeypatch.setattr(retrieve, "org_embedder", embedder)
    monkeypatch.setattr(tools, "require_role", lambda role, state: role)
    state = types.SimpleNamespace(
        model_db=api,
        roles={"analyst": {"id": "analyst", "domain_access": ["sales"], "capabilities": []}},
    )
    async with api.acquire() as conn:
        await _library(conn)
    question = "Who is a client?"
    passage = await tools.search_reference_library(cast(Any, state), "analyst", question, k=1)
    assert not any(f["reached"].startswith("via ") for f in passage)
    await router.put_settings({"retrieval": "graph"}, _request())
    found = await tools.search_reference_library(cast(Any, state), "analyst", question, k=2)
    via = [f for f in found if f["reached"].startswith("via ")]
    assert via and via[0]["term"] == "account"
    assert via[0]["term_definition"] == "Where a client's money is held."


# -- acceptance: the bank secrecy demo (REQ-2185, GRAPH RETRIEVAL) ------------------------------

ECFR = ROOT / "tests" / "fixtures" / "ecfr"
DEMO = ROOT / "demo" / "named" / "banksecrecy"
QUESTION = "which cash deposits need a currency transaction report"

#: The demo's vector model, sentence-transformers/all-MiniLM-L6-v2, as it embedded every text the
#: case embeds -- the three sections' passages, the demo's terms and the question -- computed once
#: with the model and kept beside the sections (tests/fixtures/ecfr), so the case runs offline.
MINILM: dict[str, list[float]] = json.loads((ECFR / "banksecrecy-minilm.json").read_text())


async def _minilm(texts: list[str]) -> list[list[float]]:
    return [MINILM[t] for t in texts]


DEMO_MODEL = library.Embedder(model="sentence-transformers/all-MiniLM-L6-v2", embed=_minilm)


async def _demo_glossary(conn) -> dict[str, int]:
    """The demo's glossary as its config.yaml declares it: its terms, definitions, domains and
    relationships."""
    declared = yaml.safe_load((DEMO / "config.yaml").read_text())["glossary_terms"]
    ids = {
        t["name"]: await _term(
            conn,
            t["name"],
            t.get("definition"),
            set(t["domains"]),
            approved=t.get("approve", False),
        )
        for t in declared
    }
    for t in declared:
        for edge in t.get("edges", ()):
            await glossary_repo.add_edge(conn, ids[t["name"]], ids[edge["to"]], edge["rel_type"])
    return ids


async def _ecfr(conn, section: str) -> int:
    """31 CFR ``section`` as eCFR's API serves it, read into the library as the demo's scans do."""
    url = (
        "https://www.ecfr.gov/api/versioner/v1/full/2026-10-15/title-31.xml"
        f"?part=1010&section={section}"
    )
    data = (ECFR / f"31-cfr-{section}.xml").read_bytes()
    return await library.add(
        conn,
        library.Content(data, "application/xml", None, url),
        source_kind=store.URL,
        domains=["banksecrecy"],
        status=store.KEPT,
        embedder=DEMO_MODEL,
        added_by="alice",
    )


def test_the_demo_glossary_relates_the_report_to_multiple_transactions():
    declared = yaml.safe_load((DEMO / "config.yaml").read_text())["glossary_terms"]
    edges = {(t["name"], e["to"], e["rel_type"]) for t in declared for e in t.get("edges", ())}
    assert ("multiple transactions", "currency transaction report", "PART_OF") in edges


async def test_graph_retrieval_answers_the_demo_question_with_the_aggregation_rule(db):
    async with db.acquire() as conn:
        ids = await _demo_glossary(conn)
        filing = await _ecfr(conn, "1010.311")
        aggregation = await _ecfr(conn, "1010.313")
        await _ecfr(conn, "1010.100")
        held = {o: t for o, _, _, t in await store.passages(conn, aggregation)}
        (rule,) = [o for o, t in held.items() if t.startswith("(b) Multiple transactions.")]
        # the scans' citations: the report in the filing obligation, multiple transactions in
        # the aggregation rule
        await _cite(conn, ids["currency transaction report"], filing, [0])
        await _cite(conn, ids["multiple transactions"], aggregation, [rule])
        role = frozenset({"banksecrecy"})

        by_passage = await retrieve.retrieve_with_neighbours(conn, QUESTION, role, DEMO_MODEL)
        by_graph = await retrieve.retrieve_with_neighbours(
            conn, QUESTION, role, DEMO_MODEL, retrieval=retrieve.GRAPH
        )
        record = {
            mode: [f"{c.document_title} [{c.ordinal}] {c.reached}" for c in found]
            for mode, found in (("passage", by_passage), ("graph", by_graph))
        }
        print(json.dumps(record, indent=1))

        # Both return the aggregation rule. Under the demo's model its passage is itself the one
        # nearest the question, so graph retrieval holds it from the question's passages, as
        # passage retrieval does, and the walk's offer of it is not given twice.
        rule_key = (aggregation, rule)
        assert by_passage[0] == by_graph[0]
        assert (by_graph[0].document_id, by_graph[0].ordinal) == rule_key
        assert by_graph[0].reached == retrieve.NEAREST_QUESTION
        assert len(set(_keys(by_graph))) == len(by_graph)

        # The walk itself reaches it from the term the question names, whatever the question's
        # passages are: its candidates with none held.
        documents = await retrieve._readable_documents(conn, role, DEMO_MODEL)
        query = await retrieve._embed_question(QUESTION, DEMO_MODEL)
        offered = await retrieve._graph_candidates(
            conn, QUESTION, query, [], documents, role, DEMO_MODEL
        )
        print(
            json.dumps([f"{c.document_title} [{c.ordinal}] {c.reached}" for c in offered], indent=1)
        )
        (found,) = [c for c in offered if (c.document_id, c.ordinal) == rule_key]
        assert found.reached == "via currency transaction report -> multiple transactions (part of)"
        assert found.term == "multiple transactions"
        assert found.term_definition is not None and "1010.313(b)" in found.term_definition
        # the report's own citation comes first: the seed, before the term one hop away
        assert offered[0].reached == "via currency transaction report"
        assert (offered[0].document_id, offered[0].ordinal) == (filing, 0)
