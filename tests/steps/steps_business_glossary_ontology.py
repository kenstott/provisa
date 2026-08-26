# Copyright (c) 2026 Kenneth Stott
# Canary: 5d2f8a37-1e6b-4c90-a4d7-3b8e0f5c9a12
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-1387 — Business Glossary & Ontology.

The feature file is GENERATED from the REQ-1387 scenario block in
docs/arch/requirements.yaml (do not hand-edit it; these steps track its text).
Terms derive at registration through the real table repository — the single
table_columns write path carries the glossary sync — so the steps drive
table_repo.upsert against a real SQLite store and observe the derived
vocabulary: normalization dedup, curation, and the remove-or-deprecate
lifecycle with dangling-abstract-term prevention.
"""

# Requirements: REQ-1387

from __future__ import annotations

import asyncio

import pytest
from pytest_bdd import given, scenarios, then, when
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.models import Column, Table
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import (
    glossary_term_domains,
    glossary_term_edges,
    glossary_term_experts,
    glossary_term_refs,
    glossary_terms,
    registered_tables,
    roles,
    table_columns,
)

scenarios("../features/REQ-1387.feature")

_TABLES = [
    registered_tables,
    table_columns,
    roles,
    glossary_terms,
    glossary_term_refs,
    glossary_term_domains,
    glossary_term_edges,
    glossary_term_experts,
]


@pytest.fixture
def shared_data(tmp_path) -> dict:
    """Per-scenario SQLite store + state passed between steps."""
    path = tmp_path / "glossary.db"

    async def _init() -> None:
        engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
        async with engine.begin() as c:
            await c.run_sync(lambda s: registered_tables.metadata.create_all(s, tables=_TABLES))
        await engine.dispose()

    asyncio.run(_init())
    return {"db_path": path}


def _run(shared_data: dict, fn):
    """Run one async repository interaction against the scenario's store."""

    async def _go():
        engine = create_async_engine(f"sqlite+aiosqlite:///{shared_data['db_path']}")
        try:
            async with Database(engine, name="bdd").acquire() as conn:
                return await fn(conn)
        finally:
            await engine.dispose()

    return asyncio.run(_go())


def _register(shared_data: dict, table_name: str, columns: list[str]) -> None:
    table = Table(
        source_id="__derived__",
        domain_id="d",
        schema_name="s",
        table_name=table_name,
        columns=[Column(name=c, data_type="text", visible_to=[]) for c in columns],
        view_sql="SELECT 1",
    )
    _run(shared_data, lambda conn: table_repo.upsert(conn, table))


def _terms(shared_data: dict) -> dict[str, dict]:
    rows = _run(shared_data, glossary_repo.list_terms)
    return {t["name"]: t for t in rows}


def _detail(shared_data: dict, term_id: int) -> dict:
    detail = _run(shared_data, lambda conn: glossary_repo.get_term(conn, term_id))
    assert detail is not None
    return detail


@given("tables with columns cust_id, customerId, and CUSTOMER_KEY registered in the semantic layer")
def given_tables_with_variants(shared_data: dict) -> None:
    _register(shared_data, "orders", ["cust_id", "order_dt"])
    _register(shared_data, "customers", ["customerId", "CUSTOMER_KEY"])


@when("deterministic normalization runs")
def when_deterministic_normalization_runs(shared_data: dict) -> None:
    # Normalization ran inside registration (it rides the table upsert); observe its output.
    shared_data["terms"] = _terms(shared_data)


@then("the fields resolve to deduplicated terms each listing its physical refs")
def then_fields_deduplicated_with_refs(shared_data: dict) -> None:
    customer = shared_data["terms"]["customer"]
    assert customer["ref_count"] == 3
    refs = _detail(shared_data, customer["id"])["refs"]
    assert {(r["table_name"], r["column_name"]) for r in refs} == {
        ("orders", "cust_id"),
        ("customers", "customerId"),
        ("customers", "CUSTOMER_KEY"),
    }
    shared_data["customer_id"] = customer["id"]
    shared_data["order_date_id"] = shared_data["terms"]["order date"]["id"]


@then(
    "a user can move a physical ref to another term, rename the term, and record a "
    "definition and experts"
)
def then_curation_round_trip(shared_data: dict) -> None:
    term_id = shared_data["customer_id"]
    refs = _detail(shared_data, term_id)["refs"]
    ref = next(r for r in refs if r["column_name"] == "CUSTOMER_KEY")
    assert _run(
        shared_data,
        lambda conn: glossary_repo.move_ref(
            conn, ref["table_id"], ref["column_name"], shared_data["order_date_id"]
        ),
    )
    assert _run(shared_data, lambda conn: glossary_repo.rename_term(conn, term_id, "client"))
    _run(
        shared_data,
        lambda conn: glossary_repo.set_definition(conn, term_id, "The buying party."),
    )
    _run(
        shared_data,
        lambda conn: glossary_repo.add_expert(conn, term_id, "alice", kind="author"),
    )
    detail = _detail(shared_data, term_id)
    assert detail["name"] == "client"
    assert detail["definition"] == "The buying party."
    assert detail["experts"] == [{"user_id": "alice", "kind": "author"}]
    assert {(r["table_name"], r["column_name"]) for r in detail["refs"]} == {
        ("orders", "cust_id"),
        ("customers", "customerId"),
    }


@then("a user can create an abstract term linked to a rooted term via KIND_OF")
def then_create_abstract(shared_data: dict) -> None:
    abstract_id = _run(
        shared_data, lambda conn: glossary_repo.create_abstract_term(conn, "party", domains=set())
    )
    _run(
        shared_data,
        lambda conn: glossary_repo.add_edge(
            conn, abstract_id, shared_data["customer_id"], "KIND_OF"
        ),
    )
    detail = _detail(shared_data, abstract_id)
    assert detail["is_abstract"]
    assert {(e["rel_type"], e["name"]) for e in detail["edges_out"]} == {("KIND_OF", "client")}


@when("the last physical ref of a term is deleted from the semantic layer")
def when_last_ref_deleted(shared_data: dict) -> None:
    # "order date" holds refs orders.order_dt and (moved) customers.CUSTOMER_KEY, and no
    # abstract term hangs on it. Re-register both tables without those columns: the
    # departure runs through the same single write path that created the term.
    _register(shared_data, "orders", ["cust_id"])
    _register(shared_data, "customers", ["customerId"])


@then("the term is removed")
def then_removed(shared_data: dict) -> None:
    assert "order date" not in _terms(shared_data)


@then("if an abstract term is connected to the rooted graph through the term")
def then_but_abstract_connected(shared_data: dict) -> None:
    # The contrasting branch: a rooted term WITH an abstract dependent loses its last ref.
    # "client" carries the KIND_OF edge from "party"; drop its remaining refs.
    _register(shared_data, "orders", [])
    _register(shared_data, "customers", [])


@then("the term is deprecated instead and the abstract term is not left dangling")
def then_deprecated_not_dangling(shared_data: dict) -> None:
    terms = _terms(shared_data)
    client = terms["client"]
    assert client["deprecated"]
    assert client["ref_count"] == 0
    party = _detail(shared_data, terms["party"]["id"])
    assert {(e["rel_type"], e["name"]) for e in party["edges_out"]} == {("KIND_OF", "client")}
