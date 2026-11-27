# Copyright (c) 2026 Kenneth Stott
# Canary: 9d2f6b41-8e3a-4c57-b1d0-6a7e2c9f4b18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-415: a relationship accepted from discovery with no name typed and none suggested takes an
id built from its ends -- source table, source column, target table and target column -- spelled
as an unquoted SQL identifier, with a short suffix only where that id is taken. On a real model
store (SQLite) and through the admin API."""

# Requirements: REQ-415

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from sqlalchemy import insert

from provisa.api.admin import discovery as discovery_api
from provisa.compiler.naming import name_problems
from provisa.core import schema_org as so
from provisa.core.database import Database, create_engine_from_url
from provisa.core.relationship_naming import discovered_id, ends_id
from provisa.core.repositories import relationship as relationship_repo
from provisa.discovery import candidates
from provisa.discovery.analyzer import RelationshipCandidate

_TABLES = (
    "sources",
    "domains",
    "registered_tables",
    "table_columns",
    "tag_assignments",
    "relationships",
    "naming_rules",
    "relationship_candidates",
    "glossary_terms",
    "glossary_term_refs",
    "glossary_term_domains",
    "glossary_term_edges",
    "glossary_term_experts",
    # REQ-1159: a term's domains include those of the commands whose output columns it means.
    "tracked_functions",
    "tracked_webhooks",
    "profiler_classifications",
    "rls_rules",
    "roles",
    "data_products",
)

_COMPLAINTS, _BUDGET = 1, 2
_SHAPES: dict[int, tuple[str, str, list[str]]] = {
    _COMPLAINTS: ("complaints_2026_09", "complaints", ["complaint_id", "neighborhood"]),
    _BUDGET: ("budget_2026__budget", "budget", ["neighborhood", "district"]),
}


# --- the id built from the ends ---------------------------------------------------------------


def test_the_id_is_built_from_the_ends():
    assert (
        ends_id("complaints_2026_09", "neighborhood", "budget_2026__budget", "area")
        == "complaints_2026_09__neighborhood__to__budget_2026_budget__area"
    )


def test_a_target_column_named_as_the_source_column_is_said_once():
    assert (
        ends_id("complaints_2026_09", "neighborhood", "budget_2026__budget", "neighborhood")
        == "complaints_2026_09__neighborhood__to__budget_2026_budget"
    )


# --- names needing normalisation --------------------------------------------------------------


@pytest.mark.parametrize(
    ("ends", "expected"),
    [
        # Case is folded.
        (("Orders", "CustomerID", "Customers", "ID"), "orders__customerid__to__customers__id"),
        # Spaces, dots, hyphens and runs of them are one underscore; none leads or trails.
        (
            ("sales.order lines", " store-code ", "dim.Stores", "code"),
            "sales_order_lines__store_code__to__dim_stores__code",
        ),
        # Accented letters keep their base letter.
        (("café", "rue", "quartier", "nom"), "cafe__rue__to__quartier__nom"),
        # A leading digit is not an unquoted identifier: the id is prefixed.
        (
            ("2026_permits", "address", "addresses", "line"),
            "rel_2026_permits__address__to__addresses__line",
        ),
    ],
)
def test_names_are_normalised_into_an_identifier(ends, expected):
    built = ends_id(*ends)
    assert built == expected
    assert name_problems(built, "sql") == []


def test_a_name_with_no_latin_letter_still_gives_a_distinct_identifier():
    one = ends_id("注文", "顧客", "顧客表", "番号")
    two = ends_id("注文", "店舗", "顧客表", "番号")
    assert one != two
    assert name_problems(one, "sql") == [] and name_problems(two, "sql") == []


def test_a_long_id_is_cut_to_the_identifier_limit_and_kept_apart():
    table = "a_very_long_table_name_holding_many_words_about_complaints"
    one = ends_id(table, "neighborhood_of_the_complaint", f"{table}_budget", "neighborhood_x")
    two = ends_id(table, "neighborhood_of_the_complaint", f"{table}_budget", "neighborhood_y")
    assert len(one) <= 63 and len(two) <= 63
    assert one != two
    assert one.startswith("a_very_long_table_name")
    assert name_problems(one, "sql") == [] and name_problems(two, "sql") == []


# --- a collision gets a suffix ----------------------------------------------------------------


def test_an_id_not_taken_has_no_suffix():
    assert discovered_id("orders", "store_code", "stores", "code", {"other"}) == (
        "orders__store_code__to__stores__code"
    )


def test_a_taken_id_gets_a_short_suffix():
    base = "orders__store_code__to__stores__code"
    assert discovered_id("orders", "store_code", "stores", "code", {base}) == f"{base}_2"
    assert (
        discovered_id("orders", "store_code", "stores", "code", {base.upper(), f"{base}_2"})
        == f"{base}_3"
    )


def test_a_suffixed_long_id_stays_within_the_limit():
    table = "a_very_long_table_name_holding_many_words_about_complaints"
    base = ends_id(table, "neighborhood", f"{table}_budget", "district")
    suffixed = discovered_id(table, "neighborhood", f"{table}_budget", "district", {base})
    assert suffixed != base and suffixed.endswith("_2") and len(suffixed) <= 63
    assert name_problems(suffixed, "sql") == []


# --- accepted on a real model store and through the admin API ---------------------------------


async def _store(tmp_path) -> Database:
    db = Database(
        create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'store.db'}"), name="disc-ids"
    )
    with db.engine.begin() as conn:
        so.metadata.create_all(conn, tables=[so.metadata.tables[n] for n in _TABLES])
    async with db.acquire() as conn:
        await conn.execute_core(insert(so.sources).values(id="drop", type="files"))
        await conn.execute_core(insert(so.domains).values(id="city"))
        for tid, (name, alias, columns) in _SHAPES.items():
            await conn.execute_core(
                insert(so.registered_tables).values(
                    id=tid,
                    source_id="drop",
                    domain_id="city",
                    schema_name="drop",
                    table_name=name,
                    alias=alias,
                )
            )
            for col in columns:
                await conn.execute_core(
                    insert(so.table_columns).values(
                        table_id=tid, domain_id="city", column_name=col, data_type="varchar"
                    )
                )
    return db


async def _suggest(conn: Any, target_column: str = "neighborhood") -> int:
    (candidate_id,) = await candidates.store_candidates(
        conn,
        [
            RelationshipCandidate(
                source_table_id=_COMPLAINTS,
                source_column="neighborhood",
                target_table_id=_BUDGET,
                target_column=target_column,
                cardinality="many-to-one",
                confidence=0.9,
                reasoning="same neighborhoods",
            )
        ],
        "run:1",
    )
    return candidate_id


_EXPECTED = "complaints_2026_09__neighborhood__to__budget_2026_budget"


async def test_an_unnamed_suggestion_is_accepted_under_the_id_of_its_ends(tmp_path):
    db = await _store(tmp_path)
    async with db.acquire() as conn:
        accepted = await candidates.accept(conn, await _suggest(conn))
    assert accepted["relationship_id"] == _EXPECTED


async def test_an_unnamed_suggestion_whose_id_is_taken_gets_a_suffix(tmp_path):
    db = await _store(tmp_path)
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(so.relationships).values(
                id=_EXPECTED,
                source_table_id=_BUDGET,
                target_table_id=_COMPLAINTS,
                source_column="district",
                target_column="complaint_id",
                cardinality="many-to-one",
            )
        )
        accepted = await candidates.accept(conn, await _suggest(conn))
    assert accepted["relationship_id"] == f"{_EXPECTED}_2"


async def test_a_typed_name_and_a_suggested_name_are_kept(tmp_path):
    db = await _store(tmp_path)
    async with db.acquire() as conn:
        typed = await candidates.accept(conn, await _suggest(conn), "complaint-budget")
        second = await _suggest(conn, "district")
        await conn.execute_core(
            so.relationship_candidates.update()
            .where(so.relationship_candidates.c.id == second)
            .values(suggested_name="complaint-district")
        )
        suggested = await candidates.accept(conn, second)
    assert typed["relationship_id"] == "complaint-budget"
    assert suggested["relationship_id"] == "complaint-district"


async def test_an_accepted_proposal_is_reachable_by_its_id_through_the_admin_api(
    monkeypatch, tmp_path
):
    db = await _store(tmp_path)
    monkeypatch.setattr(discovery_api, "state", SimpleNamespace(model_db=db))
    request = cast(Any, SimpleNamespace(headers={}, state=SimpleNamespace(identity=None)))
    async with db.acquire() as conn:
        candidate_id = await _suggest(conn)

    accepted = await discovery_api.accept_candidate(request, candidate_id, None)
    assert accepted["relationship_id"] == _EXPECTED

    async with db.acquire() as conn:
        rel = await relationship_repo.get(conn, _EXPECTED)
    assert rel is not None
    assert (
        rel["source_table_id"],
        rel["source_column"],
        rel["target_table_id"],
        rel["target_column"],
    ) == (_COMPLAINTS, "neighborhood", _BUDGET, "neighborhood")
