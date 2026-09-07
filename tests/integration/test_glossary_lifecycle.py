# Copyright (c) 2026 Kenneth Stott
# Canary: 1c5b8f24-7e9a-4d2c-b3f6-9a0e4c7d2b88
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1387, REQ-1581: glossary term lifecycle against the real catalog registration flow.

A real PG metadata DB through the real config loader: registration derives and dedups
terms, a column departure on reload removes its term, a full-replace reload that drops a
table sweeps the orphaned terms, and an abstract term hanging on a departing term flips
the outcome from remove to deprecate — with relink reviving the SAME term row.
"""

# Requirements: REQ-1387, REQ-1581

from pathlib import Path

import pytest
import pytest_asyncio

from provisa.core import domain_policy
from provisa.core.config_loader import load_config, parse_config_dict
from provisa.core.repositories import glossary as glossary_repo

pytestmark = [pytest.mark.integration]

SCHEMA_SQL = (Path(__file__).parent.parent.parent / "provisa" / "core" / "schema.sql").read_text()


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def _init_schema(tenant_db):
    async with tenant_db.acquire() as conn:
        await conn.execute(SCHEMA_SQL)


@pytest_asyncio.fixture(autouse=True)
async def _clean(tenant_db, _init_schema):
    domain_policy.reset()
    async with tenant_db.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE glossary_term_experts, glossary_term_edges, glossary_term_refs,
                     glossary_terms, rls_rules, relationships, relationship_candidates,
                     table_columns, registered_tables, naming_rules, roles, domains,
                     sources CASCADE
            """
        )
    yield
    domain_policy.reset()


def _config(tables: dict) -> dict:
    return {
        "sources": [
            {
                "id": "pg1",
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "d",
                "username": "u",
                "password": "p",
            }
        ],
        "domains": [{"id": "sales", "description": "Sales"}],
        "tables": [
            {
                "source_id": "pg1",
                "domain_id": "sales",
                "schema": "public",
                "table": name,
                # REQ-1426: a design carries a type for every column; the loader assigns none.
                # REQ-1581: a column entry is either the physical name or (name, alias) -- the
                # alias is the business name the term derives from.
                "columns": [
                    {
                        "name": c if isinstance(c, str) else c[0],
                        "data_type": "text",
                        "visible_to": ["admin"],
                        **({} if isinstance(c, str) else {"alias": c[1]}),
                    }
                    for c in columns
                ],
            }
            for name, columns in tables.items()
        ],
        "roles": [{"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}],
    }


async def _load(conn, tables: dict, *, replace: bool = False) -> None:
    await load_config(parse_config_dict(_config(tables)), conn, replace=replace)


async def _terms(conn) -> dict[str, dict]:
    return {t["name"]: t for t in await glossary_repo.list_terms(conn)}


@pytest.mark.asyncio(loop_scope="session")
async def test_registration_derives_and_dedups_terms(tenant_db):
    async with tenant_db.acquire() as conn:
        await _load(
            conn,
            {
                "orders": ["cust_id", "order_dt"],
                "customers": ["customerId", "CUSTOMER_KEY", "region_cd"],
            },
        )
        terms = await _terms(conn)
    assert terms["customer"]["ref_count"] == 3  # cust_id + customerId + CUSTOMER_KEY
    assert terms["order date"]["ref_count"] == 1
    assert terms["region"]["ref_count"] == 1  # region_cd: trailing proxy 'code' stripped


@pytest.mark.asyncio(loop_scope="session")
async def test_column_departure_on_reload_removes_its_term(tenant_db):
    async with tenant_db.acquire() as conn:
        await _load(conn, {"orders": ["cust_id", "order_dt"]})
        await _load(conn, {"orders": ["cust_id"]})
        terms = await _terms(conn)
    assert "order date" not in terms
    assert terms["customer"]["ref_count"] == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_replace_reload_dropping_a_table_sweeps_its_terms(tenant_db):
    async with tenant_db.acquire() as conn:
        await _load(conn, {"orders": ["cust_id"], "shipments": ["carrier_nm"]})
        assert "carrier name" in await _terms(conn)
        await _load(conn, {"orders": ["cust_id"]}, replace=True)
        terms = await _terms(conn)
    assert "carrier name" not in terms
    assert "customer" in terms


@pytest.mark.asyncio(loop_scope="session")
async def test_abstract_dependent_flips_removal_to_deprecation_and_relink_revives(tenant_db):
    async with tenant_db.acquire() as conn:
        await _load(conn, {"orders": ["order_dt"]})
        terms = await _terms(conn)
        abstract_id = await glossary_repo.create_abstract_term(conn, "business date", domains=set())
        await glossary_repo.add_edge(conn, abstract_id, terms["order date"]["id"], "KIND_OF")

        await _load(conn, {"orders": ["placed_ts"]})
        after = await _terms(conn)
        assert after["order date"]["deprecated"] is True
        assert after["order date"]["ref_count"] == 0
        assert "business date" in after  # the abstract term was never left dangling

        await _load(conn, {"orders": ["placed_ts", "order_dt"]})
        revived = await _terms(conn)
        assert revived["order date"]["deprecated"] is False
        assert revived["order date"]["id"] == terms["order date"]["id"]
        detail = await glossary_repo.get_term(conn, revived["order date"]["id"])
        assert {(e["rel_type"], e["name"]) for e in detail["edges_in"]} == {
            ("KIND_OF", "business date")
        }


class TestTheTermFollowsTheColumnsBusinessName:
    """REQ-1581: aliasing the column is the stronger correction, so the term derives from the
    alias.

    A term rename fixes one catalog entry; the column still reads ``usr_nm`` to the next agent
    that queries it. An alias travels with the data to every surface, and the glossary tracks it
    rather than asking for the same correction twice -- but only while the term it would move off
    is still an untouched proposal. Curator work outranks a later alias edit.
    """

    @pytest.mark.asyncio(loop_scope="session")
    async def test_an_alias_names_the_term_not_the_physical_column(self, tenant_db):
        async with tenant_db.acquire() as conn:
            await _load(conn, {"people": [("usr_nm", "user name")]})
            terms = await _terms(conn)
        assert "user name" in terms
        assert terms["user name"]["ref_count"] == 1
        assert "usr name" not in terms  # what usr_nm alone would have produced

    @pytest.mark.asyncio(loop_scope="session")
    async def test_aliasing_a_column_moves_its_proposal(self, tenant_db):
        async with tenant_db.acquire() as conn:
            await _load(conn, {"people": ["usr_nm"]})
            assert "usr name" in await _terms(conn)
            await _load(conn, {"people": [("usr_nm", "member handle")]})
            terms = await _terms(conn)
        assert terms["member handle"]["ref_count"] == 1
        assert "usr name" not in terms  # the proposal it left behind held nothing worth keeping

    @pytest.mark.asyncio(loop_scope="session")
    async def test_a_defined_term_keeps_its_column_when_the_alias_changes(self, tenant_db):
        async with tenant_db.acquire() as conn:
            await _load(conn, {"people": ["usr_nm"]})
            defined = (await _terms(conn))["usr name"]["id"]
            await glossary_repo.set_definition(conn, defined, "the person who signs in")
            await _load(conn, {"people": [("usr_nm", "member handle")]})
            terms = await _terms(conn)
        assert terms["usr name"]["ref_count"] == 1
        assert terms["usr name"]["id"] == defined
        assert "member handle" not in terms

    @pytest.mark.asyncio(loop_scope="session")
    async def test_an_unaliased_column_still_derives_from_its_physical_name(self, tenant_db):
        async with tenant_db.acquire() as conn:
            await _load(conn, {"people": [("usr_nm", "user name"), "region_cd"]})
            terms = await _terms(conn)
        assert terms["region"]["ref_count"] == 1


class TestAConfigDeclaredTermGroundsThroughAnEdge:
    """A config-declared abstract term is inert on its own -- ``live_term_ids()`` requires it be
    grounded, reached by an edge from a term that holds a real column ref. ``glossary_terms:``
    entries declare that edge by target-term name (REQ-1641); the loader resolves it after every
    term this config declares or derives exists, so the edge can target a term this same config
    load derives from a column.
    """

    @pytest.mark.asyncio(loop_scope="session")
    async def test_an_edge_to_a_derived_term_makes_the_abstract_term_live(self, tenant_db):
        config = _config({"orders": ["cust_id"]})
        config["glossary_terms"] = [
            {
                "name": "buyer",
                "definition": "The party responsible for an order, regardless of channel.",
                "domains": ["sales"],
                "edges": [{"to": "customer", "rel_type": "KIND_OF"}],
            }
        ]
        async with tenant_db.acquire() as conn:
            await load_config(parse_config_dict(config), conn, replace=False)
            terms = await _terms(conn)
            buyer = terms["buyer"]
            customer = terms["customer"]
            detail = await glossary_repo.get_term(conn, buyer["id"])
        assert buyer["is_abstract"] is True
        assert buyer["ref_count"] == 0  # holds no column refs of its own
        assert customer["is_abstract"] is False  # the target is untouched by the edge
        assert {(e["rel_type"], e["name"]) for e in detail["edges_out"]} == {
            ("KIND_OF", "customer")
        }
        assert buyer["live"] is True  # in-service, defined, and reachable to a rooted term

    @pytest.mark.asyncio(loop_scope="session")
    async def test_declaring_a_definition_for_a_not_yet_synced_column_still_ends_concrete(
        self, tenant_db
    ):
        """Glossary term upsert (loader step 4.6) runs before table/column sync (step 5), so a
        config entry naming a column this same load derives -- to give it a definition -- creates
        the row abstract first. Sync must then flip it back: a term holding a physical ref is
        concrete regardless of which loader step touched it first.
        """
        config = _config({"orders": [("cust_id", "customer")]})
        config["glossary_terms"] = [
            {
                "name": "customer",
                "definition": "The party an order belongs to.",
                "domains": ["sales"],
            }
        ]
        async with tenant_db.acquire() as conn:
            await load_config(parse_config_dict(config), conn, replace=False)
            terms = await _terms(conn)
            customer = terms["customer"]
        assert customer["is_abstract"] is False
        assert customer["ref_count"] == 1
        assert customer["definition"] == "The party an order belongs to."

    @pytest.mark.asyncio(loop_scope="session")
    async def test_a_reload_demotes_an_already_held_ref_stuck_abstract(self, tenant_db):
        """A ref created by the step-ordering bug (glossary upsert before table sync) points at
        an abstract-but-curated term. On the NEXT load, sync sees the ref already held and the
        term already curated/name-matched, so it takes the early-continue path instead of
        ``_find_or_create_term`` -- that path must demote ``is_abstract`` too, or a term stays
        stuck abstract forever even after the create-path fix, since every later reload just
        re-confirms the same stale ref without ever re-resolving it.
        """
        config = _config({"orders": [("cust_id", "customer")]})
        config["glossary_terms"] = [
            {
                "name": "customer",
                "definition": "The party an order belongs to.",
                "domains": ["sales"],
            }
        ]
        parsed = parse_config_dict(config)
        async with tenant_db.acquire() as conn:
            await load_config(parsed, conn, replace=False)
            first = (await _terms(conn))["customer"]
            assert first["is_abstract"] is False  # create-path fix already covers this load

            # Simulate a row a pre-fix load left behind: concrete in every way (curated, holding
            # a ref) but with the stale flag, so the SECOND load's early-continue path -- which
            # sees the ref already held and skips ``_find_or_create_term`` entirely -- is what
            # has to correct it, not the create path exercised above.
            await conn.execute(
                "UPDATE glossary_terms SET is_abstract = TRUE WHERE id = $1", first["id"]
            )

            await load_config(parsed, conn, replace=False)
            second = (await _terms(conn))["customer"]
        assert second["is_abstract"] is False
        assert second["id"] == first["id"]
        assert second["ref_count"] == 1

    @pytest.mark.asyncio(loop_scope="session")
    async def test_an_edge_to_a_name_absent_from_config_and_catalog_fails_loudly(self, tenant_db):
        config = _config({"orders": ["cust_id"]})
        config["glossary_terms"] = [
            {
                "name": "buyer",
                "domains": ["sales"],
                "edges": [{"to": "nonexistent term", "rel_type": "KIND_OF"}],
            }
        ]
        async with tenant_db.acquire() as conn:
            with pytest.raises(ValueError, match="nonexistent term"):
                await load_config(parse_config_dict(config), conn, replace=False)
