# Copyright (c) 2026 Kenneth Stott
# Canary: 1c5b8f24-7e9a-4d2c-b3f6-9a0e4c7d2b88
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1387: glossary term lifecycle against the real catalog registration flow.

A real PG metadata DB through the real config loader: registration derives and dedups
terms, a column departure on reload removes its term, a full-replace reload that drops a
table sweeps the orphaned terms, and an abstract term hanging on a departing term flips
the outcome from remove to deprecate — with relink reviving the SAME term row.
"""

# Requirements: REQ-1387

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


def _config(tables: dict[str, list[str]]) -> dict:
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
                "columns": [
                    {"name": c, "data_type": "text", "visible_to": ["admin"]} for c in columns
                ],
            }
            for name, columns in tables.items()
        ],
        "roles": [{"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}],
    }


async def _load(conn, tables: dict[str, list[str]], *, replace: bool = False) -> None:
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
        abstract_id = await glossary_repo.create_abstract_term(conn, "business date")
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
