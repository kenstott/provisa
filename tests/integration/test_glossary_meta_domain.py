# Copyright (c) 2026 Kenneth Stott
# Canary: 8f4d2a19-6c73-4b0e-91af-3d5e2c7b8a64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1584, REQ-1585: the glossary and the tag graph as queryable meta-domain views.

Real PG, real catalog registration, the real view DDL the startup seed installs. The
questions the views exist to answer are asked in SQL: does the recursive `live` flag agree
with the Python admission rule it restates, does the synthesized column_key resolve a term
to its column, is a retired term returned with its flag rather than withheld, and does a
term reach the governance tags on the columns it binds.
"""

# Requirements: REQ-1584, REQ-1585

from pathlib import Path

import pytest
import pytest_asyncio

from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.api.startup_seed import _adapt_view_ddl
from provisa.core import domain_policy
from provisa.core.config_loader import load_config, parse_config_dict
from provisa.core.repositories import glossary as glossary_repo

pytestmark = [pytest.mark.integration]

SCHEMA_SQL = (Path(__file__).parent.parent.parent / "provisa" / "core" / "schema.sql").read_text()

# The views under test plus the two they join to.
_VIEWS = (
    "table_columns",
    "tag_assignments",
    "glossary_terms",
    "glossary_term_refs",
    "glossary_term_edges",
    "glossary_term_experts",
)


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
            TRUNCATE tag_assignments, glossary_term_experts, glossary_term_edges,
                     glossary_term_refs, glossary_terms, rls_rules, relationships,
                     relationship_candidates, table_columns, registered_tables,
                     naming_rules, roles, domains, sources CASCADE
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
                "columns": [
                    {"name": c, "data_type": "text", "visible_to": ["admin"]} for c in columns
                ],
            }
            for name, columns in tables.items()
        ],
        "roles": [{"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}],
    }


async def _setup(conn, tables: dict) -> None:
    await load_config(parse_config_dict(_config(tables)), conn)
    for tbl in _VIEWS:
        await conn.execute(_adapt_view_ddl(_META_TABLE_VIEWS[tbl], conn.capabilities.dialect))


async def _terms(conn) -> dict[str, dict]:
    return {t["name"]: t for t in await glossary_repo.list_terms(conn)}


@pytest.mark.asyncio(loop_scope="session")
async def test_live_flag_agrees_with_the_python_admission_rule(tenant_db):
    """The recursive CTE restates live_term_ids; it must not approximate it."""
    async with tenant_db.acquire() as conn:
        await _setup(conn, {"orders": ["cust_id", "order_dt"]})
        terms = await _terms(conn)
        # defined + rooted -> live; the abstract term reaches data only through it.
        await glossary_repo.set_definition(conn, terms["customer"]["id"], "A buying party.")
        abstract = await glossary_repo.create_abstract_term(conn, "party", domains=set())
        await glossary_repo.set_definition(conn, abstract, "Anyone we transact with.")
        await glossary_repo.add_edge(conn, abstract, terms["customer"]["id"], "KIND_OF")
        # defined but grounded in nothing, and rooted but undefined: neither is live.
        floating = await glossary_repo.create_abstract_term(conn, "ambition", domains=set())
        await glossary_repo.set_definition(conn, floating, "Wired to no column.")

        expected = await glossary_repo.live_ids(conn)
        rows = await conn.fetch("SELECT id, name, live FROM glossary_terms_meta")

    from_view = {r["id"] for r in rows if r["live"]}
    by_name = {r["name"]: r for r in rows}
    assert from_view == expected
    assert by_name["customer"]["live"] is True
    assert by_name["party"]["live"] is True  # grounded transitively over the edge
    assert by_name["ambition"]["live"] is False  # defined, but grounded in nothing
    assert by_name["order date"]["live"] is False  # rooted, but undefined


@pytest.mark.asyncio(loop_scope="session")
async def test_column_key_resolves_a_ref_to_its_column_and_address(tenant_db):
    async with tenant_db.acquire() as conn:
        await _setup(conn, {"orders": ["cust_id"]})
        rows = await conn.fetch(
            """
            SELECT t.name AS term, r.table_name, r.schema_name, r.source_id, r.domain_id,
                   c.column_name, c.data_type
            FROM glossary_term_refs_meta r
            JOIN glossary_terms_meta t ON t.id = r.term_id
            JOIN table_columns_meta c ON c.column_key = r.column_key
            """
        )
    assert len(rows) == 1
    row = rows[0]
    assert row["term"] == "customer"
    assert (row["source_id"], row["schema_name"], row["table_name"]) == ("pg1", "public", "orders")
    assert row["column_name"] == "cust_id"
    assert row["data_type"] == "text"


@pytest.mark.asyncio(loop_scope="session")
async def test_retired_and_export_excluded_terms_are_flagged_not_withheld(tenant_db):
    async with tenant_db.acquire() as conn:
        await _setup(conn, {"orders": ["cust_id", "order_dt"]})
        terms = await _terms(conn)
        await glossary_repo.set_retired(conn, terms["customer"]["id"], True)
        await glossary_repo.set_export_excluded(conn, terms["order date"]["id"], True)
        rows = {
            r["name"]: r
            for r in await conn.fetch(
                "SELECT name, retired, export_excluded, live FROM glossary_terms_meta"
            )
        }
    assert set(rows) == {"customer", "order date"}
    assert rows["customer"]["retired"] is True
    assert rows["customer"]["live"] is False  # out of service, so not admitted
    assert rows["order date"]["export_excluded"] is True


@pytest.mark.asyncio(loop_scope="session")
async def test_edges_and_experts_carry_their_endpoint_names(tenant_db):
    async with tenant_db.acquire() as conn:
        await _setup(conn, {"orders": ["cust_id"]})
        terms = await _terms(conn)
        abstract = await glossary_repo.create_abstract_term(conn, "party", domains=set())
        await glossary_repo.add_edge(conn, abstract, terms["customer"]["id"], "KIND_OF")
        await glossary_repo.add_expert(conn, abstract, "dana", kind="author")
        edges = await conn.fetch(
            "SELECT from_term, to_term, rel_type FROM glossary_term_edges_meta"
        )
        experts = await conn.fetch(
            "SELECT term_name, user_id, kind FROM glossary_term_experts_meta"
        )
    assert [(e["from_term"], e["to_term"], e["rel_type"]) for e in edges] == [
        ("party", "customer", "KIND_OF")
    ]
    assert [(x["term_name"], x["user_id"], x["kind"]) for x in experts] == [
        ("party", "dana", "author")
    ]


@pytest.mark.asyncio(loop_scope="session")
async def test_a_term_reaches_the_tags_on_the_columns_it_binds(tenant_db):
    """REQ-1585: term -> ref -> tag_assignments in one traversal, on the shared column_key."""
    async with tenant_db.acquire() as conn:
        await _setup(conn, {"orders": ["cust_id", "order_dt"]})
        table_id = (
            await conn.fetch("SELECT id FROM registered_tables WHERE table_name = 'orders'")
        )[0]["id"]
        # object_key is passed as a parameter, not built in SQL: a ':' inside a literal
        # would be read as a bind marker by the driver.
        await conn.execute(
            """
            INSERT INTO tag_assignments
                (tag_id, base_tag_id, object_type, source_id, table_id, column_name,
                 object_key, reason)
            VALUES
                ('pii', 'pii', 'column', 'pg1', $1, 'cust_id', $2, 'identity'),
                ('deprecated', 'deprecated', 'table', 'pg1', $1, NULL, $3, 'migrating')
            """,
            table_id,
            f"column:{table_id}:cust_id",
            f"table:{table_id}",
        )
        rows = await conn.fetch(
            """
            SELECT t.name AS term, a.tag_id, a.column_name
            FROM glossary_terms_meta t
            JOIN glossary_term_refs_meta r ON r.term_id = t.id
            JOIN tag_assignments_meta a ON a.column_key = r.column_key
            ORDER BY t.name, a.tag_id
            """
        )
        # The table-level assignment is registered, and its column_key is null.
        all_tags = await conn.fetch(
            "SELECT tag_id, column_key FROM tag_assignments_meta ORDER BY tag_id"
        )

    assert [(r["term"], r["tag_id"], r["column_name"]) for r in rows] == [
        ("customer", "pii", "cust_id")
    ]
    assert [(a["tag_id"], a["column_key"] is None) for a in all_tags] == [
        ("deprecated", True),
        ("pii", False),
    ]
