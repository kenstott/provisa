# Copyright (c) 2026 Kenneth Stott
# Canary: d2c3bbc4-24a6-4855-8d30-32fd753a827e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2125: data products in the meta domain.

data_products_meta holds one row per product and only its own fields; every section of the
product -- its domain, member tables (checkers told apart by dq_contract, each reaching the
table its contract scans), commands, tags,
glossary terms and profile results -- is reached from that row over the seeded meta
relationships alone. The views run on the real control plane, and each hop joins on the columns
the seeded relationship row names, so what is tested is the edge a GraphQL, Cypher or SQL client
follows.
"""

# Requirements: REQ-2125

from __future__ import annotations

import pytest

from provisa.api._meta_seed import META_RELATIONSHIPS
from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.api.app_loaders import _META_TABLE_ALIAS, _META_TABLES
from provisa.dq.registration import write_checker_targets
from provisa.security.rights import GOVERNANCE_META_COLUMNS

# The fields REQ-2125 lists, in order: nothing flattened, no arrays, no counts.
PRODUCT_FIELDS = [
    "id",
    "domain_id",
    "name",
    "owner_role",
    "team_role",
    "purpose",
    "limitations",
    "usage",
    "version",
    "status",
    "status_reason",
    "retire_on",
    "successor_id",
    "support",
    "support_contact",
    "custom_properties",
]

_VIEWS = (
    "data_products",
    "registered_tables",
    "table_columns",
    "tracked_functions",
    "tag_assignments",
    "glossary_term_domains",  # glossary_term_refs_meta reads each term's domains from it
    "glossary_term_refs",
)


def _relation(name: str) -> str:
    """The relation a meta table is read from: its curated view, else the table itself."""
    return _META_TABLE_ALIAS.get(name, name)


def _edge(rel_id: str) -> tuple[str, str, str, str]:
    """(source table, source column, target column, target table) of a seeded meta edge."""
    for row in META_RELATIONSHIPS:
        if row[0] == rel_id:
            return row[2], row[3], row[4], row[7]
    raise AssertionError(f"no seeded meta relationship {rel_id!r}")


async def _follow(conn, rel_id: str, keys: list, column: str = "id") -> list[dict]:
    """The rows of an edge's target reached from source rows whose join column is in ``keys``:
    the join the seeded relationship row declares, nothing else."""
    from sqlalchemy import bindparam, text

    src, src_col, tgt_col, tgt = _edge(rel_id)
    rows = await conn.execute_core(
        text(
            f"SELECT t.* FROM {_relation(src)} s JOIN {_relation(tgt)} t "
            f"ON t.{tgt_col} = s.{src_col} WHERE s.{column} IN :keys"
        ).bindparams(bindparam("keys", value=keys, expanding=True))
    )
    return [dict(r._mapping) for r in rows.fetchall()]


async def _seed(conn) -> None:
    """customer_360 in sales: a member table, a member checker table with a dq_contract, a
    member command, a tag on the product, a term bound to a member column, and the member table
    profiled with a result table registered on its profiler source. Plus a table that is in no
    product and a draft member, which are reached by nothing."""
    from sqlalchemy import insert

    from provisa.core.schema_org import (
        data_products,
        domains,
        glossary_term_refs,
        glossary_terms,
        registered_tables,
        sources,
        table_columns,
        tag_assignments,
        tracked_functions,
    )

    await conn.execute_core(insert(domains).values(id="sales", description="sales"))
    for sid, kind in (("crm", "postgres"), ("dq", "soda"), ("prof", "data_profiler")):
        await conn.execute_core(insert(sources).values(id=sid, type=kind))
    await conn.execute_core(
        insert(data_products).values(
            id="customer_360",
            domain_id="sales",
            name="Customer 360",
            owner_role="sales_lead",
            purpose="One view of each customer",
            status="live",
            custom_properties={"tier": "gold"},
        )
    )
    tables = [
        dict(
            id=1,
            source_id="crm",
            table_name="customers",
            product_id="customer_360",
            profiler_source_id="prof",
        ),
        dict(
            id=2,
            source_id="dq",
            table_name="customers_checks",
            product_id="customer_360",
            dq_contract="checks for customers:\n  - row_count > 0",
        ),
        dict(id=3, source_id="crm", table_name="regions"),
        dict(id=4, source_id="prof", table_name="customers_1_profile_runs"),
        dict(
            id=5, source_id="crm", table_name="draft_member", product_id="customer_360", draft=True
        ),
    ]
    for t in tables:
        await conn.execute_core(
            insert(registered_tables).values(domain_id="sales", schema_name="public", **t)
        )
    for column in ("id", "email"):
        await conn.execute_core(
            insert(table_columns).values(
                table_id=1, domain_id="sales", column_name=column, data_type="varchar"
            )
        )
    await conn.execute_core(
        insert(tracked_functions).values(
            name="refresh_customer", domain_id="sales", product_id="customer_360"
        )
    )
    await conn.execute_core(
        insert(tag_assignments).values(
            tag_id="certified",
            base_tag_id="certified",
            object_type="product",
            product_id="customer_360",
            object_key="product:customer_360",
        )
    )
    await conn.execute_core(insert(glossary_terms).values(id=10, name="Customer email"))
    await conn.execute_core(
        insert(glossary_term_refs).values(term_id=10, table_id=1, column_name="email")
    )


@pytest.mark.asyncio
async def test_a_product_and_every_part_of_it_are_reached_by_relationships(control_plane):
    from sqlalchemy import text

    from provisa.api.startup_seed import _adapt_view_ddl

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _seed(conn)
            for name in _VIEWS:
                await conn.execute(_adapt_view_ddl(_META_TABLE_VIEWS[name]))
            # The model build's own step: the checker's contract scans customers (id 1).
            await write_checker_targets(conn, {2: 1})

            # The product's own fields, and nothing else.
            rows = (await conn.execute_core(text("SELECT * FROM data_products_meta"))).fetchall()
            assert [list(r._mapping) for r in rows] == [PRODUCT_FIELDS]
            product = dict(rows[0]._mapping)
            assert (product["name"], product["status"]) == ("Customer 360", "live")
            assert product["custom_properties"] == {"tier": "gold"}
            key = [product["id"]]

            # Its domain.
            domain = await _follow(conn, "meta:data_products:domains", key)
            assert [d["id"] for d in domain] == ["sales"]

            # Both member tables, the checker told apart by its contract; the draft member is in
            # no catalog and the table in no product is reached by nothing.
            members = await _follow(conn, "meta:data_products:registered_tables", key)
            assert {m["table_name"]: m["dq_contract"] is not None for m in members} == {
                "customers": False,
                "customers_checks": True,
            }
            member_ids = [m["id"] for m in members]

            # The checker reaches the table it checks, and that table its checker.
            checker = [m for m in members if m["dq_contract"] is not None]
            assert [c["checked_table_id"] for c in checker] == [1]
            checks = await _follow(
                conn, "meta:registered_tables:checks", [c["id"] for c in checker]
            )
            assert [t["table_name"] for t in checks] == ["customers"]
            checked_by = await _follow(conn, "meta:registered_tables:checked_by", [1])
            assert [t["table_name"] for t in checked_by] == ["customers_checks"]

            # Back from a member to its product.
            back = await _follow(conn, "meta:registered_tables:data_products", member_ids)
            assert {b["id"] for b in back} == {"customer_360"}

            # The command and the tag.
            commands = await _follow(conn, "meta:data_products:tracked_functions", key)
            assert [c["name"] for c in commands] == ["refresh_customer"]
            tags = await _follow(conn, "meta:data_products:tag_assignments", key)
            assert [(t["tag_id"], t["object_type"]) for t in tags] == [("certified", "product")]

            # The term: member table -> its columns -> the refs bound to them -> the term.
            columns = await _follow(conn, "meta:registered_tables:table_columns", member_ids)
            refs = await _follow(
                conn,
                "meta:table_columns:glossary_term_refs",
                [c["column_key"] for c in columns],
                column="column_key",
            )
            assert [(r["term_id"], r["column_name"]) for r in refs] == [(10, "email")]

            # The profile results registered for the profiled member on its profiler source.
            results = await _follow(conn, "meta:registered_tables:profile_results", member_ids)
            assert [(r["table_name"], r["profiled_table_id"]) for r in results] == [
                ("customers_1_profile_runs", 1)
            ]
            profiled = await _follow(
                conn, "meta:registered_tables:profiled_table", [r["id"] for r in results]
            )
            assert [p["table_name"] for p in profiled] == ["customers"]
    finally:
        await db.close()


def test_the_product_view_is_a_meta_table_whose_relationships_are_all_seeded():
    """Every edge REQ-2125 names is seeded between registered meta tables, both ways, so a
    traversal reaches each section and comes back."""
    assert "data_products" in _META_TABLES
    for rel_id in (
        "meta:data_products:domains",
        "meta:domains:data_products",
        "meta:data_products:registered_tables",
        "meta:registered_tables:data_products",
        "meta:data_products:tracked_functions",
        "meta:tracked_functions:data_products",
        "meta:data_products:tag_assignments",
        "meta:tag_assignments:data_products",
        "meta:registered_tables:profile_results",
        "meta:registered_tables:profiled_table",
        "meta:registered_tables:checks",
        "meta:registered_tables:checked_by",
    ):
        src, *_, tgt = _edge(rel_id)
        assert {src, tgt} <= set(_META_TABLES), rel_id


def test_each_view_keeps_its_own_tiers():
    """A product's fields and the parts REQ-2125 adds are discovery (CORE) columns; what a
    member's registration hides from a caller without view_governance stays hidden however the
    member is reached, because the tier is the target view's own."""
    assert not set(PRODUCT_FIELDS) & GOVERNANCE_META_COLUMNS
    assert (
        not {
            "dq_contract",
            "checked_table_id",
            "profiled_table_id",
            "product_id",
        }
        & GOVERNANCE_META_COLUMNS
    )
    assert "view_sql" in GOVERNANCE_META_COLUMNS  # registered_tables_meta's governance column


@pytest.mark.asyncio
async def test_each_build_replaces_the_checker_targets(control_plane):
    """The table is the last build's resolution and nothing else: a contract that scans another
    table, or a checker that is gone, leaves no row of the build before."""
    from sqlalchemy import text

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await write_checker_targets(conn, {2: 1, 5: 3})
            await write_checker_targets(conn, {2: 4})
            rows = (
                await conn.execute_core(
                    text("SELECT table_id, checked_table_id FROM checker_targets")
                )
            ).fetchall()
    finally:
        await db.close()
    assert [tuple(r) for r in rows] == [(2, 4)]
