# Copyright (c) 2026 Kenneth Stott
# Canary: 59261064-2259-4c83-9a32-2d3187eb2990
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2126: data_product_edges, a data product's links to the rest of the model.

Relationship edges walk from a product's members through tables that belong to no product and stop
at another product's member; every path to one target is one row. Lineage edges are read from the
lineage edge table the model build writes (REQ-2129), here written by that build step itself. The
view runs on the real control plane.
"""

# Requirements: REQ-2126

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.compiler.stage2 import build_governance_context
from provisa.core.models import DERIVED_SOURCE_ID
from provisa.security.rights import META_DOMAIN_ID

# Table ids: P1's member A and its view V, P2's member B, C and E in no product, D read by V.
A, B, C, D, E, V = 1, 2, 3, 4, 5, 6


async def _seed(
    conn, *, dead_end: bool = False, domain: str = "sales", view_alias: str | None = None
) -> None:
    from sqlalchemy import insert

    from provisa.compiler.naming import domain_to_sql_name

    from provisa.core.schema_org import (
        data_products,
        domains,
        registered_tables,
        relationships,
        sources,
        table_columns,
    )

    await conn.execute_core(insert(domains).values(id=domain, description=domain))
    await conn.execute_core(insert(sources).values(id="crm", type="postgres"))
    await conn.execute_core(insert(sources).values(id=DERIVED_SOURCE_ID, type="postgres"))
    for pid in ("p1", "p2"):
        await conn.execute_core(insert(data_products).values(id=pid, domain_id=domain, name=pid))
    tables = [
        dict(id=A, table_name="a", product_id="p1"),
        dict(id=B, table_name="b", product_id="p2"),
        dict(id=C, table_name="c"),
        dict(id=D, table_name="d"),
        dict(id=E, table_name="e"),
        dict(
            id=V,
            table_name="v",
            product_id="p1",
            source_id=DERIVED_SOURCE_ID,
            alias=view_alias,
            view_sql=f"SELECT d.amount AS amount FROM {domain_to_sql_name(domain)}.d d",
        ),
    ]
    for t in tables:
        await conn.execute_core(
            insert(registered_tables).values(
                **{"source_id": "crm", "domain_id": domain, "schema_name": "public", **t}
            )
        )
    for table_id, column in ((D, "amount"), (V, "amount"), (A, "id"), (B, "id")):
        await conn.execute_core(
            insert(table_columns).values(
                table_id=table_id, domain_id=domain, column_name=column, data_type="varchar"
            )
        )
    links = [("a_b", A, B, "toB"), ("a_c", A, C, None), ("c_b", C, B, "viaC")]
    if dead_end:
        links.append(("a_e", A, E, "toE"))
    for rid, src, tgt, alias in links:
        await conn.execute_core(
            insert(relationships).values(
                id=rid,
                source_table_id=src,
                target_table_id=tgt,
                source_column="id",
                target_column="id",
                cardinality="many-to-one",
                alias=alias,
            )
        )


async def _edges(conn, product_id: str) -> list[dict]:
    from sqlalchemy import text

    from provisa.api.admin.lineage_router import write_lineage_edges
    from provisa.api.startup_seed import _adapt_view_ddl

    # The model build's own step: this model's lineage, written to the edge table.
    await write_lineage_edges(conn, SimpleNamespace(tracked_functions={}))
    await conn.execute(_adapt_view_ddl(_META_TABLE_VIEWS["data_product_edges"]))
    rows = await conn.execute_core(
        text("SELECT * FROM data_product_edges WHERE product_id = :p ORDER BY kind, id").bindparams(
            p=product_id
        )
    )
    return [dict(r._mapping) for r in rows.fetchall()]


@pytest.mark.asyncio
async def test_two_paths_to_another_product_are_one_direct_edge(control_plane):
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _seed(conn)
            rows = await _edges(conn, "p1")
    finally:
        await db.close()

    relationship = [r for r in rows if r["kind"] == "relationship"]
    lineage = [r for r in rows if r["kind"] == "lineage"]

    # One edge P1 -> P2, direct, both paths counted on it; C only supports the indirect one.
    assert len(relationship) == 1
    (edge,) = relationship
    assert (edge["target_product_id"], edge["target_table_id"]) == ("p2", None)
    assert (edge["direct"], edge["hops"], edge["path_count"]) == (True, 1, 2)
    assert edge["aliases"] == "toB, viaC"
    assert edge["relationship_ids"] == "a_b, a_c, c_b"
    assert edge["supporting_tables"] == "sales.c"

    # One lineage edge: D, which P1's view reads, upstream of P1.
    assert [
        (r["direction"], r["target_relation"], r["target_table_id"], r["hops"], r["vias"])
        for r in lineage
    ] == [("upstream", "sales.d", D, 1, "sales.v")]


@pytest.mark.asyncio
async def test_every_table_a_row_names_is_named_as_the_model_names_it(control_plane):
    """A view a lineage edge goes through and a supporting table are named by their domain and
    their alias (or name), as the Data Products panel names every table -- not by the SQL form the
    lineage records (sales_ops.v)."""
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _seed(conn, domain="sales-ops", view_alias="revenue")
            rows = await _edges(conn, "p1")
    finally:
        await db.close()

    (edge,) = [r for r in rows if r["kind"] == "relationship"]
    assert edge["supporting_tables"] == "sales-ops.c"
    (lineage,) = [r for r in rows if r["kind"] == "lineage"]
    assert (lineage["target_relation"], lineage["target_table_id"], lineage["vias"]) == (
        "sales_ops.d",
        D,
        "sales-ops.revenue",
    )


@pytest.mark.asyncio
async def test_a_table_in_no_product_is_a_target_only_as_a_dead_end(control_plane):
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _seed(conn, dead_end=True)
            rows = await _edges(conn, "p1")
    finally:
        await db.close()

    tables = {
        r["target_table_id"]: (r["direct"], r["path_count"])
        for r in rows
        if r["kind"] == "relationship" and r["target_table_id"] is not None
    }
    # E leads nowhere: it is P1's related table. C leads on to P2: it is not.
    assert tables == {E: (True, 1)}


def _edges_ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables["data_product_edges"] = TableMeta(
        table_id=200,
        field_name="data_product_edges",
        type_name="DataProductEdges",
        source_id="provisa-admin",
        catalog_name="main",
        schema_name="public",
        table_name="data_product_edges",
        domain_id=META_DOMAIN_ID,
    )
    return ctx


def _gov(capabilities: list[str]):
    role = {"id": "analyst", "capabilities": capabilities, "domain_access": [META_DOMAIN_ID]}
    tables = [{"id": 200, "columns": [{"column_name": "kind", "data_type": "varchar"}]}]
    return build_governance_context(
        "analyst", SimpleNamespace(rules={}), {}, _edges_ctx(), tables, role
    )


def test_lineage_edges_need_view_governance():
    """The relationship rows are discovery; the lineage rows are read with view_governance."""
    assert _gov([]).rls_rules[200] == "kind <> 'lineage'"
    assert 200 not in _gov(["view_governance"]).rls_rules


@pytest.mark.asyncio
async def test_the_cost_grows_with_the_model_not_with_the_routes(control_plane):
    """60 tables and 120 relationships between two products, and a lineage graph of 60 relations
    and 120 derivations into a member: enumerating routes took over a minute here; reachability
    answers in a fraction of the statement timeout."""
    import random
    import time

    from sqlalchemy import insert, text

    from provisa.api.startup_seed import _adapt_view_ddl
    from provisa.core.schema_org import (
        data_products,
        domains,
        lineage_edges,
        registered_tables,
        relationships,
        sources,
    )

    rnd = random.Random(7)
    tables, rels = 60, 120
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await conn.execute_core(insert(domains).values(id="s", description="s"))
            await conn.execute_core(insert(sources).values(id="x", type="postgres"))
            for pid in ("p1", "p2"):
                await conn.execute_core(
                    insert(data_products).values(id=pid, domain_id="s", name=pid)
                )
            await conn.execute_core(
                insert(registered_tables).values(
                    [
                        dict(
                            id=i,
                            source_id="x",
                            domain_id="s",
                            schema_name="public",
                            table_name=f"t{i}",
                            product_id={1: "p1", 2: "p2"}.get(i),
                        )
                        for i in range(1, tables + 1)
                    ]
                )
            )
            pairs = set()
            while len(pairs) < rels:
                a, b = rnd.sample(range(1, tables + 1), 2)
                pairs.add((a, b))
            await conn.execute_core(
                insert(relationships).values(
                    [
                        dict(
                            id=f"r{k}",
                            source_table_id=a,
                            target_table_id=b,
                            source_column="id",
                            target_column="id",
                            cardinality="many-to-one",
                        )
                        for k, (a, b) in enumerate(sorted(pairs))
                    ]
                )
            )
            # A layered derivation graph whose routes into t1 multiply at every layer.
            derivations = set()
            while len(derivations) < rels:
                a, b = sorted(rnd.sample(range(1, tables + 1), 2), reverse=True)
                derivations.add((a, b))
            await conn.execute_core(
                insert(lineage_edges).values(
                    [
                        dict(
                            source_relation=f"s.t{a}",
                            source_column="id",
                            target_relation=f"s.t{b}",
                            target_column="id",
                            via=f"s.t{b}",
                            via_kind="view",
                            source_table_id=a,
                            source_column_name="id",
                            target_table_id=b,
                            target_column_name="id",
                        )
                        for a, b in sorted(derivations)
                    ]
                )
            )
            await conn.execute(_adapt_view_ddl(_META_TABLE_VIEWS["data_product_edges"]))
            await conn.execute("SET statement_timeout = '60s'")
            started = time.perf_counter()
            rows = (
                await conn.execute_core(
                    text("SELECT kind, COUNT(*) FROM data_product_edges GROUP BY kind")
                )
            ).fetchall()
            took = time.perf_counter() - started
    finally:
        await db.close()

    counts = dict(rows)
    assert counts.get("relationship") and counts.get("lineage"), counts
    assert took < 10, (
        f"data_product_edges took {took:.2f}s over {tables} tables, {rels} relationships"
    )
