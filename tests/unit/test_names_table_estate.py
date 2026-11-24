# Copyright (c) 2026 Kenneth Stott
# Canary: 9d4b2e7a-3c61-4f08-b5a2-7e1c9f3d6a84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-194 (amended 2026-11-07, THE NAMES TABLE SPANS THE DATA ESTATE; ONLY ADMITTED OBJECTS; NOT
EVERY NAME HAS A SOURCE NAME; EVERY EXPORTED NAME, ONLY WHERE EXPORTED; A COLUMN'S PRIME GROUNDED
TERM): the stored names table records each object's name where it lives -- its source and each
engine it is read through or replicated to --, each export target's name only where the object is
exported, the glossary terms it realises, and a column's prime grounded term."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import delete, insert, select, text

from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.compiler import naming
from provisa.compiler.names_table import (
    Estate,
    NameRow,
    build_names_table,
    estate_tables,
    stored_name_rows,
)
from provisa.compiler.sql_types import TableMeta
from provisa.core.model_uri import command_uri, named_column_uri, named_table_uri, source_uri
from provisa.core.repositories.model_names import read_estate, write_names_table

ORG = "acme"


@pytest.fixture(autouse=True)
def _convention():
    naming.configure(gql="apollo_graphql", sql="snake")
    yield
    naming.configure(gql="apollo_graphql", sql="snake")


def _meta(tid: int, name: str, domain: str = "sales") -> TableMeta:
    return TableMeta(
        table_id=tid,
        field_name=name,
        type_name=name.capitalize(),
        source_id="crm",
        catalog_name="crm",
        schema_name="public",
        table_name=name,
        domain_id=domain,
    )


def _stored(tid: int, name: str, columns: list[str], **extra: Any) -> dict[str, Any]:
    return {
        "id": tid,
        "source_id": "crm",
        "schema_name": "public",
        "table_name": name,
        "domain_id": extra.pop("domain_id", "sales"),
        "alias": extra.pop("alias", None),
        "view_sql": extra.pop("view_sql", None),
        "columns": [{"column_name": c, "alias": None, "path": None} for c in columns],
    }


def _model():
    """orders (1) and customers (2) admitted from crm; a model view (3); a meta table (4); and a
    command."""
    metas = {
        "orders": _meta(1, "orders"),
        "customers": _meta(2, "customers"),
        "big_orders": _meta(3, "big_orders"),
        "tables": _meta(4, "tables", domain="meta"),
    }
    stored = [
        _stored(1, "orders", ["id", "amount"]),
        _stored(2, "customers", ["id"], alias="clients"),
        _stored(3, "big_orders", ["id"], view_sql="SELECT id FROM orders"),
        _stored(4, "tables", ["id"], domain_id="meta"),
    ]
    rows = []
    for tm in metas.values():
        rows.append(
            NameRow(
                "view" if tm.table_id == 3 else "table",
                str(tm.table_id),
                "sql",
                tm.table_name,
                tm.table_name,
                f"crm.public.{tm.table_name}",
                domain_id=tm.domain_id,
                table_id=tm.table_id,
            )
        )
    for t in stored:
        for c in t["columns"]:
            rows.append(
                NameRow(
                    "column",
                    f"{t['id']}:{c['column_name']}",
                    "sql",
                    c["column_name"],
                    c["column_name"],
                    f"crm.public.{t['table_name']}.{c['column_name']}",
                    domain_id=t["domain_id"],
                    table_id=t["id"],
                )
            )
    rows.append(NameRow("source", "crm", "catalog", "crm", "crm", "source crm"))
    rows.append(NameRow("command", "top", "graphql_field", "top", "top", "command top"))
    names = build_names_table(rows)
    return type(names)(
        rows=names.rows,
        estate=estate_tables(SimpleNamespace(tables=metas), stored),  # type: ignore[arg-type]
    )


def _estate(**over: Any) -> Estate:
    fields: dict[str, Any] = {
        "org_id": ORG,
        "engine": "duckdb",
        "replicas": {},
        "exports": {},
        "terms": {},
        "column_terms": {},
        "table_terms": {},
    }
    fields.update(over)
    return Estate(**fields)


def _by_object(rows: list[NameRow]) -> dict[tuple[str, str], dict[str, str]]:
    out: dict[tuple[str, str], dict[str, str]] = {}
    for r in rows:
        out.setdefault((r.object_kind, r.object_id), {})[r.surface] = r.name
    return out


def test_each_admitted_object_has_its_source_name_and_each_table_its_engine_addresses():
    rows = stored_name_rows(
        _model(),
        _estate(replicas={("crm", "public", "orders"): ("replicas_acme", "crm_public_orders")}),
    )
    named = _by_object(rows)
    assert named[("table", "1")]["source"] == "public.orders"
    assert named[("table", "1")]["engine:duckdb"] == "crm.public.orders"
    assert named[("table", "1")]["replica:duckdb"] == "replicas_acme.crm_public_orders"
    assert named[("column", "1:amount")]["source"] == "amount"
    # Not replicated: read through the engine only.
    assert named[("table", "2")]["engine:duckdb"] == "crm.public.customers"
    assert "replica:duckdb" not in named[("table", "2")]
    # A model-defined view and a meta table have no source name, and none is invented.
    assert "source" not in named[("view", "3")]
    assert "source" not in named[("column", "3:id")]
    assert "source" not in named[("table", "4")]
    assert "source" not in named[("column", "4:id")]
    # The meta table is still read through the engine.
    assert named[("table", "4")]["engine:duckdb"] == "crm.public.tables"


def test_an_export_target_names_only_what_it_carries():
    exports = {
        "atlas-1": {
            named_table_uri(ORG, "sales", "orders"): "crm.public.orders@prod",
            named_column_uri(
                ORG, "sales", "orders", "amount", None
            ): "crm.public.orders.amount@prod",
            source_uri(ORG, "crm"): "crm@prod",
        },
        # customers is exported under its alias, the name its URI carries.
        "horizon": {
            named_table_uri(ORG, "sales", "clients"): "PROVISA.SALES.CLIENTS",
            command_uri(ORG, "top"): "PROVISA.SALES.TOP",
        },
    }
    named = _by_object(stored_name_rows(_model(), _estate(exports=exports)))
    assert {s: n for s, n in named[("table", "1")].items() if s.startswith("export:")} == {
        "export:atlas-1": "crm.public.orders@prod"
    }
    assert named[("column", "1:amount")]["export:atlas-1"] == "crm.public.orders.amount@prod"
    assert named[("source", "crm")]["export:atlas-1"] == "crm@prod"
    assert {s: n for s, n in named[("table", "2")].items() if s.startswith("export:")} == {
        "export:horizon": "PROVISA.SALES.CLIENTS"
    }
    assert named[("command", "top")]["export:horizon"] == "PROVISA.SALES.TOP"
    # Not exported anywhere: no export name.
    for obj in (("column", "1:id"), ("view", "3"), ("table", "4")):
        assert not [s for s in named[obj] if s.startswith("export:")], obj


def test_realised_terms_and_a_columns_prime_grounded_term():
    estate = _estate(
        terms={7: "Order Amount", 8: "Customer", 9: "Order"},
        column_terms={(1, "amount"): 7},
        table_terms={2: 8, 1: 9},
    )
    rows = stored_name_rows(_model(), estate)
    named = _by_object(rows)
    assert named[("column", "1:amount")]["term:7"] == "Order Amount"
    assert named[("table", "2")]["term:8"] == "Customer"
    assert named[("table", "1")]["term:9"] == "Order"
    # The column the term owns carries it as its prime term, with its ontology name, on its rows.
    amount = [r for r in rows if (r.object_kind, r.object_id) == ("column", "1:amount")]
    assert {(r.prime_term, r.prime_term_ontology_name) for r in amount} == {
        ("Order Amount", "order_amount")
    }
    # A column no term owns has none; the table's Class term is not its columns' prime term.
    others = [r for r in rows if (r.object_kind, r.object_id) != ("column", "1:amount")]
    assert {(r.prime_term, r.prime_term_ontology_name) for r in others} == {(None, None)}


async def _stored_names(conn: Any) -> list[tuple]:
    from provisa.core.schema_org import model_names

    return [
        tuple(r)
        for r in (
            await conn.execute_core(
                select(*model_names.c).order_by(
                    model_names.c.object_kind, model_names.c.object_id, model_names.c.surface
                )
            )
        ).fetchall()
    ]


async def _seed(conn: Any) -> None:
    from provisa.core.schema_org import (
        catalog_bindings,
        domains,
        glossary_term_refs,
        glossary_terms,
        registered_tables,
        sources,
    )

    await conn.execute_core(insert(domains).values(id="sales", description="sales"))
    await conn.execute_core(insert(sources).values(id="crm", type="postgres"))
    await conn.execute_core(insert(glossary_terms).values(id=7, name="Order Amount"))
    await conn.execute_core(insert(glossary_terms).values(id=8, name="Customer"))
    await conn.execute_core(insert(glossary_terms).values(id=9, name="Gone", retired=True))
    for tid, name, term in ((1, "orders", None), (2, "customers", 8)):
        await conn.execute_core(
            insert(registered_tables).values(
                id=tid,
                source_id="crm",
                domain_id="sales",
                schema_name="public",
                table_name=name,
                class_term_id=term,
            )
        )
    await conn.execute_core(
        insert(glossary_term_refs).values(term_id=7, table_id=1, column_name="amount")
    )
    # A retired term's ref is no longer realised.
    await conn.execute_core(
        insert(glossary_term_refs).values(term_id=9, table_id=1, column_name="id")
    )
    await conn.execute_core(
        insert(catalog_bindings).values(
            target="atlas-1",
            semantic_uri=named_table_uri(ORG, "sales", "orders"),
            vendor_ref="guid-1",
            physical_key="crm.public.orders@prod",
        )
    )


@pytest.mark.asyncio
async def test_the_estate_is_read_from_the_control_plane_and_an_incremental_write_equals_a_rebuild(
    control_plane,
):
    from provisa.core.schema_org import catalog_bindings, glossary_term_refs, model_names

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _seed(conn)
            estate = await read_estate(conn, org_id=ORG, engine="duckdb", replicas={})
            before = stored_name_rows(_model(), estate)

            # The estate moves: orders is exported to a second target, amount loses its term.
            await conn.execute_core(
                insert(catalog_bindings).values(
                    target="horizon",
                    semantic_uri=named_table_uri(ORG, "sales", "orders"),
                    vendor_ref="v",
                    physical_key="PROVISA.SALES.ORDERS",
                )
            )
            await conn.execute_core(
                delete(glossary_term_refs).where(glossary_term_refs.c.term_id == 7)
            )
            moved = await read_estate(
                conn,
                org_id=ORG,
                engine="duckdb",
                replicas={("crm", "public", "orders"): ("replicas_acme", "crm_public_orders")},
            )
            after = stored_name_rows(_model(), moved)

            await write_names_table(conn, after)
            full = await _stored_names(conn)
            await conn.execute_core(delete(model_names))
            await write_names_table(conn, before)
            await write_names_table(conn, after)
            incremental = await _stored_names(conn)

            await conn.execute_core(text(_META_TABLE_VIEWS["model_names"]))
            await write_names_table(conn, before)
            meta = (
                await conn.execute_core(
                    text(
                        "SELECT object_id, surface, name, prime_term, prime_term_ontology_name "
                        "FROM model_names_meta WHERE object_kind = 'column' AND object_id = '1:amount' "
                        "ORDER BY surface"
                    )
                )
            ).fetchall()
    finally:
        await db.close()

    assert estate.terms == {7: "Order Amount", 8: "Customer"}
    assert estate.column_terms == {(1, "amount"): 7}
    assert estate.table_terms == {2: 8}
    assert estate.exports == {
        "atlas-1": {named_table_uri(ORG, "sales", "orders"): "crm.public.orders@prod"}
    }
    assert incremental == full
    assert len(full) == len(after)
    surfaces = {(r[1], r[2]) for r in full if r[0] == "table" and r[1] == "1"}
    assert {("1", "export:horizon"), ("1", "replica:duckdb")} <= surfaces
    assert [tuple(r) for r in meta] == [
        ("1:amount", "source", "amount", "Order Amount", "order_amount"),
        ("1:amount", "sql", "amount", "Order Amount", "order_amount"),
        ("1:amount", "term:7", "Order Amount", "Order Amount", "order_amount"),
    ]
