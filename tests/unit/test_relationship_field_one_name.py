# Copyright (c) 2026 Kenneth Stott
# Canary: f9757505-003f-4632-bb18-c0f474a50eda
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship's stored GraphQL field is the field the schema serves it under: one function
names both (``naming.relationship_field_name``), so for every relationship -- each shipped
configuration's, and generated ones over aliased, renamed and differently-cased tables -- the
``graphql_field`` stored on its row is a field of its source table's GraphQL type reaching its
target's type, in the schema built from the relationships' own declarations."""

# Requirements: REQ-194, REQ-351

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from graphql import get_named_type
from sqlalchemy import insert, select

from provisa.compiler import naming
from provisa.core.models import Relationship
from provisa.core.repositories import relationship as rel_repo
from provisa.core.schema_org import (
    domains,
    naming_rules,
    registered_tables,
    relationships,
    sources,
)

_REPO = Path(__file__).resolve().parents[2]

#: Every model the product ships, found on disk so a new demo is covered by being added.
_SHIPPED = sorted(
    str(path.relative_to(_REPO))
    for pattern in ("config/provisa*.yaml", "demo/named/*/config.yaml")
    for path in _REPO.glob(pattern)
)


def _shipped_model(path: str) -> dict[str, Any]:
    """The tables, relationships and naming of a shipped configuration, as its load reads them."""
    from provisa.core.config_loader import read_config_with_includes, views_as_tables

    from provisa.core.models import DERIVED_SOURCE_ID

    raw = views_as_tables(read_config_with_includes(_REPO / path))
    naming_cfg = raw.get("naming") or {}
    return {
        "sources": [
            {
                "id": s["id"],
                "type": s["type"],
                "gql_naming_convention": s.get("gql_naming_convention"),
            }
            for s in raw.get("sources", [])
        ]
        # The views' sentinel source, seeded on every boot on the engine's type (startup_seed).
        + [{"id": DERIVED_SOURCE_ID, "type": "duckdb", "gql_naming_convention": None}],
        "tables": [
            {
                "source_id": t["source_id"],
                "domain_id": t["domain_id"],
                "schema_name": t.get("schema") or "public",
                "table_name": t["table"],
                "alias": t.get("alias"),
                "gql_naming_convention": t.get("gql_naming_convention"),
            }
            for t in raw.get("tables", [])
            if "table" in t
        ],
        "relationships": raw.get("relationships", []),
        "naming_rules": [
            {"pattern": r["pattern"], "replacement": r["replace"]}
            for r in naming_cfg.get("rules", [])
        ],
        "convention": naming_cfg.get("convention"),
    }


#: Generated relationships: targets reached by an alias, by a renamed table (a naming rule), by a
#: verb-led OpenAPI operation, in their own convention, and a junction-backed edge.
_GENERATED: dict[str, Any] = {
    "sources": [
        {"id": "shop-db", "type": "postgresql", "gql_naming_convention": None},
        {"id": "api", "type": "openapi", "gql_naming_convention": "hasura_graphql"},
    ],
    "tables": [
        {
            "source_id": "shop-db",
            "domain_id": "shop",
            "schema_name": "shop",
            "table_name": name,
            "alias": alias,
            "gql_naming_convention": conv,
        }
        for name, alias, conv in (
            ("customers", None, None),
            ("tbl_orders", None, None),
            ("shop_access", "access_log", None),
            ("rma_cause_of_loss", "crop_losses", None),
            ("order_links", None, None),
            ("line_items", None, "hasura_graphql"),
        )
    ]
    + [
        {
            "source_id": "api",
            "domain_id": "shop",
            "schema_name": "api",
            "table_name": "get_pet_by_id",
            "alias": "pet_by_id",
            "gql_naming_convention": None,
        }
    ],
    "relationships": [
        {
            "id": "order-to-customer",
            "source_table_id": "tbl_orders",
            "target_table_id": "customers",
            "source_column": "customer_id",
            "target_column": "id",
            "cardinality": "many-to-one",
        },
        {
            "id": "customer-to-orders",
            "source_table_id": "customers",
            "target_table_id": "tbl_orders",
            "source_column": "id",
            "target_column": "customer_id",
            "cardinality": "one-to-many",
        },
        {
            "id": "customer-to-access",
            "source_table_id": "customers",
            "target_table_id": "access_log",
            "source_column": "id",
            "target_column": "customer_id",
            "cardinality": "one-to-many",
        },
        {
            "id": "order-to-loss",
            "source_table_id": "tbl_orders",
            "target_table_id": "crop_losses",
            "source_column": "loss_id",
            "target_column": "id",
            "cardinality": "many-to-one",
        },
        {
            "id": "order-to-pets",
            "source_table_id": "tbl_orders",
            "target_table_id": "pet_by_id",
            "source_column": "id",
            "target_column": "order_id",
            "cardinality": "one-to-many",
        },
        {
            "id": "order-to-line-items",
            "source_table_id": "tbl_orders",
            "target_table_id": "line_items",
            "source_column": "id",
            "target_column": "order_id",
            "cardinality": "one-to-many",
        },
        {
            "id": "order-replaces",
            "source_table_id": "tbl_orders",
            "target_table_id": "tbl_orders",
            "source_column": "id",
            "target_column": "id",
            "cardinality": "many-to-one",
            "via_table": "order_links",
            "via_source_column": "from_id",
            "via_target_column": "to_id",
            "via_type_column": "kind",
            "via_type_value": "replaces",
            "via_label_source": "column",
        },
    ],
    "naming_rules": [{"pattern": "^tbl_", "replacement": ""}],
    "convention": None,
}


async def _seed(conn, model: dict[str, Any]) -> None:
    for s in model["sources"]:
        await conn.execute_core(insert(sources).values(**s))
    for d in sorted({t["domain_id"] for t in model["tables"]}):
        await conn.execute_core(insert(domains).values(id=d))
    for t in model["tables"]:
        await conn.execute_core(insert(registered_tables).values(**t))
    for r in model["naming_rules"]:
        await conn.execute_core(insert(naming_rules).values(**r))
    for rel in model["relationships"]:
        await rel_repo.upsert(conn, Relationship.model_validate(rel))


def _served(
    tables: list[dict], rels: list[dict], model: dict[str, Any]
) -> tuple[dict[int, str], dict[tuple[str, str], int], dict[str, dict[str, str]]]:
    """What the schema built from the relationships as declared (no stored name given to the build)
    serves: each table id's type name, each (type, relationship field) join's target table id, and
    each type's fields with the named type each reaches."""
    from provisa.compiler.context import build_context
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput, generate_schema

    source_conv = {s["id"]: s["gql_naming_convention"] for s in model["sources"]}
    si = SchemaInput(
        tables=[
            {
                **t,
                "source_gql_naming_convention": source_conv[t["source_id"]],
                "columns": [{"column_name": "id", "visible_to": ["admin"]}],
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
            for t in tables
        ],
        relationships=rels,
        column_types={
            t["id"]: [ColumnMetadata(column_name="id", data_type="integer", is_nullable=False)]
            for t in tables
        },
        naming_rules=model["naming_rules"],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": d, "description": d} for d in sorted({t["domain_id"] for t in tables})],
        source_types={s["id"]: s["type"] for s in model["sources"]},
    )
    ctx = build_context(si)
    schema = generate_schema(si)
    type_of = {meta.table_id: meta.type_name for meta in ctx.tables.values()}
    joins = {key: join.target.table_id for key, join in ctx.joins.items()}
    fields: dict[str, dict[str, str]] = {}
    for type_name in set(type_of.values()):
        gql_type = schema.get_type(type_name)
        fields[type_name] = {
            name: get_named_type(f.type).name for name, f in getattr(gql_type, "fields", {}).items()
        }
    return type_of, joins, fields


async def _mismatches(plane, model: dict[str, Any]) -> list[str]:
    convention = model["convention"]
    if convention:
        naming.configure(gql=naming.preset_convention(convention))
    try:
        db = plane.org()
        async with db.acquire() as conn:
            await _seed(conn, model)
            tables = [
                dict(r._mapping)
                for r in (await conn.execute_core(select(registered_tables))).fetchall()
            ]
            rels = await rel_repo.list_all(conn)
            stored = {
                r.id: r.graphql_field
                for r in (
                    await conn.execute_core(
                        select(relationships.c.id, relationships.c.graphql_field)
                    )
                ).fetchall()
            }
        type_of, joins, fields = _served(tables, rels, model)
    finally:
        naming.configure()
    out: list[str] = []
    for rel in rels:
        if rel["target_function_name"]:
            continue  # a computed relationship reaches its function's table, stored under no name
        source = type_of.get(rel["source_table_id"])
        target = type_of.get(rel["target_table_id"])
        if source is None or target is None:
            continue
        field = stored[rel["id"]]
        joined = sorted(
            f for (t, f), tid in joins.items() if t == source and tid == rel["target_table_id"]
        )
        if joins.get((source, field)) != rel["target_table_id"]:
            out.append(f"{rel['id']}: stored {field!r}, joined as {joined} on {source}")
        # A junction edge is a Cypher relationship, not a GraphQL field (schema_gen).
        elif not rel["via_table_id"] and fields[source].get(field) != target:
            out.append(
                f"{rel['id']}: stored {field!r}, no such field reaching {target} on {source}"
            )
    return out


@pytest.mark.parametrize("config_file", _SHIPPED)
async def test_every_shipped_relationship_is_stored_under_its_served_field(
    control_plane, config_file
):
    mismatched = await _mismatches(control_plane, _shipped_model(config_file))
    assert not mismatched, "\n".join(mismatched)


async def test_generated_relationships_are_stored_under_their_served_fields(control_plane):
    mismatched = await _mismatches(control_plane, _GENERATED)
    assert not mismatched, "\n".join(mismatched)
