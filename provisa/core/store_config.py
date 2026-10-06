# Copyright (c) 2026 Kenneth Stott
# Canary: 5c0e7a52-7f43-4b8e-9a51-1d3f2e6b9c84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The model the store holds, written as a configuration (REQ-1919).

A configuration file seeds the model store once; from then on the store alone owns the model.
Everything that needs the model in the shape of a configuration reads it from here: the running
process (its ``state.config`` is the file's settings with every model section taken from the
store), and the export an admin writes from the UI. Each section is the store's rows projected
onto the configuration's own models, as written — a credential stays the reference the store
holds — so applying the export to an empty store builds the same model.

What the deployment seeds into every store (the built-in sources and their tables, the demo's
GraphQL source, the system domains, the two reserved administrative roles) is not part of it:
a store has it before any configuration is applied.
"""

# Requirements: REQ-1919, REQ-164, REQ-1096

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel
from sqlalchemy import select

from provisa.core import domain_policy
from provisa.core.models import (
    DataProduct,
    Domain,
    Function,
    GlossaryTermConfig,
    GlossaryTermEdgeConfig,
    Metric,
    Relationship,
    RLSRule,
    Role,
    ScheduledTrigger,
    Table,
    Tag,
    TagAssignment,
    Webhook,
)
from provisa.core.schema_org import (
    glossary_term_domains,
    glossary_term_edges,
    glossary_terms,
    naming_rules,
)
from provisa.security.rights import ORG_ADMIN_ROLE, PLATFORM_ADMIN_ROLE

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: Every section of a configuration the model store holds. ``naming.rules`` is held too, inside
#: the ``naming`` block whose other keys are settings.
MODEL_SECTIONS: tuple[str, ...] = (
    "stores",
    "regions",
    "sources",
    "domains",
    "roles",
    "data_products",
    "glossary_terms",
    "tables",
    "relationships",
    "metrics",
    "tags",
    "tag_assignments",
    "rls_rules",
    "functions",
    "webhooks",
    "scheduled_triggers",
    "kafka_sources",
)

_RESERVED_ROLES = frozenset({ORG_ADMIN_ROLE, PLATFORM_ADMIN_ROLE})


def _written(model: BaseModel) -> dict[str, Any]:
    """A model as a configuration writes it: by its config names, defaults left out."""
    return model.model_dump(mode="json", by_alias=True, exclude_defaults=True)


def _fields(model: type[BaseModel], row: dict[str, Any]) -> dict[str, Any]:
    """The row's values for the model's fields; a NULL is the field's own default."""
    return {k: v for k, v in row.items() if k in model.model_fields and v is not None}


def _seeded_domains() -> frozenset[str]:
    from provisa.core.db import SEEDED_DOMAIN_IDS

    return SEEDED_DOMAIN_IDS | frozenset(domain_policy.system_domain_ids())


async def _sources(conn: "Connection") -> list[dict[str, Any]]:
    from provisa.core.db import SEEDED_SOURCE_IDS
    from provisa.core.repositories import source as source_repo

    return [
        _written(source_repo.source_from_row(row))
        for row in await source_repo.list_all(conn)
        if row["id"] not in SEEDED_SOURCE_IDS
    ]


async def _stores_and_regions(conn: "Connection") -> tuple[list[dict], list[dict]]:
    from provisa.core.repositories import region as region_repo

    stores = [_written(s) for s in await region_repo.list_stores(conn)]
    regions = [_written(r) for r in await region_repo.list_regions(conn)]
    return stores, regions


async def _domains(conn: "Connection") -> list[dict[str, Any]]:
    from provisa.core.repositories import domain as domain_repo

    seeded = _seeded_domains()
    return [
        _written(Domain.model_validate(_fields(Domain, row)))
        for row in await domain_repo.list_all(conn)
        if row["id"] not in seeded
    ]


async def _roles(conn: "Connection") -> list[dict[str, Any]]:
    from provisa.core.repositories import role as role_repo

    out = []
    for row in await role_repo.list_all(conn):
        if row["id"] in _RESERVED_ROLES:
            continue
        written = _written(Role.model_validate(_fields(Role, row)))
        # A role always states its domains (ProvisaConfig refuses one that does not).
        written["domain_access"] = list(row["domain_access"] or [])
        written["capabilities"] = list(row["capabilities"] or [])
        out.append(written)
    return out


def _internal_table(row: dict[str, Any]) -> bool:
    """A table the deployment seeds: on a built-in source or the demo's, or in the meta/ops
    domains. A view is on the derived-view source and is the model's like any table."""
    from provisa.core.db import SEEDED_SOURCE_IDS
    from provisa.core.models import DERIVED_SOURCE_ID

    seeded = SEEDED_SOURCE_IDS - {DERIVED_SOURCE_ID}
    return row["source_id"] in seeded or row.get("domain_id") in ("meta", "ops")


def _table(row: dict[str, Any]) -> dict[str, Any]:
    fields = _fields(Table, row)
    fields["columns"] = [{**c, "name": c["column_name"]} for c in row["columns"]]
    fields["columns"] = [
        {k: v for k, v in c.items() if v is not None and k != "column_name"}
        for c in fields["columns"]
    ]
    table = Table.model_validate(fields)
    written = _written(table)
    # A table's identity is written out whole, defaults included: readers of a configuration key
    # tables by (source, schema, table).
    written["schema"] = table.schema_name
    written["domain_id"] = table.domain_id
    return written


async def _glossary_terms(conn: "Connection") -> list[dict[str, Any]]:
    """The terms declared with domains of their own — the ones a configuration can state."""
    names = {
        r.id: r.name
        for r in (
            await conn.execute_core(select(glossary_terms.c.id, glossary_terms.c.name))
        ).fetchall()
    }
    declared: dict[int, list[str]] = {}
    for r in (
        await conn.execute_core(
            select(glossary_term_domains.c.term_id, glossary_term_domains.c.domain_id).order_by(
                glossary_term_domains.c.domain_id
            )
        )
    ).fetchall():
        declared.setdefault(r.term_id, []).append(r.domain_id)
    edges: dict[int, list[GlossaryTermEdgeConfig]] = {}
    for r in (
        await conn.execute_core(
            select(
                glossary_term_edges.c.from_term_id,
                glossary_term_edges.c.to_term_id,
                glossary_term_edges.c.rel_type,
            ).order_by(glossary_term_edges.c.id)
        )
    ).fetchall():
        edges.setdefault(r.from_term_id, []).append(
            GlossaryTermEdgeConfig(to=names[r.to_term_id], rel_type=r.rel_type)
        )
    rows = (
        await conn.execute_core(
            select(glossary_terms.c.id, glossary_terms.c.name, glossary_terms.c.definition)
            .where(glossary_terms.c.id.in_(sorted(declared)))
            .order_by(glossary_terms.c.name)
        )
    ).fetchall()
    return [
        _written(
            GlossaryTermConfig(
                name=r.name,
                definition=r.definition,
                domains=declared[r.id],
                edges=edges.get(r.id, []),
            )
        )
        for r in rows
    ]


async def _naming_rules(conn: "Connection") -> list[dict[str, str]]:
    rows = (
        await conn.execute_core(
            select(naming_rules.c.pattern, naming_rules.c.replacement).order_by(naming_rules.c.id)
        )
    ).fetchall()
    return [{"pattern": r.pattern, "replace": r.replacement} for r in rows]


async def store_model(conn: "Connection") -> dict[str, Any]:
    """Every model section the store holds, as a configuration writes it, and the naming rules
    (``naming_rules``). Sections are complete lists; an empty one is an empty list."""
    from provisa.core.db import SEEDED_SOURCE_IDS
    from provisa.core.models import DERIVED_TAG_IDS, SYSTEM_TAG_IDS
    from provisa.core.repositories import data_product as data_product_repo
    from provisa.core.repositories import function as function_repo
    from provisa.core.repositories import kafka_source as kafka_repo
    from provisa.core.repositories import metric as metric_repo
    from provisa.core.repositories import relationship as rel_repo
    from provisa.core.repositories import rls as rls_repo
    from provisa.core.repositories import scheduled_trigger as trigger_repo
    from provisa.core.repositories import tag as tag_repo
    from provisa.core.repositories import table as table_repo

    tables = await table_repo.list_all(conn)
    internal = {int(t["id"]) for t in tables if _internal_table(t)}
    # A table is named in a configuration by its virtual name: its alias when it has one.
    name_of = {int(t["id"]): str(t.get("alias") or t["table_name"]) for t in tables}

    def _refs_internal(row: dict[str, Any]) -> bool:
        ids = [row.get(k) for k in ("source_table_id", "target_table_id", "table_id")]
        return any(i is not None and int(i) in internal for i in ids)

    relationships = []
    for row in await rel_repo.list_all(conn):
        if _refs_internal(row):
            continue
        fields = _fields(Relationship, row)
        fields["source_table_id"] = name_of[int(row["source_table_id"])]
        if row.get("target_table_id") is not None:
            fields["target_table_id"] = name_of[int(row["target_table_id"])]
        if row.get("via_table_id") is not None:
            fields["via_table"] = name_of[int(row["via_table_id"])]
        relationships.append(_written(Relationship.model_validate(fields)))

    rls_rules = []
    for row in await rls_repo.list_all(conn):
        if _refs_internal(row) or row.get("domain_id") in ("meta", "ops"):
            continue
        rule = {
            "role_id": row["role_id"],
            "filter": row["filter_expr"],
            "table_id": None if row["table_id"] is None else name_of[int(row["table_id"])],
            "domain_id": row["domain_id"],
            "action_name": row["action_name"],
        }
        rls_rules.append(_written(RLSRule.model_validate(rule)))

    tag_assignments = []
    for row in await tag_repo.list_assignments(conn):
        if row.get("table_id") is not None and int(row["table_id"]) in internal:
            continue
        fields = _fields(TagAssignment, row)
        fields.pop("table_id", None)  # a serial: the configuration names the table by table_ref
        tag_assignments.append(_written(TagAssignment.model_validate(fields)))

    stores, regions = await _stores_and_regions(conn)
    return {
        "stores": stores,
        "regions": regions,
        "sources": await _sources(conn),
        "domains": await _domains(conn),
        "roles": await _roles(conn),
        "data_products": [
            _written(DataProduct.model_validate(_fields(DataProduct, row)))
            for row in await data_product_repo.list_all(conn)
            if row["domain_id"] not in ("meta", "ops")
        ],
        "glossary_terms": await _glossary_terms(conn),
        "tables": [_table(t) for t in tables if int(t["id"]) not in internal],
        "relationships": relationships,
        "metrics": [
            # A fact-derived metric is a part of its fact table: the table brings it.
            _written(Metric.model_validate(_fields(Metric, row)))
            for row in await metric_repo.list_all(conn)
            if row.get("from_fact") is None
        ],
        "tags": [
            # The system and derived tags are code-defined, in every store.
            _written(Tag.model_validate(_fields(Tag, row)))
            for row in await tag_repo.list_all(conn)
            if row["id"] not in SYSTEM_TAG_IDS + DERIVED_TAG_IDS
        ],
        "tag_assignments": tag_assignments,
        "rls_rules": rls_rules,
        "functions": [
            _written(Function.model_validate(_fields(Function, row)))
            for row in await function_repo.list_functions(conn)
            if row["source_id"] not in SEEDED_SOURCE_IDS
        ],
        "webhooks": [
            _written(Webhook.model_validate(_fields(Webhook, row)))
            for row in await function_repo.list_webhooks(conn)
        ],
        "scheduled_triggers": [
            _written(ScheduledTrigger.model_validate(_fields(ScheduledTrigger, row)))
            for row in await trigger_repo.list_all(conn)
        ],
        "kafka_sources": await kafka_repo.list_specs(conn),  # REQ-147
        "naming_rules": await _naming_rules(conn),
    }


def with_store_model(raw: dict[str, Any], model: dict[str, Any]) -> dict[str, Any]:
    """The configuration ``raw`` (the deployment's file, as written) with every model section
    replaced by the store's. The file's settings — server, engine, auth and the rest — stay."""
    # A ``views:`` block declares tables of the derived source (config_loader.views_as_tables);
    # the store holds them as tables, so the file's are not carried over.
    out = {k: v for k, v in raw.items() if k not in MODEL_SECTIONS and k != "views"}
    for section in MODEL_SECTIONS:
        out[section] = model[section]
    out["naming"] = {**(raw.get("naming") or {}), "rules": model["naming_rules"]}
    return out


def only_sections(cfg: dict[str, Any], sections: list[str]) -> dict[str, Any]:
    """The part of a configuration an admin chose: the named model sections, and the minimum a
    configuration states (its required lists, empty when not chosen). Unknown names are refused."""
    unknown = sorted(set(sections) - set(MODEL_SECTIONS))
    if unknown:
        raise ValueError(f"not a model section: {', '.join(unknown)}; one of {MODEL_SECTIONS}")
    out: dict[str, Any] = {section: cfg[section] for section in sections}
    for required in ("sources", "domains", "tables", "roles"):
        out.setdefault(required, [])
    return out
