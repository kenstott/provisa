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

import json
import re
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel
from sqlalchemy import select

from provisa.core import domain_policy
from provisa.core.models import (
    CalendarConfig,
    ConnectorConfig,
    DataProduct,
    DeclaredOffer,
    Domain,
    Function,
    GlossaryTermConfig,
    GlossaryTermDeprecation,
    GlossaryTermRetirement,
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
    registered_tables,
)

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
    "calendars",
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
    "connectors",  # REQ-2221
)


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

    from provisa.core.db import STANDARD_ROLES

    out = []
    for row in await role_repo.list_all(conn):
        # REQ-2155: a standard role is Provisa's, never a config file's (role_repo refuses one).
        if row["id"] in STANDARD_ROLES:
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
    return row["source_id"] in seeded or row.get("domain_id") in ("meta", "ops", "ontology")


async def seeded_table_names(conn: "Connection") -> frozenset[str]:  # REQ-1634
    """Every name that identifies a seeded table: its virtual name (its alias, else its table
    name) and ``source.schema.table``, the two ways a data product names a member."""
    from provisa.core.repositories import table as table_repo

    return frozenset(
        name
        for row in await table_repo.list_all(conn, parts_of=lambda _row: False)
        if _internal_table(row)
        for name in (
            str(row.get("alias") or row["table_name"]),
            f"{row['source_id']}.{row['schema_name']}.{row['table_name']}",
        )
    )


_PROFILE_RESULT = re.compile(r"(.+)_(\d+)_profile_(\w+)")


def _profile_result_of(
    row: dict[str, Any], by_id: dict[int, dict[str, Any]]
) -> tuple[str, str] | None:
    """(member, kind) of a profiler's result table (REQ-2052). Its name carries the member's stored
    id, which another store does not share, so a configuration names it by its member."""
    match = _PROFILE_RESULT.fullmatch(row["table_name"])
    if match is None:
        return None
    member = by_id.get(int(match[2]))
    if member is None or member.get("profiler_source_id") != row["source_id"]:
        return None
    return str(member["table_name"]), match[3]


def _table(row: dict[str, Any], class_term: str | None) -> dict[str, Any]:
    fields = _fields(Table, {**row, "class_term": class_term})
    if fields.get("view_metrics") is not None:
        # REQ-1318: registration stores a metrics view's declaration and the SQL it compiles to;
        # a configuration states the declaration alone, and loading it compiles the SQL again.
        fields.pop("view_sql", None)
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


def _named_by_member(
    written: dict[str, Any], row: dict[str, Any], by_id: dict[int, dict[str, Any]]
) -> dict[str, Any]:  # REQ-2222
    """``written`` with the member and kind of a profiler's result table, if ``row`` is one."""
    profile_result = _profile_result_of(row, by_id)
    if profile_result is None:
        return written
    member, kind = profile_result
    return {**written, "profile_of": member, "kind": kind}


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
            select(
                glossary_terms.c.id,
                glossary_terms.c.name,
                glossary_terms.c.definition,
                glossary_terms.c.calculation_method,
                glossary_terms.c.retired,
                glossary_terms.c.approved,
                glossary_terms.c.deprecation_asserted,
                glossary_terms.c.status_reason,
                glossary_terms.c.retire_on,
            )
            .where(glossary_terms.c.id.in_(sorted(declared)))
            .order_by(glossary_terms.c.name)
        )
    ).fetchall()

    def _status(r: Any) -> dict[str, Any]:
        # REQ-2144: what a curator asserted, as a configuration states it; a retirement's
        # successor rides its REPLACES edge, on the successor's own entry.
        if r.retired and r.status_reason:
            return {"retire": GlossaryTermRetirement(reason=r.status_reason)}
        # REQ-1387: an approval is a person's, so a configuration states it.
        approve = {"approve": True} if r.approved else {}
        if r.deprecation_asserted:
            return approve | {
                "deprecate": GlossaryTermDeprecation(
                    reason=r.status_reason or "", retire_on=r.retire_on
                )
            }
        return approve

    return [
        _written(
            GlossaryTermConfig(
                name=r.name,
                definition=r.definition,
                calculation_method=r.calculation_method,  # REQ-2243
                domains=declared[r.id],
                # By what names each edge, never its serial: the same model reads the same.
                edges=sorted(edges.get(r.id, []), key=lambda e: (e.to, e.rel_type)),
                **_status(r),
            )
        )
        for r in rows
    ]


async def _class_terms(conn: "Connection") -> dict[int, str]:
    """Each table's class term, by name, as a configuration states it (REQ-2141): every grounding
    a configuration declared or a person made. A draft sync derived, on a term no one has
    curated, is derived and not stated."""
    from provisa.core.repositories.glossary import draft_class_terms

    derived = await draft_class_terms(conn)
    rows = (
        await conn.execute_core(
            select(
                registered_tables.c.id,
                registered_tables.c.class_term_id,
                registered_tables.c.class_term_derived,
                glossary_terms.c.name,
            ).join(glossary_terms, glossary_terms.c.id == registered_tables.c.class_term_id)
        )
    ).fetchall()
    return {r.id: r.name for r in rows if not (r.class_term_derived and r.class_term_id in derived)}


async def _naming_rules(conn: "Connection") -> list[dict[str, str]]:
    rows = (
        await conn.execute_core(
            select(naming_rules.c.pattern, naming_rules.c.replacement).order_by(naming_rules.c.id)
        )
    ).fetchall()
    return [{"pattern": r.pattern, "replace": r.replacement} for r in rows]


#: How a list of naming rules is named: in a diff of files, and in what :func:`store_model` returns.
NAMING_RULE_SECTIONS = frozenset({"naming.rules", "naming_rules"})


def item_identity(section: str, item: dict[str, Any]) -> str:  # REQ-2222
    """What names an item of a model section, the same however the item was written."""
    if section == "tables":
        if item.get("profile_of") is not None:
            return f"{item['profile_of']}/profile/{item['kind']}"
        return f"{item['source_id']}/{item['schema']}/{item['table']}"
    if section == "calendars":
        return f"{item['name']}@{item['version']}"
    if section in NAMING_RULE_SECTIONS:
        return str(item["pattern"])
    if section == "rls_rules":
        target = (item.get(k) for k in ("table_id", "domain_id", "action_name", "tag_id"))
        named = "/".join(str(t) for t in target if t is not None)
        return f"{item['role_id']}/{named}/{','.join(sorted(item.get('rights') or []))}"
    for name in ("id", "name"):
        if name in item:
            return str(item[name])
    return json.dumps(item, sort_keys=True, default=str)


def _differing_fields(old: Any, new: Any, path: str = "") -> list[str]:
    """The dotted paths at which two items differ. Names only: a value may be a credential."""
    if not isinstance(old, dict) or not isinstance(new, dict):
        return [] if old == new else [path]
    out: list[str] = []
    for key in sorted(old.keys() | new.keys()):
        out += _differing_fields(old.get(key), new.get(key), f"{path}.{key}" if path else key)
    return out


def model_changes(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:  # REQ-2222
    """What changed between two :func:`store_model` readings, by section: the items added, those
    changed with the fields that differ, and those removed. Both readings are written by one hand,
    so a difference is a change."""
    sections: dict[str, Any] = {}
    unchanged = 0
    for section, items in after.items():
        old = {item_identity(section, i): i for i in before[section]}
        new = {item_identity(section, i): i for i in items}
        held = old.keys() & new.keys()
        changed = [
            {"item": key, "fields": _differing_fields(old[key], new[key])}
            for key in sorted(held)
            if old[key] != new[key]
        ]
        unchanged += len(held) - len(changed)
        entry = {
            "add": sorted(new.keys() - old.keys()),
            "change": changed,
            "remove": sorted(old.keys() - new.keys()),
        }
        if any(entry.values()):
            sections[section] = entry
    return {"changes": bool(sections), "unchanged": unchanged, "sections": sections}


async def store_model(conn: "Connection") -> dict[str, Any]:
    """Every model section the store holds, as a configuration writes it, and the naming rules
    (``naming_rules``). Sections are complete lists; an empty one is an empty list."""
    from provisa.core.db import BUILT_IN_SOURCE_IDS, SEEDED_SOURCE_IDS
    from provisa.core.models import DERIVED_TAG_IDS, SYSTEM_TAG_IDS
    from provisa.core.repositories import calendar as calendar_repo
    from provisa.core.repositories import connector as connector_repo
    from provisa.core.repositories import data_product as data_product_repo
    from provisa.core.repositories import data_product_offer as offer_repo
    from provisa.core.repositories import function as function_repo
    from provisa.core.repositories import kafka_source as kafka_repo
    from provisa.core.repositories import metric as metric_repo
    from provisa.core.repositories import relationship as rel_repo
    from provisa.core.repositories import rls as rls_repo
    from provisa.core.repositories import scheduled_trigger as trigger_repo
    from provisa.core.repositories import tag as tag_repo
    from provisa.core.repositories import table as table_repo

    # A seeded table is not the model's, so the file does not hold it and its columns are never
    # read (the meta domain's SQL catalog views alone are hundreds); a data product's member and a
    # relationship that name it are the model's, and keep its name (REQ-1634): the import holds
    # that name until the seeded table exists.
    # REQ-2222: a row rule or a tag on the demo's seeded tables is the model's, as a member or a
    # relationship naming one is; the built-in catalog tables' are the deployment's.
    demo_seeded = SEEDED_SOURCE_IDS - BUILT_IN_SOURCE_IDS
    tables = await table_repo.list_all(conn, parts_of=lambda t: not _internal_table(t))
    by_id = {int(t["id"]): t for t in tables}
    internal = {int(t["id"]) for t in tables if _internal_table(t)}
    system = {i for i in internal if by_id[i]["source_id"] not in demo_seeded}
    # A table is named in a configuration by its virtual name: its alias when it has one.
    name_of = {int(t["id"]): str(t.get("alias") or t["table_name"]) for t in tables}

    def _refs_internal(row: dict[str, Any]) -> bool:
        ids = [row.get(k) for k in ("source_table_id", "target_table_id", "table_id")]
        return any(i is not None and int(i) in system for i in ids)

    relationships = []
    for row in await rel_repo.list_all(conn):
        fields = _fields(Relationship, row)
        fields["source_table_id"] = name_of[int(row["source_table_id"])]
        if row.get("target_table_id") is not None:
            fields["target_table_id"] = name_of[int(row["target_table_id"])]
        if row.get("via_table_id") is not None:
            fields["via_table"] = name_of[int(row["via_table_id"])]
        relationships.append(_written(Relationship.model_validate(fields)))

    rls_rules = []
    for row in await rls_repo.list_all(conn):
        if _refs_internal(row) or row.get("domain_id") in ("meta", "ops", "ontology"):
            continue
        rule = {
            "role_id": row["role_id"],
            "filter": row["filter_expr"],
            "description": row["description"],  # REQ-2081
            "table_id": None if row["table_id"] is None else name_of[int(row["table_id"])],
            "domain_id": row["domain_id"],
            "action_name": row["action_name"],
            "why": row.get("why"),  # REQ-2094
            "rights": list(row["rights"]),  # REQ-2188
        }
        rls_rules.append(_written(RLSRule.model_validate(rule)))

    tag_assignments = []
    for row in await tag_repo.list_assignments(conn):
        if row.get("table_id") is not None and int(row["table_id"]) in system:
            continue
        fields = _fields(TagAssignment, row)
        fields.pop("table_id", None)  # a serial: the configuration names the table by table_ref
        fields.pop("term_id", None)  # a serial: the configuration names the term (REQ-2145)
        # REQ-2208: a serial: the configuration names the rule by its role, target and rights.
        fields.pop("rls_rule_id", None)
        tag_assignments.append(_written(TagAssignment.model_validate(fields)))

    class_terms = await _class_terms(conn)
    stores, regions = await _stores_and_regions(conn)
    model = {
        "stores": stores,
        "regions": regions,
        "sources": await _sources(conn),
        "domains": await _domains(conn),
        "roles": await _roles(conn),
        "data_products": [
            # REQ-1634 STATUS: the stored successor_id is the config's ``successor``; its members
            # are named as the configuration names them (a table by its virtual name).
            _written(
                DataProduct.model_validate(
                    _fields(
                        DataProduct,
                        {
                            **row,
                            "successor": row["successor_id"],
                            "tables": sorted(
                                name_of[t]
                                for t in await data_product_repo.member_table_ids(conn, row["id"])
                            ),
                            "commands": await data_product_repo.member_command_names(
                                conn, row["id"]
                            ),
                            # REQ-2180: its offers, a sample's views named as its tables are.
                            "offers": [
                                {
                                    **_fields(DeclaredOffer, offer),
                                    "sample_views": [name_of[v] for v in offer["sample_views"]],
                                }
                                for offer in await offer_repo.list_offers(conn, row["id"])
                            ],
                        },
                    )
                )
            )
            for row in await data_product_repo.list_all(conn)
            if row["domain_id"] not in ("meta", "ops", "ontology")
        ],
        "glossary_terms": await _glossary_terms(conn),
        "tables": [
            _named_by_member(_table(t, class_terms.get(int(t["id"]))), t, by_id)
            for t in tables
            if int(t["id"]) not in internal
        ],
        "relationships": relationships,
        "metrics": [
            # A fact-derived metric is a part of its fact table: the table brings it.
            _written(Metric.model_validate(_fields(Metric, row)))
            for row in await metric_repo.list_all(conn)
            if row.get("from_fact") is None
        ],
        "calendars": [  # REQ-2120: every version, each under its name
            _written(CalendarConfig.model_validate(_fields(CalendarConfig, row)))
            for row in sorted(
                await calendar_repo.list_all(conn), key=lambda r: (r["name"], r["version"])
            )
        ],
        "tags": [
            # The system and derived tags are code-defined, in every store.
            _written(Tag.model_validate(_fields(Tag, row)))
            for row in await tag_repo.list_all(conn)
            if row["id"] not in SYSTEM_TAG_IDS + DERIVED_TAG_IDS
        ],
        # Ordered by what names the object, never by a serial: the same model reads the same.
        "tag_assignments": sorted(
            tag_assignments, key=lambda a: json.dumps(a, sort_keys=True, default=str)
        ),
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
        "connectors": [  # REQ-2221
            _written(ConnectorConfig.model_validate(row))
            for row in await connector_repo.list_all(conn)
        ],
        "naming_rules": await _naming_rules(conn),
    }
    return _in_a_stable_order(model)


def _key(item: dict[str, Any]) -> str:
    """What names an item, as text: the same model reads in the same order on every backend
    (a database's collation does not decide it)."""
    for name in ("id", "name"):
        if name in item:
            return str(item[name])
    return json.dumps(item, sort_keys=True, default=str)


def _roles_parents_first(roles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Roles by id, each after the roles it inherits from, so an apply finds its parents stored."""
    by_id = {r["id"]: r for r in roles}
    out: list[dict[str, Any]] = []
    placed: set[str] = set()

    def _place(role: dict[str, Any]) -> None:
        if role["id"] in placed:
            return
        for parent in role.get("parents") or []:
            if parent in by_id:
                _place(by_id[parent])
        placed.add(role["id"])
        out.append(role)

    for role in sorted(roles, key=_key):
        _place(role)
    return out


def _in_a_stable_order(model: dict[str, Any]) -> dict[str, Any]:
    out = dict(model)
    for section, items in model.items():
        if section == "roles":
            out[section] = _roles_parents_first(items)
        elif section == "tables":
            out[section] = sorted(items, key=lambda t: (t["source_id"], t["schema"], t["table"]))
        elif section != "naming_rules":  # the order rules are tried in is theirs
            out[section] = sorted(items, key=_key)
    return out


def _without_stored_name(table: dict[str, Any]) -> dict[str, Any]:  # REQ-2222
    """A table as a configuration states it. A profile result's name carries its member's id in
    this store, which another store does not share: the configuration names it by member and kind
    (REQ-2052), and the store it is applied to names it."""
    if table.get("profile_of") is None:
        return table
    return {k: v for k, v in table.items() if k != "table"}


def with_store_model(
    raw: dict[str, Any], model: dict[str, Any], *, stored: bool = False
) -> dict[str, Any]:
    """The configuration ``raw`` (the deployment's file, as written) with every model section
    replaced by the store's. The file's settings — server, engine, auth and the rest — stay.

    ``stored``: for the process that runs this store's model, which reads a profile result by the
    name it has here. Without it, the configuration an export writes."""
    # A ``views:`` block declares tables of the derived source (config_loader.views_as_tables);
    # the store holds them as tables, so the file's are not carried over.
    out = {k: v for k, v in raw.items() if k not in MODEL_SECTIONS and k != "views"}
    for section in MODEL_SECTIONS:
        out[section] = model[section]
    if not stored:
        out["tables"] = [_without_stored_name(t) for t in out["tables"]]
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
