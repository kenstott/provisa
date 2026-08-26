# Copyright (c) 2026 Kenneth Stott
# Canary: 8c40a7d3-16fe-4b52-9e21-73adc0e5f118
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An environment's schema, projected into the file tree REQ-1526 describes.

:mod:`provisa.core.env_files` says what the tree IS -- where a file lives, what may appear in one,
and how it is rendered. This module is the half that reads a database: it takes one environment's
schema and returns the tree that environment's model projects to, which the write-through
(REQ-1524) then commits.

WHY THE TWO ARE SEPARATE. The layout has to be decided identically by the writer, the loader and
the diff, and only the writer holds a connection. Keeping the layout pure means the loader and the
diff agree with the writer by CALLING the same functions rather than by re-implementing them.

EVERY REFERENCE TO A SURROGATE IS RESOLVED HERE, because this is where the rows to resolve against
are in hand. A relationship's ``target_table_id`` becomes the target's path, a term edge's
``to_term_id`` becomes the term's path, and a tag assignment's ``object_key`` -- which embeds a
serial -- is replaced by the REQ-1385 fragment naming what was tagged. Nothing downstream ever sees
an integer that means something only in the schema it was read from.
"""

# Requirements: REQ-1385, REQ-1489, REQ-1491, REQ-1524, REQ-1526

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core import schema_org as org
from provisa.core.env_copy import _scoped
from provisa.core.env_files import (
    Child,
    FileLayoutError,
    domain_path,
    entity,
    kind_path,
    table_path,
)

if TYPE_CHECKING:
    from sqlalchemy import Table as SaTable

    from provisa.core.database import Connection

#: Where each root-level registry's files live. Every one of these is a RESERVED_KIND_KEYWORDS
#: entry, because a domain of the same name would put its directory here.
KIND_DIRS: dict[str, str] = {
    "sources": "sources",
    "api_sources": "api-sources",
    "kafka_sources": "kafka-sources",
    "kafka_sinks": "kafka-sinks",
    "metrics": "metrics",
    "roles": "roles",
    "tags": "tags",
    "glossary_terms": "terms",
    "materialized_views": "views",
    "calendars": "calendars",
}

#: The two command registries share one directory, because REQ-1385 gives them one address space:
#: ``provisa://<org>/commands/<name>``. A name used by both is a collision in that address space
#: and is refused rather than resolved by preferring one registry.
COMMANDS_DIR = "commands"

#: The naming rules are ORDERED -- a later rule rewrites what an earlier one produced -- so they are
#: one entity, the rule set, and not one file per rule. A directory of files whose order mattered
#: would encode that order in filenames nobody could rename.
NAMING_PATH = "naming/rules.yaml"


async def _rows(conn: "Connection", table: "SaTable", schema: str) -> list[dict[str, Any]]:
    """Every row of ``table`` in ``schema``, as plain dicts with plain string keys.

    ``str(key)`` is not decoration: a result mapping is keyed by SQLAlchemy''s ``quoted_name``, a str
    SUBCLASS, and the YAML dumper refuses a type it was not taught -- so a key that reads as a
    string in every comparison here would fail at serialization time instead.
    """
    result = await conn.execute_core(select(_scoped(table, schema)))
    return [{str(k): v for k, v in r._mapping.items()} for r in result.fetchall()]


def _business_name(table_row: dict[str, Any]) -> str:
    """REQ-1385's business identity: the alias when the table has one, else its physical name."""
    return table_row["alias"] or table_row["table_name"]


def _table_paths(tables: list[dict[str, Any]]) -> dict[int, str]:
    """Every registered table's path, by the surrogate id the other rows reference it with."""
    paths: dict[int, str] = {}
    for row in tables:
        path = table_path(row["domain_id"], _business_name(row))
        if path in paths.values():
            raise FileLayoutError(
                f"two registered tables address {path!r}; a business name is unique within a domain "
                f"by the naming authority's write-time conflict check, so this schema was written "
                f"around it"
            )
        paths[row["id"]] = path
    return paths


def _fragment(assignment: dict[str, Any], rel_names: dict[str, str]) -> str | None:
    """What a tag assignment is ON, as the REQ-1385 fragment rather than the stored object_key.

    ``object_key`` embeds ``table_id`` -- a serial that means nothing in another environment -- so
    it is recomputed on load rather than written down. ``None`` for a tag on the entity the file is
    ABOUT: the file already says which object that is, and ``on: ''`` would only invite a reader to
    wonder what the empty string addressed.
    """
    kind = assignment["object_type"]
    if kind == "column":
        return f"#field:{assignment['column_name']}"
    if kind == "relationship":
        return f"#rel:{rel_names[assignment['relationship_id']]}"
    return None


#: The routing columns :func:`_assignments_for` adds so a caller can group assignments by owner.
#: Named in every nesting Child so they are dropped again on the way into the file: which object an
#: assignment hangs off is what the nesting and the fragment say, and the serials that said it
#: before are not portable.
#: ``at`` joins them: the fragment alone repeats across the tags on one object, and a sort key that
#: repeats leaves the order of those tags to whatever the database returned. (base_tag_id,
#: object_key) is unique, so fragment-plus-tag is unique too.
OWNER_KEYS = ("owner_table_id", "owner_source_id", "owner_command_name", "at")


def _assignments_for(
    assignments: list[dict[str, Any]],
    rel_names: dict[str, str],
    rel_owner: dict[str, int],
) -> list[dict[str, Any]]:
    """Tag assignments rewritten to name their object by fragment, ready to nest."""
    out = []
    for row in assignments:
        carried = {
            k: v
            for k, v in row.items()
            if k in ("tag_id", "base_tag_id", "reason", "expires_on") and v is not None
        }
        fragment = _fragment(row, rel_names)
        if fragment is not None:
            carried["on"] = fragment
        carried["at"] = f"{fragment or ''}|{row['tag_id']}"
        # A RELATIONSHIP tag is owned by the table the edge starts at, which is the file the edge
        # itself is written in. Left on its own table_id -- NULL for a relationship assignment --
        # it would belong to no file and vanish from the projection.
        carried["owner_table_id"] = (
            row["table_id"] if row["relationship_id"] is None else rel_owner[row["relationship_id"]]
        )
        carried["owner_source_id"] = row["source_id"]
        # A COMMAND tag hangs off neither a table nor a source, so neither nesting would claim it
        # and it would vanish the way a relationship tag once did. Its file is commands/<name>.
        carried["owner_command_name"] = row["command_name"]
        out.append(carried)
    return out


async def project(conn: "Connection", schema: str) -> dict[str, dict[str, Any]]:
    """The whole tree for the environment stored in ``schema``, addressed by path.

    Read in one pass rather than per entity: a projection is a snapshot of the model, and reading
    each table once inside one transaction is what makes it one.
    """
    tables = await _rows(conn, org.registered_tables, schema)
    paths = _table_paths(tables)
    columns = await _rows(conn, org.table_columns, schema)
    relationships = await _rows(conn, org.relationships, schema)
    rls = await _rows(conn, org.rls_rules, schema)
    term_refs = await _rows(conn, org.glossary_term_refs, schema)
    meta_links = await _rows(conn, org.table_meta_links, schema)
    assignments = await _rows(conn, org.tag_assignments, schema)
    terms = await _rows(conn, org.glossary_terms, schema)

    rel_names = {r["id"]: (r["alias"] or r["id"]) for r in relationships}
    rel_owner = {r["id"]: r["source_table_id"] for r in relationships}
    tagged = _assignments_for(assignments, rel_names, rel_owner)
    term_paths = {t["id"]: kind_path(KIND_DIRS["glossary_terms"], t["name"]) for t in terms}

    tree: dict[str, dict[str, Any]] = {}

    for row in await _rows(conn, org.domains, schema):
        tree[domain_path(row["id"])] = entity(
            "domains",
            row,
            row_policies=Child(
                "role_id",
                "domain_id",
                [r for r in rls if r["domain_id"] == row["id"] and r["table_id"] is None],
                table="rls_rules",
            ),
        )

    for row in tables:
        table_id = row["id"]
        own_rels = [r for r in relationships if r["source_table_id"] == table_id]
        for rel in own_rels:
            # A relationship whose target is a FUNCTION has no target table, and gets no "target"
            # key rather than a null one -- the tree writes absence as absence (REQ-1526).
            if rel["target_table_id"] is not None:
                rel["target"] = paths[rel["target_table_id"]]
        own_links = [
            {"target": paths[m["target_table_id"]]}
            for m in meta_links
            if m["source_table_id"] == table_id
        ]
        own_refs = [
            {"column_name": r["column_name"], "term": term_paths[r["term_id"]]}
            for r in term_refs
            if r["table_id"] == table_id
        ]
        tree[paths[table_id]] = entity(
            "registered_tables",
            row,
            columns=Child(
                "column_name",
                "table_id",
                [c for c in columns if c["table_id"] == table_id],
                table="table_columns",
            ),
            # SORTED AND ADDRESSED BY ``id``, not by alias: alias is nullable, and a null sort key
            # is no address at all. The id is a name the caller chose (upsertRelationship takes it),
            # so unlike every other child key it is model rather than storage, and ``keep`` re-admits
            # it. Both endpoints are dropped -- the source is the file it is written in, the target
            # is written above as a path.
            relationships=Child(
                "id",
                ("source_table_id", "target_table_id"),
                own_rels,
                table="relationships",
                keep=("id",),
            ),
            row_policies=Child(
                "role_id",
                "table_id",
                [r for r in rls if r["table_id"] == table_id],
                table="rls_rules",
            ),
            glossary=Child("column_name", "table_id", own_refs, table="glossary_term_refs"),
            meta_links=Child("target", "source_table_id", own_links, table="table_meta_links"),
            tags=Child(
                "at",
                OWNER_KEYS,
                [a for a in tagged if a["owner_table_id"] == table_id],
                table="tag_assignments",
            ),
        )

    edges = await _rows(conn, org.glossary_term_edges, schema)
    experts = await _rows(conn, org.glossary_term_experts, schema)
    # REQ-1591: a rooted term's domains are derived from its refs and so travel with the tables,
    # but an abstract term's are DECLARED and exist nowhere else -- projecting them is what keeps
    # a copied environment's terms scoped to the domains their author named.
    term_domains = await _rows(conn, org.glossary_term_domains, schema)
    for row in terms:
        tree[term_paths[row["id"]]] = entity(
            "glossary_terms",
            row,
            edges=Child(
                "to",
                "from_term_id",
                [
                    {"rel_type": e["rel_type"], "to": term_paths[e["to_term_id"]]}
                    for e in edges
                    if e["from_term_id"] == row["id"]
                ],
                table="glossary_term_edges",
            ),
            experts=Child(
                "user_id",
                "term_id",
                [e for e in experts if e["term_id"] == row["id"]],
                table="glossary_term_experts",
            ),
            domains=Child(
                "domain_id",
                "term_id",
                [d for d in term_domains if d["term_id"] == row["id"]],
                table="glossary_term_domains",
            ),
        )

    tag_values = await _rows(conn, org.tag_param_values, schema)
    for row in await _rows(conn, org.tags, schema):
        tree[kind_path(KIND_DIRS["tags"], row["id"])] = entity(
            "tags",
            row,
            values=Child(
                "value",
                "tag_id",
                [v for v in tag_values if v["tag_id"] == row["id"]],
                table="tag_param_values",
            ),
        )

    topics = await _rows(conn, org.kafka_topics, schema)
    endpoints = await _rows(conn, org.api_endpoints, schema)
    source_tags = [t for t in tagged if t["owner_source_id"] is not None]
    for row in await _rows(conn, org.sources, schema):
        tree[kind_path(KIND_DIRS["sources"], row["id"])] = entity(
            "sources",
            row,
            tags=Child(
                "at",
                OWNER_KEYS,
                [t for t in source_tags if t["owner_source_id"] == row["id"]],
                table="tag_assignments",
            ),
        )
    for row in await _rows(conn, org.kafka_sources, schema):
        tree[kind_path(KIND_DIRS["kafka_sources"], row["id"])] = entity(
            "kafka_sources",
            row,
            topics=Child(
                "topic",
                "source_id",
                [t for t in topics if t["source_id"] == row["id"]],
                table="kafka_topics",
            ),
        )
    for row in await _rows(conn, org.api_sources, schema):
        tree[kind_path(KIND_DIRS["api_sources"], row["id"])] = entity(
            "api_sources",
            row,
            endpoints=Child(
                "table_name",
                "source_id",
                [e for e in endpoints if e["source_id"] == row["id"]],
                table="api_endpoints",
            ),
        )
    for row in await _rows(conn, org.kafka_sinks, schema):
        tree[kind_path(KIND_DIRS["kafka_sinks"], row["id"])] = entity("kafka_sinks", row)

    for table, key in (
        (org.metrics, "name"),
        (org.roles, "id"),
        (org.materialized_views, "id"),
    ):
        for row in await _rows(conn, table, schema):
            tree[kind_path(KIND_DIRS[table.name], row[key])] = entity(table.name, row)

    for row in await _rows(conn, org.calendars, schema):
        tree[kind_path(KIND_DIRS["calendars"], row["name"], row["version"])] = entity(
            "calendars", row
        )

    for table in (org.tracked_functions, org.tracked_webhooks):
        for row in await _rows(conn, table, schema):
            path = kind_path(COMMANDS_DIR, row["name"])
            if path in tree:
                raise FileLayoutError(
                    f"the command name {row['name']!r} is registered as both a function and a "
                    f"webhook; REQ-1385 gives commands ONE address space, so the two registries "
                    f"cannot both claim {path!r}"
                )
            tree[path] = entity(
                table.name,
                row,
                tags=Child(
                    "at",
                    OWNER_KEYS,
                    [a for a in tagged if a["owner_command_name"] == row["name"]],
                    table="tag_assignments",
                ),
            )

    rules = await _rows(conn, org.naming_rules, schema)
    if rules:
        tree[NAMING_PATH] = {
            "rules": [
                {"pattern": r["pattern"], "replacement": r["replacement"]}
                for r in sorted(rules, key=lambda r: r["id"])
            ]
        }
    return tree
