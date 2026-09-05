# Copyright (c) 2026 Kenneth Stott
# Canary: 3f1c9a06-5b74-4d2e-9a83-61c0f4b7e2d5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A tree of files, validated whole and applied whole to an environment's schema (REQ-1496).

:mod:`provisa.core.env_project` reads a schema and returns the tree it projects to. This module is
the direction that matters for governance: it takes a tree -- a commit, a working copy, a branch
somebody merged in a forge -- and makes it the environment's model, or refuses it entirely.

A TREE IS NOT A MODEL UNTIL A DEPLOY ACCEPTS IT. That is the whole of REQ-1496's protection, and it
is why validation here is total rather than per-file. Git computes textual three-way merges of the
serialized model, which can produce a tree that parses and does not hold -- a relationship whose
target file the other side of the merge deleted, a row policy naming a role nobody defines. Those
are found here, before any schema holds them, and named by path.

WHAT A DEPLOY DOES NOT CARRY is exactly what a merge does not carry. Bindings never travel (REQ-1491):
an ``IDENTITY_ONLY`` row that already exists keeps the connection values this environment bound, and
one the tree introduces arrives unbound. ``SEEDED_AT_CREATION`` never travels either, unless the
deploy is the CREATION (REQ-1539) -- the projected ``roles`` are a description of the model's shape,
not an instruction about who may act here, and a deploy that applied them would carry a private
control plane's self-granted rights into whatever loaded the branch.

IDENTITY IS THE PATH, and root entities keep the surrogate they already have. A registered table
whose file is unchanged must not be deleted and re-inserted under a new serial: rows outside this
module's scope reference it, and a deploy is a statement about the model rather than an instruction to
renumber it. Children -- a table's columns, a term's edges -- are replaced within their parent,
because the file is the whole statement of what they are.
"""

# Requirements: REQ-1489, REQ-1491, REQ-1496, REQ-1524, REQ-1526, REQ-1539, REQ-1556

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sqlalchemy import Table, delete, select

from provisa.core import schema_org as org
from provisa.core.env_classes import (
    BOUND_COLUMN,
    CARRIED,
    IDENTITY_ONLY,
    SEEDED_AT_CREATION,
    binding_columns,
)
from provisa.core.env_copy import _insert_rows, _resync_sequences, _scoped
from provisa.core.env_files import SUFFIX, _unsegment
from provisa.core.env_project import COMMANDS_DIR, KIND_DIRS, NAMING_PATH, project
from provisa.core.environments import org_schema
from provisa.core.schema_org import metadata as org_metadata

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database
    from provisa.core.env_conflicts import Conflict


class DeployError(Exception):
    """The tree does not hold: a reference resolves to nothing, or two files claim one address."""


#: The tables a deploy owns. Everything the projection writes, which is every carried and
#: identity-only table except ``user_role_assignments`` -- who holds a role is never projected, so a
#: tree carries no statement about it and a deploy must not act as though it did (REQ-1539).
PROJECTED: frozenset[str] = (CARRIED | IDENTITY_ONLY) - frozenset({"user_role_assignments"})

#: The directories that address a root kind rather than a domain. Every one is a reserved keyword,
#: which is what lets a first path segment decide between the two without ambiguity.
_ROOT_DIRS: dict[str, str] = {
    **{directory: table for table, directory in KIND_DIRS.items()},
    COMMANDS_DIR: COMMANDS_DIR,
    "naming": "naming_rules",
}

#: The registries whose files say how a source is REACHED rather than what it holds (REQ-1544). A
#: change to one of these invalidates open connections; a change to anything else does not.
CONNECTIVITY = frozenset({"sources", "api_sources", "kafka_sources", "kafka_sinks"})

#: The column that tells the two command registries apart. ``tracked_functions.impl_kind`` is NOT
#: NULL with a server default, so every function row has one and every projected function file
#: carries it; ``tracked_webhooks`` has no such column. The discrimination is therefore a fact about
#: the row rather than a guess from which optional keys happen to be present.
_FUNCTION_MARKER = "impl_kind"


@dataclass
class DeployDelta:
    """What a deploy does to the tree, by PATH.

    Paths rather than table rows because a path is what a person reviewing the deploy reads, and
    because the surrogate keys the rows carry are this schema's and mean nothing in the tree.
    """

    added: list[str] = field(default_factory=list)
    changed: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    unchanged: int = 0

    @property
    def touches_connectivity(self) -> bool:
        """Whether anything this deploy does changes HOW A SOURCE IS REACHED (REQ-1544).

        This is the whole of the refresh decision. An environment's runtime holds two kinds of
        thing built from the model: compiled schemas and catalog names, which are derived and can
        be rebuilt from the rows in place, and source POOLS, which are open connections. A delta
        that renames a column or adds a metric has not changed a connection, so the pools it left
        open are still pools to the right database and tearing them down would cost every later
        query a reconnect for nothing. A delta that touches a connection registry HAS, and a pool
        pointed at the old one is not a stale cache -- it is a connection to the wrong database.
        """
        return any(
            table_of(path) in CONNECTIVITY for path in (*self.added, *self.changed, *self.removed)
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "added": self.added,
            "changed": self.changed,
            "removed": self.removed,
            "unchanged": self.unchanged,
        }


def report_touches_connectivity(report: dict[str, Any]) -> bool:
    """The same question asked of a STORED report -- the dict an approval row carries (REQ-1544).

    An approved request is applied by the approval module, and what comes back is the report as it
    was written to the row rather than the object that computed it. The two report shapes are told
    apart by ``tables``, which only a copy's report has: a copy's delta is already grouped by the
    registry it wrote, and a deploy's is a flat list of tree paths.
    """
    if "tables" in report:
        return any(table["table"] in CONNECTIVITY for table in report["tables"])
    paths = (*report["added"], *report["changed"], *report["removed"])
    return any(table_of(path) in CONNECTIVITY for path in paths)


@dataclass
class DeployReport:
    env: str
    ref: str
    seed: bool
    delta: DeployDelta
    #: The commit the environment's branch and the incoming tree last shared, and every object they
    #: each moved away from it differently (REQ-1556). Both are the pull's question and nothing
    #: else's: a deploy of an arbitrary ref is the operator stating which model the environment
    #: runs, so its divergence from what stood there is the point of the act. ``base`` is None when
    #: the caller asked no such question, and the empty list under it then means NOTHING WAS
    #: COMPARED rather than nothing collided.
    base: str | None = None
    conflicts: list["Conflict"] = field(default_factory=list)

    @property
    def compared(self) -> bool:
        return self.base is not None

    def as_dict(self) -> dict[str, Any]:
        return {
            "env": self.env,
            "ref": self.ref,
            "seed": self.seed,
            **self.delta.as_dict(),
            "base": self.base,
            "compared": self.compared,
            "conflicts": [c.as_dict() for c in self.conflicts],
        }


async def plan_deploy(
    db: "Database",
    org_id: str,
    env: str | None,
    tree: dict[str, dict[str, Any]],
    *,
    ref: str,
    seed: bool = False,
    base_sha: str | None = None,
) -> DeployReport:
    """What :func:`deploy_tree` would do, without doing any of it.

    Validation runs here in full, so a plan that returns is a plan that can be applied: the report a
    person approves and the deploy they approved are computed from the same check.
    """
    async with db.acquire() as conn:
        return await _load(conn, org_id, env, tree, ref, seed, base_sha, apply=False)


async def deploy_tree(
    db: "Database",
    org_id: str,
    env: str | None,
    tree: dict[str, dict[str, Any]],
    *,
    ref: str,
    seed: bool = False,
    base_sha: str | None = None,
) -> DeployReport:
    """Make ``tree`` the model of ``env``. One transaction: it holds whole or not at all.

    ``base_sha`` names the commit this environment and the incoming tree last shared, and asks for
    the conflict report (REQ-1556). Passed by the pull and by nothing else; the comparison runs on
    this connection inside this transaction, BEFORE the apply, because afterwards the environment
    holds the incoming model and there is nothing left to notice.
    """
    async with db.acquire() as conn, conn.transaction():
        return await _load(conn, org_id, env, tree, ref, seed, base_sha, apply=True)


async def _load(
    conn: "Connection",
    org_id: str,
    env: str | None,
    tree: dict[str, dict[str, Any]],
    ref: str,
    seed: bool,
    base_sha: str | None,
    *,
    apply: bool,
) -> DeployReport:
    from provisa.core.env_conflicts import against_base

    schema = org_schema(org_id, env)
    scope = PROJECTED if seed else PROJECTED - SEEDED_AT_CREATION
    incoming = {p: b for p, b in tree.items() if in_scope(p, scope)}
    current = {p: b for p, b in (await project(conn, schema)).items() if in_scope(p, scope)}

    # REQ-1556: which of this environment's own work the incoming tree carries away. Asked against
    # the commit the two lines last shared, because ``current`` alone cannot say whether an object
    # that differs is one the incoming model changed or one somebody changed HERE -- and for a
    # fast-forward pull, where nothing diverged, the second is a local edit no commit ever holds.
    conflicts = against_base(org_id, base_sha, incoming, current) if base_sha else []

    delta = _diff(current, incoming)
    rows = _decompose(incoming, await _existing_ids(conn, schema))
    if not apply:
        return DeployReport(env or "prod", ref, seed, delta, base_sha, conflicts)

    ordered = [t for t in org_metadata.sorted_tables if t.name in scope]
    await _apply(conn, ordered, schema, rows, delta)
    await _resync_sequences(conn, ordered, schema)
    return DeployReport(env or "prod", ref, seed, delta, base_sha, conflicts)


def _diff(current: dict[str, dict], incoming: dict[str, dict]) -> DeployDelta:
    delta = DeployDelta()
    for path in sorted(set(current) | set(incoming)):
        if path not in current:
            delta.added.append(path)
        elif path not in incoming:
            delta.removed.append(path)
        elif current[path] != incoming[path]:
            delta.changed.append(path)
        else:
            delta.unchanged += 1
    return delta


def table_of(path: str) -> str:
    """Which table a path's entity lives in. The first segment decides, because every root kind
    directory is a reserved keyword a domain cannot take."""
    if path == NAMING_PATH:
        return "naming_rules"
    head = path.split("/", 1)[0]
    if head in _ROOT_DIRS:
        return _ROOT_DIRS[head]
    return "domains" if path.endswith(f"domain{SUFFIX}") else "registered_tables"


_COMMAND_TABLES: frozenset[str] = frozenset({"tracked_functions", "tracked_webhooks"})


def in_scope(path: str, scope: frozenset[str]) -> bool:
    """Whether ``path``'s entity is one this apply carries.

    Which registry a command file belongs to is a fact about its BODY, not its path -- REQ-1526
    gives both registries one address space, ``commands/<name>`` -- so ``table_of`` can only name
    the directory, and a directory is never in a scope of table names. The two registries are
    therefore in scope together or not at all; without this every command file is filtered out of
    the tree while ``_apply`` still clears both registries, and a deploy deletes the target's
    commands and writes none back.
    """
    table = table_of(path)
    if table == COMMANDS_DIR:
        return _COMMAND_TABLES <= scope
    return table in scope


# --------------------------------------------------------------------------------------------
# Reading the tree back into rows
# --------------------------------------------------------------------------------------------


def _name_of(path: str) -> list[str]:
    """The unquoted segments a root-kind path addresses, less its directory and extension."""
    return [_unsegment(s) for s in path[: -len(SUFFIX)].split("/")[1:]]


def _domain_of(path: str) -> str:
    """The domain a table's file sits in.

    A table at the tree root belongs to the seeded no-domain row, whose id is the empty string --
    ``registered_tables.domain_id`` is NOT NULL and ``domains`` always holds ``''`` (schema.sql), so
    the root is an address like any other and not an absence.
    """
    parts = path.split("/")
    return "/".join(_unsegment(p) for p in parts[:-2])


def _split(body: dict[str, Any], *children: str) -> tuple[dict[str, Any], dict[str, list]]:
    """An entity file's own columns, and the child collections nested inside it."""
    nested = {name: list(body.get(name) or []) for name in children}
    return {k: v for k, v in body.items() if k not in children}, nested


class _Ids:
    """Surrogate keys, handed out once per entity and reused where the schema already has one."""

    def __init__(self, existing: dict[str, dict[str, int]]) -> None:
        self._existing = existing
        self._next: dict[str, int] = {}

    def of(self, table: str, key: str) -> int:
        known = self._existing.get(table, {}).get(key)
        if known is not None:
            return known
        start = max([*self._existing.get(table, {}).values(), 0]) + 1
        nxt = self._next.get(table, start)
        self._next[table] = nxt + 1
        return nxt


async def _existing_ids(conn: "Connection", schema: str) -> dict[str, dict[str, int]]:
    """The surrogate each root entity already holds in this schema, addressed the way the tree
    addresses it. A deploy keeps them: rows outside this module reference a registered table, and a
    file that did not change must not renumber the thing it describes."""
    from provisa.core.env_project import _table_paths

    tables = [
        dict(r._mapping)
        for r in (
            await conn.execute_core(select(_scoped(org.registered_tables, schema)))
        ).fetchall()
    ]
    terms = [
        dict(r._mapping)
        for r in (await conn.execute_core(select(_scoped(org.glossary_terms, schema)))).fetchall()
    ]
    commands: dict[str, int] = {}
    for table in (org.tracked_functions, org.tracked_webhooks):
        for row in (await conn.execute_core(select(_scoped(table, schema)))).fetchall():
            commands[f"{table.name}/{dict(row._mapping)['name']}"] = dict(row._mapping)["id"]
    return {
        "registered_tables": {p: i for i, p in _table_paths(tables).items()},
        "glossary_terms": {t["name"]: t["id"] for t in terms},
        "commands": commands,
    }


def _decompose(  # noqa: C901 -- allow-complex: one branch per projected kind, and the inverse of
    # a projection is only readable when its kinds are read in the same order the projection wrote
    # them; splitting it into per-kind helpers would hide that correspondence without removing a
    # single branch.
    tree: dict[str, dict[str, Any]],
    existing: dict[str, dict[str, int]],
) -> dict[str, list[dict[str, Any]]]:
    """Every row the tree describes, with references resolved and surrogates assigned.

    Nothing is written here. A tree that does not hold raises before a statement is issued, which is
    what lets the caller show a report and apply exactly what it showed.
    """
    ids = _Ids(existing)
    # The tree belongs to the caller, who holds it to show a report and may deploy it again. Nothing
    # below reads a file twice, but the splitting does pop nested keys off the bodies it is handed.
    tree = deepcopy(tree)
    rows: dict[str, list[dict[str, Any]]] = {name: [] for name in PROJECTED}

    table_paths = sorted(p for p in tree if table_of(p) == "registered_tables")
    term_paths = sorted(p for p in tree if table_of(p) == "glossary_terms")
    table_id = {p: ids.of("registered_tables", p) for p in table_paths}
    term_id = {p: ids.of("glossary_terms", _name_of(p)[0]) for p in term_paths}

    def resolve_table(path: str, inside: str, what: str) -> int:
        if path not in table_id:
            raise DeployError(
                f"{inside!r} names {path!r} as its {what}, and the tree holds no such table; a "
                f"merge that deleted it on one side and kept the reference on the other produces "
                f"exactly this, and it is refused whole rather than loaded with a dangling edge"
            )
        return table_id[path]

    def resolve_term(path: str, inside: str) -> int:
        if path not in term_id:
            raise DeployError(
                f"{inside!r} names the glossary term {path!r}, which the tree does not hold"
            )
        return term_id[path]

    for path in sorted(tree):
        table = table_of(path)
        body = tree[path]

        if table == "domains":
            own, nested = _split(body, "row_policies")
            domain = _domain_id_of(path)
            rows["domains"].append({**own, "id": domain})
            for policy in nested["row_policies"]:
                rows["rls_rules"].append({**policy, "domain_id": domain, "table_id": None})

        elif table == "registered_tables":
            own, nested = _split(
                body, "columns", "relationships", "row_policies", "glossary", "meta_links", "tags"
            )
            tid = table_id[path]
            domain = _domain_of(path)
            rows["registered_tables"].append({**own, "id": tid, "domain_id": domain})
            for column in nested["columns"]:
                rows["table_columns"].append({**column, "table_id": tid, "domain_id": domain})
            for rel in nested["relationships"]:
                target = rel.pop("target", None)
                rows["relationships"].append(
                    {
                        **rel,
                        "source_table_id": tid,
                        "target_table_id": (
                            None if target is None else resolve_table(target, path, "target")
                        ),
                    }
                )
            for policy in nested["row_policies"]:
                rows["rls_rules"].append({**policy, "table_id": tid, "domain_id": None})
            for ref in nested["glossary"]:
                rows["glossary_term_refs"].append(
                    {
                        "table_id": tid,
                        "column_name": ref["column_name"],
                        "term_id": resolve_term(ref["term"], path),
                    }
                )
            for link in nested["meta_links"]:
                rows["table_meta_links"].append(
                    {
                        "source_table_id": tid,
                        "target_table_id": resolve_table(link["target"], path, "meta link"),
                    }
                )
            rows["tag_assignments"].extend(_tags(nested["tags"], path, table_id=tid))

        elif table == "glossary_terms":
            own, nested = _split(body, "edges", "experts", "domains")
            gid = term_id[path]
            rows["glossary_terms"].append({**own, "id": gid})
            for edge in nested["edges"]:
                rows["glossary_term_edges"].append(
                    {
                        "from_term_id": gid,
                        "to_term_id": resolve_term(edge["to"], path),
                        "rel_type": edge["rel_type"],
                    }
                )
            for expert in nested["experts"]:
                rows["glossary_term_experts"].append({**expert, "term_id": gid})
            for domain in nested["domains"]:
                rows["glossary_term_domains"].append({**domain, "term_id": gid})

        elif table == "tags":
            own, nested = _split(body, "values")
            tag = _name_of(path)[0]
            rows["tags"].append({**own, "id": tag})
            for value in nested["values"]:
                rows["tag_param_values"].append({**value, "tag_id": tag})

        elif table == "sources":
            own, nested = _split(body, "tags")
            source = _name_of(path)[0]
            rows["sources"].append({**own, "id": source})
            rows["tag_assignments"].extend(_tags(nested["tags"], path, source_id=source))

        elif table == "kafka_sources":
            own, nested = _split(body, "topics")
            source = _name_of(path)[0]
            rows["kafka_sources"].append({**own, "id": source})
            for topic in nested["topics"]:
                rows["kafka_topics"].append({**topic, "source_id": source})

        elif table == "api_sources":
            own, nested = _split(body, "endpoints")
            source = _name_of(path)[0]
            rows["api_sources"].append({**own, "id": source})
            for endpoint in nested["endpoints"]:
                rows["api_endpoints"].append({**endpoint, "source_id": source})

        elif table == "calendars":
            name, version = _name_of(path)
            rows["calendars"].append({**body, "name": name, "version": version})

        elif table == COMMANDS_DIR:
            own, nested = _split(body, "tags")
            name = _name_of(path)[0]
            registry = "tracked_functions" if _FUNCTION_MARKER in own else "tracked_webhooks"
            rows[registry].append(
                {**own, "name": name, "id": ids.of("commands", f"{registry}/{name}")}
            )
            rows["tag_assignments"].extend(_tags(nested["tags"], path, command_name=name))

        elif table == "naming_rules":
            for order, rule in enumerate(body.get("rules") or [], start=1):
                rows["naming_rules"].append({**rule, "id": order})

        else:
            # metrics, roles, materialized_views, kafka_sinks -- a flat kind keyed by its path.
            key = "name" if table == "metrics" else "id"
            entry = {**body, key: _name_of(path)[0]}
            if table == "kafka_sinks":
                # The one flat kind with a surrogate: its address is the name it was projected
                # under, and nothing references the serial.
                entry = {k: v for k, v in entry.items() if k != "id"}
                entry["id"] = ids.of("kafka_sinks", _name_of(path)[0])
            rows[table].append(entry)

    return rows


def _domain_id_of(path: str) -> str:
    """The domain a ``domain.yaml`` describes: its directory, unquoted."""
    # The root ``domain.yaml`` is the seeded no-domain row, id ``''`` -- the same row the tables
    # beside it point at. It is a real domain with a real file, not a missing one.
    return "/".join(_unsegment(p) for p in path.split("/")[:-1])


def _tags(
    entries: list[dict[str, Any]],
    path: str,
    *,
    table_id: int | None = None,
    source_id: str | None = None,
    command_name: str | None = None,
) -> list[dict[str, Any]]:
    """Nested tag entries back into ``tag_assignments`` rows.

    ``on`` is REQ-1385's fragment and says which PART of the entity carries the tag; its absence
    says the entity itself does. ``object_key`` is recomputed rather than read, because the stored
    one embeds a serial that means nothing in the tree it arrived from.
    """
    out: list[dict[str, Any]] = []
    for entry in entries:
        body = {k: v for k, v in entry.items() if k not in ("on", "at")}
        fragment = entry.get("on")
        if command_name is not None and fragment is not None:
            raise DeployError(
                f"{path!r} carries a tag on {fragment!r}, but a command has no parts a fragment "
                f"can name; a command tag is on the command itself"
            )
        if fragment is None and command_name is not None:
            body |= {
                "object_type": "command",
                "command_name": command_name,
                "object_key": f"command:{command_name}",
            }
        elif fragment is None and table_id is not None:
            body |= {
                "object_type": "table",
                "table_id": table_id,
                "object_key": f"table:{table_id}",
            }
        elif fragment is None:
            body |= {
                "object_type": "source",
                "source_id": source_id,
                "object_key": f"source:{source_id}",
            }
        elif fragment.startswith("#field:"):
            column = fragment[len("#field:") :]
            body |= {
                "object_type": "column",
                "table_id": table_id,
                "column_name": column,
                "object_key": f"column:{table_id}:{column}",
            }
        elif fragment.startswith("#rel:"):
            rel = fragment[len("#rel:") :]
            body |= {
                "object_type": "relationship",
                "relationship_id": rel,
                "object_key": f"relationship:{rel}",
            }
        else:
            raise DeployError(
                f"{path!r} carries a tag on {fragment!r}, which is not an address REQ-1385 gives a "
                f"part of an entity; the fragments a tag may name are #field: and #rel:"
            )
        body.setdefault("base_tag_id", body["tag_id"])
        out.append(body)
    return out


# --------------------------------------------------------------------------------------------
# Applying
# --------------------------------------------------------------------------------------------


def _writable_columns(table: Table) -> set[str]:
    """The columns a deploy supplies. An identity-only table's bindings are not among them: they are
    this environment's own, and the tree has nothing to say about them (REQ-1491)."""
    names = {c.name for c in table.columns}
    if table.name in IDENTITY_ONLY:
        return names - binding_columns(table.name)
    return names


async def _apply(
    conn: "Connection",
    ordered: list[Table],
    schema: str,
    rows: dict[str, list[dict[str, Any]]],
    delta: DeployDelta,
) -> None:
    """Delete what the tree no longer holds, then write what it does, parents before children.

    An identity-only row is never deleted, for REQ-1491's reason: the binding it holds is a fact
    this environment established, and a model that stopped naming the source is not evidence that
    the credential should be destroyed. The row simply goes unreferenced.
    """
    del delta  # the report is computed from the projection; the write is unconditional per table
    for table in reversed(ordered):
        if table.name in IDENTITY_ONLY:
            continue
        await conn.execute_core(delete(_scoped(table, schema)))
    for table in ordered:
        scoped = _scoped(table, schema)
        writable = _writable_columns(table)
        payload = [
            {k: v for k, v in row.items() if k in writable} for row in rows.get(table.name, [])
        ]
        if table.name in IDENTITY_ONLY:
            await _upsert_identity(conn, scoped, payload)
            continue
        # Grouped by which columns each row actually sets: a multi-row INSERT takes its column
        # list from the first row, so one file that omits an optional key would blank that column
        # for every file after it that set one. A key absent from a group takes the server default.
        for shape in sorted({frozenset(row) for row in payload}, key=sorted):
            await _insert_rows(conn, scoped, [r for r in payload if frozenset(r) == shape])


async def _upsert_identity(conn: "Connection", table: Table, rows: list[dict[str, Any]]) -> None:
    """An identity-only kind: update what is here, insert what is not, delete nothing.

    A row this deploy introduces is marked unbound (REQ-1491) -- an empty host is not an absent one,
    and the connection builder would read the column defaults as localhost.
    """
    key = next(iter(table.primary_key.columns)).name
    present = {
        dict(r._mapping)[key] for r in (await conn.execute_core(select(table.c[key]))).fetchall()
    }
    inserts = []
    for row in rows:
        if row[key] in present:
            values = {k: v for k, v in row.items() if k != key}
            if values:
                await conn.execute_core(
                    table.update().where(table.c[key] == row[key]).values(**values)
                )
        else:
            inserts.append({**row, BOUND_COLUMN: False})
    await _insert_rows(conn, table, inserts)
