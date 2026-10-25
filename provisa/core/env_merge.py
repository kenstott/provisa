# Copyright (c) 2026 Kenneth Stott
# Canary: 94f60667-013d-445f-928a-b796c8b13795
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Merging one environment's model into another's (REQ-1490), landed by the one apply (REQ-1548).

A MERGE HAS TWO HALVES. The first decides WHAT the target's model becomes: every object the source
has, the target gets; an object the source no longer has is removed only when the merge asks for
removals. The second WRITES that model, and it is not this module's: the merged tree is handed to
:func:`provisa.core.env_deploy.land`, the same validation and the same apply a deploy, an undo, a
redo and a pull go through. So a merge cannot land a model a deploy would refuse, and there is one
place a model is written into an environment.

IDENTITY IS THE PATH (REQ-1526). Two environments holding the same table hold it at the same path
-- its domain and its business name -- whatever serial each schema happened to hand it. Matching by
the storage key instead matched two DIFFERENT tables that had drawn the same serial in their own
schemas, and merged one over the other.

WHAT A MERGE DOES NOT MOVE is what no load moves: bindings never travel (REQ-1491), an
identity-only row is never removed, and the seeded classes -- roles and who holds them -- are the
target's own answer (REQ-1539). Those are properties of the apply, which is why they hold here
without being restated.

A MERGE MAY BE SCOPED TO PART OF A MODEL (REQ-1506). A selection names the objects to carry, by
path; the rest of the target is left exactly as it is. The selection CLOSES OVER WHAT IT REQUIRES:
a table brought without its source, or a relationship without the table on its other side, would
land inconsistent, so an object a selected one requires and the target does not hold is carried
too and NAMED in the report. What cannot be closed -- a requirement neither environment holds -- is
refused by the apply's validation, whole.

THE GOVERNANCE SETTINGS travel with a whole merge and are not part of the tree (REQ-1489 classes
``org_settings`` per key), so they are copied beside the apply, in its transaction.
"""

# Requirements: REQ-1489, REQ-1490, REQ-1491, REQ-1506, REQ-1539, REQ-1548, REQ-1555

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from provisa.core.env_classes import IDENTITY_ONLY, SEEDED_AT_CREATION
from provisa.core.env_conflicts import Conflict
from provisa.core.env_copy import TableDelta, _copy_settings
from provisa.core.env_deploy import PROJECTED, DeployDelta, in_scope, land, table_of
from provisa.core.env_files import domain_path, kind_path
from provisa.core.env_project import KIND_DIRS, project
from provisa.core.environments import PROD, org_schema

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database

Tree = dict[str, dict[str, Any]]


class MergeSelectionError(Exception):
    """A selection that names something a merge cannot carry, saying which and why."""


@dataclass
class MergeReport:
    """What a merge does, by PATH: the unit the model is versioned in and the one a reader can
    act on. The counts are the same lists, counted, for a surface that shows a summary."""

    source_env: str
    target_env: str
    removals: bool
    delta: DeployDelta
    settings: TableDelta
    #: The paths the merge was scoped to, or None for a merge of the whole model (REQ-1506).
    selection: list[str] | None = None
    #: What the selection REQUIRED and did not name, carried with it and said so (REQ-1506).
    pulled_in: list[str] = field(default_factory=list)
    #: The commit both lines last held, and every object they each moved away from it differently
    #: (REQ-1555). ``base`` is None when the lines share no ancestor, and the empty list under it
    #: then means NOTHING WAS COMPARED rather than nothing collided.
    base: str | None = None
    conflicts: list[Conflict] = field(default_factory=list)

    @property
    def compared(self) -> bool:
        return self.base is not None

    @property
    def touches_connectivity(self) -> bool:
        """Whether the merge changed how a source is REACHED (REQ-1544)."""
        return self.delta.touches_connectivity

    def as_dict(self) -> dict[str, Any]:
        settings = self.settings
        return {
            "source_env": self.source_env,
            "target_env": self.target_env,
            "mode": "merge",
            "removals": self.removals,
            "added": len(self.delta.added) + len(settings.added),
            "changed": len(self.delta.changed) + len(settings.changed),
            "unchanged": self.delta.unchanged + settings.unchanged,
            "removed": len(self.delta.removed) + len(settings.removed),
            "paths": {
                "added": self.delta.added,
                "changed": self.delta.changed,
                "removed": self.delta.removed,
            },
            "settings": settings.as_dict(),
            "selection": self.selection,
            "pulled_in": self.pulled_in,
            "base": self.base,
            "compared": self.compared,
            "conflicts": [c.as_dict() for c in self.conflicts],
        }


async def plan_merge(
    db: "Database",
    org_id: str,
    source_env: str | None,
    target_env: str | None,
    *,
    removals: bool = False,
    selection: list[str] | None = None,
) -> MergeReport:
    """What :func:`merge_model` would do, without doing any of it. Validation runs in full, so a
    plan that returns is a plan that can be applied."""
    async with db.acquire() as conn:
        return await _merge(conn, org_id, source_env, target_env, removals, selection, apply=False)


async def merge_model(
    db: "Database",
    org_id: str,
    source_env: str | None,
    target_env: str | None,
    *,
    removals: bool = False,
    selection: list[str] | None = None,
) -> MergeReport:
    """Merge ``source_env``'s model into ``target_env``. One transaction: whole or not at all."""
    async with db.acquire() as conn, conn.transaction():
        return await _merge(conn, org_id, source_env, target_env, removals, selection, apply=True)


async def _merge(
    conn: "Connection",
    org_id: str,
    source_env: str | None,
    target_env: str | None,
    removals: bool,
    selection: list[str] | None,
    *,
    apply: bool,
) -> MergeReport:
    src_schema = org_schema(org_id, source_env)
    dst_schema = org_schema(org_id, target_env)
    if src_schema == dst_schema:
        raise ValueError(f"an environment cannot be merged into itself ({src_schema})")

    scope = PROJECTED - SEEDED_AT_CREATION
    source = {
        p: _rebased(p, b, src_schema, dst_schema)
        for p, b in (await project(conn, src_schema)).items()
        if in_scope(p, scope)
    }
    target = {p: b for p, b in (await project(conn, dst_schema)).items() if in_scope(p, scope)}

    chosen, pulled_in = _chosen(selection, source, target, removals)
    merged = merged_tree(source, target, chosen, removals)

    report = await land(
        conn,
        org_id,
        target_env,
        merged,
        target,
        _source_ref(org_id, source_env),
        False,
        _base(org_id, source_env, target_env),
        apply=apply,
    )
    # The settings are the model's too, and a scoped merge carries the objects it named alone.
    settings = (
        await _copy_settings(conn, src_schema, dst_schema, removals, apply)
        if selection is None
        else TableDelta("org_settings")
    )
    return MergeReport(
        source_env or PROD,
        target_env or PROD,
        removals,
        report.delta,
        settings,
        None if selection is None else sorted(selection),
        pulled_in,
        report.base,
        report.conflicts,
    )


def _source_ref(org_id: str, source_env: str | None) -> str:
    """What the merge names as where the model came from: the source's commit when it has one."""
    from provisa.core.env_repo import tip

    return tip(org_id, source_env or PROD) or (source_env or PROD)


def _base(org_id: str, source_env: str | None, target_env: str | None) -> str | None:
    """The commit the two environments last shared, or None when they share none (REQ-1555).

    None covers two cases that read the same way to a caller: a line that has not started holds no
    commits, and two environments rooted separately have no common ancestor. Either way there is
    nothing to compare against, and the report says so rather than reporting a clean merge.
    """
    from provisa.core.env_repo import merge_base, tip

    source_tip = tip(org_id, source_env or PROD)
    target_tip = tip(org_id, target_env or PROD)
    if source_tip is None or target_tip is None:
        return None
    return merge_base(org_id, source_tip, target_tip)


def _rebased(path: str, body: dict[str, Any], src_schema: str, dst_schema: str) -> dict[str, Any]:
    """A table registered against the source environment's OWN schema, re-addressed to the target's.

    The org tables hold a self-catalog (the registry view of REQ-1301 is registered against the
    schema it lives in), so carried verbatim it would tell the target that its registry view sits
    in the source's schema. Only a value equal to the source schema is rewritten; a real source
    schema (``public`` and the like) is left exactly as it is.
    """
    if table_of(path) == "registered_tables" and body.get("schema_name") == src_schema:
        return {**body, "schema_name": dst_schema}
    return body


def merged_tree(source: Tree, target: Tree, chosen: set[str], removals: bool) -> Tree:
    """The model the target holds after the merge: its own, with every ``chosen`` object the source
    has laid over it, and -- when ``removals`` -- less every chosen object the source lacks.

    An identity-only object is never removed (REQ-1491): its binding is a fact the target
    established, and the source not naming it is no evidence that it should go.
    """
    merged = dict(target)
    for path in chosen:
        if path in source:
            merged[path] = source[path]
        elif removals and path in merged and table_of(path) not in IDENTITY_ONLY:
            del merged[path]
    return merged


def _chosen(
    selection: list[str] | None, source: Tree, target: Tree, removals: bool
) -> tuple[set[str], list[str]]:
    """The paths the merge acts on, and those among them the selection required without naming."""
    if selection is None:
        return set(source) | set(target), []
    named = set(selection)
    for path in sorted(named):
        if path not in source and path not in target:
            raise MergeSelectionError(
                f"the selection names {path!r}, which neither environment holds"
            )
        if path not in source and not removals:
            raise MergeSelectionError(
                f"the selection names {path!r}, which the source does not hold; carrying its "
                f"absence is a removal, and removals are asked for separately"
            )
    chosen = set(named)
    pulled_in: list[str] = []
    queue = sorted(named & set(source))
    while queue:
        path = queue.pop()
        for needed in sorted(requirements(path, source[path], source)):
            # What the target already holds is not carried for the selection's sake: the selection
            # said which objects to bring, and the target's own version of the rest stays.
            if needed in target or needed in chosen or needed not in source:
                continue
            chosen.add(needed)
            pulled_in.append(needed)
            queue.append(needed)
    return chosen, sorted(pulled_in)


def requirements(path: str, body: dict[str, Any], source: Tree) -> set[str]:
    """The paths ``path``'s object cannot stand without: what it references by name or address."""
    kind = table_of(path)
    needs: set[str] = set()
    if kind == "registered_tables":
        needs.add(domain_path("/".join(path.split("/")[:-2])))
        if body.get("source_id") is not None:
            needs.add(kind_path(KIND_DIRS["sources"], body["source_id"]))
        for rel in body.get("relationships") or []:
            needs.update(p for p in (rel.get("target"), rel.get("via")) if p is not None)
        needs.update(ref["term"] for ref in body.get("glossary") or [])
        needs.update(ref["term"] for ref in body.get("class_term") or [])  # REQ-2130
        needs.update(link["target"] for link in body.get("meta_links") or [])
    elif kind == "data_products":
        if body.get("domain_id") is not None:
            needs.add(domain_path(body["domain_id"]))
    elif kind == "glossary_terms":
        needs.update(edge["to"] for edge in body.get("edges") or [])
    elif kind == "metrics":
        needs.update(_metric_tables(body.get("expression"), source))
    if kind in ("registered_tables", "commands") and body.get("product_id") is not None:
        needs.add(kind_path(KIND_DIRS["data_products"], body["product_id"]))
    return needs


def _metric_tables(expression: str | None, source: Tree) -> set[str]:
    """The table files a metric's expression reads (``SUM(orders.amount)`` reads ``orders``).

    Resolved against the source's own tables by the name the expression uses for one -- its alias,
    else its physical name. An expression that does not parse names no table here; the metric's own
    save refused such an expression, so this is not where it is reported.
    """
    import sqlglot
    from sqlglot import exp
    from sqlglot.errors import SqlglotError

    if not expression:
        return set()
    try:
        parsed = sqlglot.parse_one(expression)
    except SqlglotError:
        return set()
    named = {column.table for column in parsed.find_all(exp.Column) if column.table}
    return {
        path
        for path, body in source.items()
        if table_of(path) == "registered_tables"
        and (body.get("alias") or body.get("table_name")) in named
    }
