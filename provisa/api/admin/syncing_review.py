# Copyright (c) 2026 Kenneth Stott
# Canary: 9a3c6e17-2d48-4f05-b1e9-7c0d5f8a2e63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Syncing entry's differences with its catalog, and a steward's decisions on them (REQ-2247,
REQ-2122).

The review reads the catalog's values for exactly what the entry is published (its domains, as its
role reads them) and lists each value that differs from Provisa's, with both values and the value
last exchanged, grouped by kind and then by table or term.

A decision takes one side for a row (a kind and an object) or a group (a kind and a table or term):

* **Provisa's** reaches the catalog through the entry's publish, the one write path to a catalog,
  and is refused when the entry does not export.
* **The catalog's** is written into Provisa, and is refused when the entry does not import. A
  table's or column's description and a term's definition are written; an owner, a tag or a glossary
  link is refused by name, since Provisa has no write of those from a catalog yet.

Either way the value taken is recorded as last exchanged, decided by the steward by name.
"""

# Requirements: REQ-2247, REQ-2122

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import select, update

from provisa.api.errors import ApiError
from provisa.api.metadata_export import sync_values as sv
from provisa.core.repositories import catalog_exchange

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import MetadataSnapshot
    from provisa.core.database import Connection
    from provisa.core.models import CatalogConnection

PROVISA = "provisa"
CATALOG = "catalog"
SIDES = (PROVISA, CATALOG)
EXPORT = "export"
IMPORT = "import"

#: The kinds a steward can take the catalog's value of; the others are refused by name.
IMPORTABLE = (sv.DESCRIPTION, sv.COLUMN_DESCRIPTION, sv.TERM_DEFINITION)


@dataclass(frozen=True)
class Difference:  # REQ-2247
    """One value Provisa and the catalog hold differently."""

    ours: sv.SyncValue
    theirs: sv.SyncValue
    last: catalog_exchange.Exchange | None

    @property
    def changed(self) -> str | None:
        """Which side moved off the value last exchanged: provisa, catalog, both, or None when
        nothing was exchanged yet."""
        if self.last is None:
            return None
        if self.last.value == self.theirs.value:
            return PROVISA
        if self.last.value == self.ours.value:
            return CATALOG
        return "both"

    @property
    def directions(self) -> tuple[str, ...]:
        """The directions that list this difference (REQ-2247): one that Provisa moved off the
        value last exchanged is Export's to send, one the catalog moved is Import's to take; one
        both moved, or never exchanged, is listed by each direction."""
        if self.changed == PROVISA:
            return (EXPORT,)
        if self.changed == CATALOG:
            return (IMPORT,)
        return (EXPORT, IMPORT)

    def view(self) -> dict[str, Any]:
        last = self.last
        return {
            "kind": self.ours.kind,
            "group": self.ours.group,
            "object": self.ours.object,
            "provisa": catalog_exchange.to_json(self.ours.value),
            "catalog": catalog_exchange.to_json(self.theirs.value),
            "last_exchanged": None
            if last is None
            else {
                "value": catalog_exchange.to_json(last.value),
                "direction": last.direction,
                "decided_by": last.decided_by,
                "at": last.at.isoformat(),
            },
            "changed": self.changed,
        }


def differences(
    ours: dict[sv.Key, sv.SyncValue],
    theirs: dict[sv.Key, sv.SyncValue],
    last: dict[sv.Key, catalog_exchange.Exchange],
) -> list[Difference]:
    """Every value the catalog holds that differs from Provisa's. A value the catalog does not
    hold (an asset it has not been published) is not a difference: publishing is what sends it."""
    out = []
    for key, value in theirs.items():
        mine = ours[key]  # the catalog is read for the snapshot's own assets
        if mine.value != value.value:
            out.append(Difference(mine, value, last.get(key)))
    order = {kind: i for i, kind in enumerate(sv.KINDS)}
    return sorted(out, key=lambda d: (order[d.ours.kind], d.ours.group, d.ours.object))


def grouped(diffs: list[Difference], standing: dict[str, dict[str, str]]) -> dict[str, Any]:
    """The review's shape: kinds in order, each with its count, its standing choice per direction
    (``standing``, as :func:`catalog_standing_choice.held` reads it; ``None`` where the kind has
    none) and its groups, each group with its count and rows."""
    by_kind: dict[str, dict[str, list[Difference]]] = defaultdict(lambda: defaultdict(list))
    for diff in diffs:
        by_kind[diff.ours.kind][diff.ours.group].append(diff)
    return {
        "total": len(diffs),
        "kinds": [
            {
                "kind": kind,
                "count": sum(len(rows) for rows in groups.values()),
                "standing_choice": {
                    direction: choices.get(kind) for direction, choices in standing.items()
                },
                "groups": [
                    {"group": group, "count": len(rows), "rows": [r.view() for r in rows]}
                    for group, rows in groups.items()
                ],
            }
            for kind, groups in by_kind.items()
        ],
    }


def listed(entry: CatalogConnection, diffs: list[Difference]) -> list[Difference]:
    """The differences the entry lists: a direction that is off lists none (REQ-2247), so both off
    lists nothing and one off lists only the other direction's."""
    running = {d for d, on in ((EXPORT, entry.exports), (IMPORT, entry.imports)) if on}
    return [d for d in diffs if running.intersection(d.directions)]


def chosen(diffs: list[Difference], decisions: list[dict[str, Any]]) -> dict[str, list[Difference]]:
    """The differences each side takes, by the decisions sent: a row names its kind and object, a
    group its kind and group. A decision naming no current difference is refused by name, as is a
    difference decided twice."""
    taken: dict[str, list[Difference]] = {side: [] for side in SIDES}
    seen: set[sv.Key] = set()
    for decision in decisions:
        side = decision.get("side")
        kind = decision.get("kind")
        if side not in SIDES:
            raise ApiError(400, "syncing.unknown_side", f"side must be one of {', '.join(SIDES)}")
        if "object" in decision:
            matched = [d for d in diffs if d.ours.key == (kind, decision["object"])]
            named = f"{kind} on {decision['object']!r}"
        elif "group" in decision:
            matched = [d for d in diffs if (d.ours.kind, d.ours.group) == (kind, decision["group"])]
            named = f"{kind} in {decision['group']!r}"
        else:
            raise ApiError(
                400, "syncing.decision_unaddressed", "a decision names an object or a group"
            )
        if not matched:
            raise ApiError(
                409,
                "syncing.no_such_difference",
                f"there is no difference {named}",
                difference=named,
            )
        for diff in matched:
            if diff.ours.key in seen:
                raise ApiError(
                    400,
                    "syncing.decided_twice",
                    f"{diff.ours.kind} on {diff.ours.object!r} is decided twice",
                    difference=diff.ours.object,
                )
            seen.add(diff.ours.key)
            taken[side].append(diff)
    return taken


def require_sides_run(entry: CatalogConnection, taken: dict[str, list[Difference]]) -> None:
    """A direction that is off never runs (REQ-2247): Provisa's side is an Export, the catalog's
    an Import. Refused by name, as is a kind Provisa cannot take from a catalog yet."""
    if taken[PROVISA] and not entry.exports:
        raise ApiError(
            400,
            "syncing.direction_off",
            f"{entry.id!r} does not export, so Provisa's value cannot be sent to it",
            entry=entry.id,
        )
    if taken[CATALOG] and not entry.imports:
        raise ApiError(
            400,
            "syncing.direction_off",
            f"{entry.id!r} does not import, so its value cannot be taken into Provisa",
            entry=entry.id,
        )
    for diff in taken[CATALOG]:
        if diff.ours.kind not in IMPORTABLE:
            raise ApiError(
                400,
                "syncing.import_kind_unsupported",
                f"taking a catalog's {diff.ours.kind} into Provisa is not supported yet",
                kind=diff.ours.kind,
            )


async def write_into_provisa(
    conn: "Connection", snapshot: "MetadataSnapshot", diffs: list[Difference]
) -> None:
    """Write each catalog value into Provisa's model: a table's or column's description, or a
    term's definition (:data:`IMPORTABLE`)."""
    from provisa.core.repositories import glossary
    from provisa.core.schema_org import registered_tables, table_columns

    terms = {t.name: t.term_id for t in snapshot.glossary_terms}
    tables = {t.ref.fqn(): t for t in snapshot.tables}
    for diff in diffs:
        value = diff.theirs.value
        assert isinstance(value, str), diff
        if diff.ours.kind == sv.TERM_DEFINITION:
            # The empty definition is how a catalog holds a term with none (REQ-1387), which is
            # Provisa's no definition.
            await glossary.set_definition(conn, terms[diff.ours.object], value or None)
            continue
        source_id, schema_name, table_name = tables[diff.ours.group].ref.parts
        table_id = (
            select(registered_tables.c.id)
            .where(registered_tables.c.source_id == source_id)
            .where(registered_tables.c.schema_name == schema_name)
            .where(registered_tables.c.table_name == table_name)
            .scalar_subquery()
        )
        if diff.ours.kind == sv.DESCRIPTION:
            stmt = update(registered_tables).where(registered_tables.c.id == table_id)
        else:
            column = diff.ours.object.rsplit(".", 1)[-1]
            stmt = (
                update(table_columns)
                .where(table_columns.c.table_id == table_id)
                .where(table_columns.c.column_name == column)
            )
        result = await conn.execute_core(stmt.values(description=value))
        if (result.rowcount or 0) != 1:
            raise ApiError(
                409,
                "syncing.object_gone",
                f"{diff.ours.object!r} is no longer registered",
                object=diff.ours.object,
            )


async def record_taken(
    conn: "Connection", entry_id: str, taken: dict[str, list[Difference]], decided_by: str
) -> None:
    """Record each value taken as last exchanged, decided by the steward: Provisa's as an export,
    the catalog's as an import."""
    for side, direction, value_of in (
        (PROVISA, "export", lambda d: d.ours.value),
        (CATALOG, "import", lambda d: d.theirs.value),
    ):
        if taken[side]:
            await catalog_exchange.record(
                conn,
                entry_id,
                direction,
                ((d.ours.kind, d.ours.object, value_of(d)) for d in taken[side]),
                decided_by=decided_by,
                unchanged_too=True,
            )
