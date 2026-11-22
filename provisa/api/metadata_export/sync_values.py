# Copyright (c) 2026 Kenneth Stott
# Canary: 6f1d2c84-93a7-4be0-8d15-2a7e9c0b4f36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The values Syncing compares between Provisa and a catalog (REQ-2247, REQ-2122).

One neutral form for both sides, so a difference is a plain inequality: each value is addressed by
its kind and the object it is on (a table, a column or a term, by its Provisa name), and belongs to
a group (the table a column is on, or the term itself) that the review lists it under.

A description is a string; owners, tags and glossary links are sorted tuples of names. A provider's
``read`` returns the catalog's values in this form; :func:`provisa_values` gives Provisa's from the
snapshot the entry is published.

Governance tags are not compared: Provisa derives them from its rules and enforces them, so a
catalog's copy is never a value to take back.
"""

# Requirements: REQ-2247, REQ-2122

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import MetadataSnapshot

DESCRIPTION = "description"
COLUMN_DESCRIPTION = "column_description"
OWNER = "owner"
TAG = "tag"
GLOSSARY_LINK = "glossary_link"
TERM_DEFINITION = "term_definition"

#: The kinds of difference, in the order the review lists them.
KINDS = (DESCRIPTION, COLUMN_DESCRIPTION, OWNER, TAG, GLOSSARY_LINK, TERM_DEFINITION)

Value = Union[str, tuple[str, ...]]
Key = tuple[str, str]


@dataclass(frozen=True)
class SyncValue:  # REQ-2247
    """One value on one object: what kind it is, the object by its Provisa name, its group."""

    kind: str
    group: str
    object: str
    value: Value

    @property
    def key(self) -> Key:
        return (self.kind, self.object)


def names(values: "Any") -> tuple[str, ...]:
    """A set of names in its neutral form: distinct and sorted."""
    return tuple(sorted({str(v) for v in values}))


def provisa_values(snapshot: "MetadataSnapshot") -> dict[Key, SyncValue]:
    """Provisa's value of every kind on every table, column and term the snapshot holds."""
    from provisa.api.metadata_export.model import AssetKind

    stewards = {d.id: d.steward.id for d in snapshot.domains if d.steward is not None}
    tags: dict[str, set[str]] = {}
    for tag in snapshot.model_tags:
        if tag.asset is not None:
            tags.setdefault(tag.asset.fqn(), set()).add(tag.tag_id)
    links: dict[str, set[str]] = {}
    for term in snapshot.glossary_terms:
        for ref in term.assigned_to():
            links.setdefault(ref.fqn(), set()).add(term.name)
    out: list[SyncValue] = []
    for table in snapshot.tables:
        group = table.ref.fqn()
        steward = stewards.get(table.domain_id) if table.domain_id is not None else None
        out += [
            SyncValue(DESCRIPTION, group, group, table.description),
            SyncValue(OWNER, group, group, names([steward] if steward is not None else [])),
            SyncValue(TAG, group, group, names(tags.get(group, ()))),
            SyncValue(GLOSSARY_LINK, group, group, names(links.get(group, ()))),
        ]
        for column in table.columns:
            assert column.ref.kind == AssetKind.COLUMN, column.ref
            obj = column.ref.fqn()
            out += [
                SyncValue(COLUMN_DESCRIPTION, group, obj, column.description),
                SyncValue(TAG, group, obj, names(tags.get(obj, ()))),
                SyncValue(GLOSSARY_LINK, group, obj, names(links.get(obj, ()))),
            ]
    for term in snapshot.glossary_terms:
        # Every catalog Provisa publishes to requires a term's description, so a term with no
        # definition is published with an empty one (REQ-1387); the empty string is that value.
        out.append(SyncValue(TERM_DEFINITION, term.name, term.name, term.definition or ""))
    return {v.key: v for v in out}
