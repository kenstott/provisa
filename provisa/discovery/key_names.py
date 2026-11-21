# Copyright (c) 2026 Kenneth Stott
# Canary: 5b8e2d41-7c93-4a06-9f1e-3d6a0c4b8e72
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Relationship proposals from matching key names and types (REQ-2262).

A column of one table is proposed to refer to the single-column primary key of another when the
two share a type group (:func:`provisa.discovery.columns.type_groups`) and the column is named as
the key is (``customer_id`` and ``customer_id``) or as the key prefixed by the key's table
(``customer_id`` and ``customers.id``). Nothing is read from a source and no model is asked: the
proposals come from the registered columns alone, so they work across sources.
"""

# Requirements: REQ-2262

from __future__ import annotations

from collections.abc import Iterable, Sequence

from provisa.discovery.columns import ColumnFacts, TableFacts
from provisa.discovery.direction import Proposal, from_answer

#: The confidence a name and type match is proposed at: below a declared foreign key's 1.0.
CONFIDENCE = 0.9

#: A key named only this says nothing of which table it identifies: a column of another table
#: named the same is not proposed to refer to it.
_GENERIC = "id"


def _singulars(name: str) -> set[str]:
    """``name`` and the singular forms an English plural table name may take."""
    out = {name}
    if name.endswith("ies") and len(name) > 3:
        out.add(name[:-3] + "y")
    if name.endswith("es") and len(name) > 2:
        out.add(name[:-2])
    if name.endswith("s") and len(name) > 1:
        out.add(name[:-1])
    return out


def _referring_names(table: TableFacts, key: ColumnFacts) -> set[str]:
    """The column names that name ``table``'s key ``key``."""
    k = key.name.lower()
    names = set() if k == _GENERIC else {k}
    for tname in {table.table_name.lower(), (table.alias or table.table_name).lower()}:
        for stem in _singulars(tname):
            names.add(f"{stem}_{k}")
            names.add(f"{stem}{k}")
    return names


def _single_key(table: TableFacts) -> ColumnFacts | None:
    pk = table.primary_key
    return table.column(pk[0]) if len(pk) == 1 else None


def _matches(referring: TableFacts, keyed: TableFacts) -> Iterable[Proposal]:
    key = _single_key(keyed)
    if key is None or referring.table_id == keyed.table_id:
        return
    names = _referring_names(keyed, key)
    for col in referring.columns:
        if col.name.lower() not in names or not (col.groups & key.groups):
            continue
        yield from_answer(
            "key_names",
            (referring.table_id, col.name),
            (keyed.table_id, key.name),
            "many-to-one",
            CONFIDENCE,
            f"name and type match the primary key {keyed.shown}.{key.shown}",
            f"{referring.table_name}-{col.name}-to-{keyed.table_name}",
        )


def key_name_proposals(
    source: Sequence[TableFacts], target: Sequence[TableFacts] | None, *, either_way: bool
) -> list[Proposal]:
    """Each column of a source table named and typed as the primary key of a target table (the
    target set None: the source set). ``either_way``: a target table's column referring to a
    source table's key too."""
    targets = source if target is None else target
    pairs = [(a, b) for a in source for b in targets]
    if either_way:
        pairs += [(b, a) for a in source for b in targets]
    seen: set[tuple[int, str, int, str]] = set()
    out: list[Proposal] = []
    for referring, keyed in pairs:
        for p in _matches(referring, keyed):
            if p.edge not in seen:
                seen.add(p.edge)
                out.append(p)
    return out
