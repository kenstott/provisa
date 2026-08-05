# Copyright (c) 2026 Kenneth Stott
# Canary: 1f8a4c73-6b02-4d95-8e21-9c7f0b3ad684
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Addressing for published assets, and the refusals that protect it (REQ-1070).

Both the snapshot builder and the governance projection have to turn a configured table into
the same :class:`AssetRef`, and both have to resolve the bare table names configs use. They
share this module so a governance tag and a lineage edge naming one column always address it
identically — an external catalog matches assets by that address alone.
"""

# Requirements: REQ-1070, REQ-1071

from __future__ import annotations

from provisa.api.metadata_export.model import AssetKind, AssetRef
from provisa.core.models import Source, Table


def source_ref(source: Source) -> AssetRef:  # REQ-1070
    return AssetRef(kind=AssetKind.SOURCE, parts=(source.id,))


class SnapshotBuildError(RuntimeError):
    """Base for every refusal to build a snapshot from an inconsistent config."""


class UnknownTableError(SnapshotBuildError):
    """A relationship or lineage leaf named a table that is not in the config."""

    def __init__(self, name: str, context: str) -> None:
        super().__init__(f"{context} references table {name!r}, which is not in the config")
        self.name = name
        self.context = context


class AmbiguousTableError(SnapshotBuildError):
    """A bare table name matches tables in more than one source, so it addresses no one asset."""

    def __init__(self, name: str, candidates: list[str], context: str) -> None:
        super().__init__(
            f"{context} references table {name!r}, which matches {candidates} — "
            "qualify the reference; a snapshot must not guess which asset is governed"
        )
        self.name = name
        self.candidates = candidates
        self.context = context


class UnqualifiedLineageError(SnapshotBuildError):
    """A lineage leaf carries no relation, so the upstream column cannot be addressed."""

    def __init__(self, view: str, output: str, leaf: str) -> None:
        super().__init__(
            f"view {view!r} column {output!r} derives from unqualified leaf {leaf!r}; "
            "the upstream asset cannot be identified"
        )
        self.view = view
        self.output = output
        self.leaf = leaf


def table_ref(table: Table) -> AssetRef:  # REQ-1070
    """A table's address: source, schema, table — the same triple that keys it in Provisa."""
    return AssetRef(
        kind=AssetKind.TABLE, parts=(table.source_id, table.schema_name, table.table_name)
    )


def column_ref(table: Table, column_name: str) -> AssetRef:  # REQ-1070
    return AssetRef(kind=AssetKind.COLUMN, parts=(*table_ref(table).parts, column_name))


class TableIndex:
    """Resolve the names configs use for tables (bare ``table``, or ``schema.table``) to a Table.

    Relationship ``source_table_id``/``target_table_id`` and lineage relation names are both
    written as bare table names. When one bare name matches tables in two sources it addresses
    neither, and the build refuses rather than picking one.
    """

    def __init__(self, tables: list[Table]) -> None:
        self._by_bare: dict[str, list[Table]] = {}
        self._by_qualified: dict[str, Table] = {}
        for table in tables:
            self._by_bare.setdefault(table.table_name, []).append(table)
            self._by_qualified[f"{table.schema_name}.{table.table_name}"] = table
            self._by_qualified[".".join(table_ref(table).parts)] = table

    def resolve(self, name: str, context: str) -> Table:
        qualified = self._by_qualified.get(name)
        if qualified is not None:
            return qualified
        matches = self._by_bare.get(name)
        if not matches:
            raise UnknownTableError(name, context)
        if len(matches) > 1:
            raise AmbiguousTableError(
                name, [".".join(table_ref(t).parts) for t in matches], context
            )
        return matches[0]
