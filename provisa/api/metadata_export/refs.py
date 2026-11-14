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

# REQ-1385: the URI grammar is the core's (provisa.core.model_uri), which the model's history names
# objects by as well (REQ-1524); the export publishes the same URIs.
from provisa.core.model_uri import (  # noqa: F401  (re-exported)
    RESERVED_KIND_KEYWORDS,
    column_uri,
    command_uri,
    data_product_uri,
    domain_uri,
    metric_uri,
    relationship_uri,
    source_uri,
    table_uri,
    tag_uri,
    term_uri,
)


def source_ref(source: Source) -> AssetRef:  # REQ-1070
    return AssetRef(kind=AssetKind.SOURCE, parts=(source.id,))


def data_product_ref(product_id: str) -> AssetRef:  # REQ-1634
    return AssetRef(kind=AssetKind.DATA_PRODUCT, parts=(product_id,))


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
    written as bare table names. The config vocabulary is the VIRTUAL name — alias when set,
    else the physical table name (the loader's resolver and config_export's id_to_name agree
    on this) — so both index. When one bare name matches tables in two sources it addresses
    neither, and the build refuses rather than picking one.
    """

    def __init__(self, tables: list[Table], seeded: frozenset[str] = frozenset()) -> None:
        # REQ-1634: the names of the seeded tables the store holds and ``tables`` leaves out.
        self._seeded = seeded
        self._by_bare: dict[str, list[Table]] = {}
        self._by_qualified: dict[str, Table] = {}
        for table in tables:
            names = {table.table_name} | ({table.alias} if table.alias else set())
            for name in names:
                self._by_bare.setdefault(name, []).append(table)
                self._by_qualified[f"{table.schema_name}.{name}"] = table
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

    def resolve_published(self, name: str, context: str) -> Table | None:
        """``resolve``, except a name that identifies a seeded table answers None: a seeded table
        stays out of the export model and publishes nowhere (REQ-1634), so what names it has
        nothing to anchor to. Any other name no table answers is refused, as ``resolve`` does."""
        if name in self._seeded and name not in self._by_qualified and name not in self._by_bare:
            return None
        return self.resolve(name, context)
