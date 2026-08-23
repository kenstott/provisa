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

from urllib.parse import quote

from provisa.api.metadata_export.model import AssetKind, AssetRef
from provisa.core.models import Source, Table


def source_ref(source: Source) -> AssetRef:  # REQ-1070
    return AssetRef(kind=AssetKind.SOURCE, parts=(source.id,))


# REQ-1385: kind keywords are reserved path segments; a domain with one of these names
# would make its URIs unparseable, so domain creation refuses them.
#
# REQ-1526 widened the list from the four kinds that appear in a URI to every kind the serialized
# model gives a DIRECTORY to. In a URI a kind is only ever preceded by domain segments, so an
# unreserved kind was merely unparseable in the cases that reached it; in a directory tree the kind
# and the domain are siblings on disk, and a domain named for one would have its own file written
# into the kind's directory. Reserving the whole set costs an org the use of about a dozen domain
# names and removes the collision entirely.
RESERVED_KIND_KEYWORDS = (
    "tables",
    "sources",
    "api-sources",
    "kafka-sources",
    "kafka-sinks",
    "tags",
    "roles",
    "commands",
    "terms",
    "metrics",
    "views",
    "calendars",
    "naming",
    "domain",
)

_URI_SCHEME = "provisa"


def _segment(name: str) -> str:
    return quote(name, safe="")


def _domain_segments(domain_id: str | None) -> str:
    """The domain path portion of a URI. Hierarchical domains arrive as slash-separated ids;
    the unassigned ('') bucket contributes no segments."""
    if not domain_id:
        return ""
    return "/".join(_segment(part) for part in domain_id.split("/")) + "/"


def domain_uri(org_id: str, domain_id: str) -> str:  # REQ-1385
    return f"{_URI_SCHEME}://{_segment(org_id)}/{_domain_segments(domain_id)}".rstrip("/")


def source_uri(org_id: str, source_id: str) -> str:  # REQ-1385
    return f"{_URI_SCHEME}://{_segment(org_id)}/sources/{_segment(source_id)}"


def command_uri(org_id: str, command_name: str) -> str:  # REQ-1385
    """A governed command (tracked function/webhook) by its registered name."""
    return f"{_URI_SCHEME}://{_segment(org_id)}/commands/{_segment(command_name)}"


def table_uri(org_id: str, table: Table) -> str:  # REQ-1385
    """Business-identity address: alias when present, else table name — never the physical
    (source, schema) coordinates, which export separately as the binding."""
    business_name = table.alias or table.table_name
    return (
        f"{_URI_SCHEME}://{_segment(org_id)}/"
        f"{_domain_segments(table.domain_id)}tables/{_segment(business_name)}"
    )


def column_uri(org_id: str, table: Table, column_name: str, column_alias: str | None) -> str:
    # REQ-1385: #field:<business name> — a column is an attribute of the concept.
    return f"{table_uri(org_id, table)}#field:{_segment(column_alias or column_name)}"


def term_uri(org_id: str, term_name: str) -> str:  # REQ-1385, REQ-1387
    """A business-glossary term by its (unique) normalized name."""
    return f"{_URI_SCHEME}://{_segment(org_id)}/terms/{_segment(term_name)}"


def relationship_uri(org_id: str, source_table: Table, alias: str | None, rel_id: str) -> str:
    # REQ-1385: a relationship is a navigational field anchored at its source concept.
    # Its business name is the alias when the edge is named; the registry id otherwise.
    return f"{table_uri(org_id, source_table)}#rel:{_segment(alias or rel_id)}"


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

    def __init__(self, tables: list[Table]) -> None:
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
