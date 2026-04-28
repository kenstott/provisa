# Copyright (c) 2026 Kenneth Stott
# Canary: 9d3f5a2c-7b1e-4c8d-a2f6-3e5b7d9f1c4a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""CypherLabelMap — derive graph schema from CompilationContext.

No separate config. TableMeta.type_name → node label; JoinMeta → relationship type.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class NodeMapping:
    label: str            # Cypher label string, e.g. "SalesAnalytics:Orders" or "Orders"
    type_name: str        # internal lookup key, e.g. "SalesAnalytics_Orders"
    domain_label: str | None  # PascalCase domain part, e.g. "SalesAnalytics"; None if no domain
    table_label: str      # PascalCase table part, e.g. "Orders"
    table_id: int
    source_id: str
    id_column: str        # primary key column (first column if no explicit pk)
    pk_columns: list[str]  # user-designated PK columns (informational; empty = heuristic only)
    catalog_name: str
    schema_name: str
    table_name: str          # logical name — domain initials prefix stripped (e.g. "orders")
    properties: dict[str, str]  # cypher prop name → SQL column name
    native_filter_columns: set[str] = field(default_factory=set)  # SQL column names that are native API params
    physical_table_name: str = ""  # physical DB table name; "" means same as table_name

    @property
    def sql_table_name(self) -> str:
        return self.physical_table_name or self.table_name


@dataclass
class RelationshipMapping:
    rel_type: str          # Cypher relationship type (UPPER_SNAKE)
    source_label: str
    target_label: str
    join_source_column: str
    join_target_column: str
    field_name: str        # GraphQL field name that defines this join
    alias: str | None = None  # relationship alias from config (e.g. WORKS_FOR)
    source_constant: int | None = None  # when set, use as literal join value instead of source column


class CypherLabelMap:
    """Graph schema derived from registered Provisa tables and relationships."""

    def __init__(
        self,
        nodes: dict[str, NodeMapping],
        relationships: dict[str, RelationshipMapping],
        domains: dict[str, list[str]] | None = None,
        nodes_by_table: dict[str, list[str]] | None = None,
        aliases: dict[str, list[RelationshipMapping]] | None = None,
    ) -> None:
        self.nodes = nodes
        # keyed by rel_type (can map multiple if different source/target pairs)
        self.relationships = relationships
        # domain_label (PascalCase) → [type_name, ...]
        self.domains: dict[str, list[str]] = domains or {}
        # table_label (PascalCase) → [type_name, ...]
        self.nodes_by_table: dict[str, list[str]] = nodes_by_table or {}
        # rel_type → all RelationshipMappings with that type (supports UNION fan-out)
        self.aliases: dict[str, list[RelationshipMapping]] = aliases or {}
        # case-insensitive lookup indexes: lowercase → canonical key
        self._nodes_ci: dict[str, str] = {k.lower(): k for k in self.nodes}
        self._domains_ci: dict[str, str] = {k.lower(): k for k in self.domains}
        self._nodes_by_table_ci: dict[str, str] = {k.lower(): k for k in self.nodes_by_table}

    def display_label(self, nm: "NodeMapping") -> str:
        """Return the shortest unambiguous label for a node.

        Uses just the table label unless multiple nodes share that table label
        across different domains, in which case the full compound label is needed.
        """
        if len(self.nodes_by_table.get(nm.table_label, [])) > 1:
            return nm.label
        return nm.table_label

    def canonical_label(self, label: str) -> str:
        """Return the canonical-cased label, falling back to input if not found."""
        return (
            self._nodes_ci.get(label.lower())
            or self._domains_ci.get(label.lower())
            or self._nodes_by_table_ci.get(label.lower())
            or label
        )

    def node(self, label: str) -> NodeMapping:
        try:
            return self.nodes[label]
        except KeyError:
            raise KeyError(f"Unknown Cypher node label: {label!r}")

    def find_paths(
        self,
        start_label: str,
        end_label: str,
        rel_types: list[str] | None = None,
        max_hops: int = 10,
        bidirectional: bool = False,
    ) -> list[list[RelationshipMapping]]:
        """BFS over relationship schema graph.

        Returns all paths from start_label to end_label within max_hops.
        Each path is a list of RelationshipMapping (one per hop).
        Cycle-free within each path; bounded by max_hops.
        When bidirectional=True, edges may also be traversed in reverse
        (undirected pattern semantics); reversed edges have source/target
        and join columns swapped so downstream JOIN generation is unchanged.
        """
        results: list[list[RelationshipMapping]] = []
        # queue: (current_label, path_so_far, used_rel_keys)
        # Cypher path semantics: no repeated relationships within a path.
        # key = (rel_type, forward|reverse) to avoid traversing the same
        # physical edge twice in one path regardless of direction.
        queue: list[tuple[str, list[RelationshipMapping], frozenset[str]]] = [
            (start_label, [], frozenset())
        ]
        while queue:
            cur_label, path, used_rel_keys = queue.pop(0)
            if cur_label == end_label and path:
                results.append(list(path))
                continue  # don't expand further from end_label
            if len(path) >= max_hops:
                continue
            for rel in self.relationships.values():
                if rel_types is not None and rel.rel_type not in rel_types:
                    continue
                # Forward edge
                if rel.source_label == cur_label:
                    key = f"{rel.rel_type}:fwd"
                    if key not in used_rel_keys:
                        queue.append((rel.target_label, path + [rel], used_rel_keys | {key}))
                # Reverse edge (only when bidirectional)
                if bidirectional and rel.target_label == cur_label:
                    key = f"{rel.rel_type}:rev"
                    if key not in used_rel_keys:
                        rev = RelationshipMapping(
                            rel_type=rel.rel_type,
                            source_label=rel.target_label,
                            target_label=rel.source_label,
                            join_source_column=rel.join_target_column,
                            join_target_column=rel.join_source_column,
                            field_name=rel.field_name,
                            alias=rel.alias,
                        )
                        queue.append((rel.source_label, path + [rev], used_rel_keys | {key}))
        return results

    def relationships_for(self, source_label: str, target_label: str | None = None) -> list[RelationshipMapping]:
        result = []
        for rel in self.relationships.values():
            if rel.source_label == source_label:
                if target_label is None or rel.target_label == target_label:
                    result.append(rel)
        return result

    @classmethod
    def from_schema(cls, ctx: object) -> "CypherLabelMap":
        """Build CypherLabelMap from an existing CompilationContext."""
        from provisa.compiler.sql_gen import CompilationContext, TableMeta, JoinMeta

        nodes: dict[str, NodeMapping] = {}
        relationships: dict[str, RelationshipMapping] = {}
        domains: dict[str, list[str]] = {}
        nodes_by_table: dict[str, list[str]] = {}

        ctx_typed: CompilationContext = ctx  # type: ignore[assignment]

        # Build target_pk_columns: type_name → target_column, but ONLY for many-to-one
        # joins. On a many-to-one join the target column is the PK/unique key of the
        # "one" side. On a one-to-many join the target column is a FK in the "many"
        # table and must not be mistaken for that table's primary key.
        target_pk: dict[str, str] = {}
        for join_meta in ctx_typed.joins.values():
            tname = join_meta.target.type_name
            if tname not in target_pk and getattr(join_meta, "cardinality", None) == "many-to-one":
                target_pk[tname] = join_meta.target_column

        # Build node mappings from table metadata
        # Skip _connection and _aggregate synthetic variants registered for GraphQL pagination
        for field_name, table_meta in ctx_typed.tables.items():
            if field_name.endswith("_connection") or field_name.endswith("_aggregate"):
                continue
            col_list = ctx_typed.aggregate_columns.get(table_meta.table_id, [])
            col_names = [c for c, _ in col_list]
            user_pks = ctx_typed.pk_columns.get(table_meta.table_id, [])
            id_col = _resolve_id_column(table_meta.type_name, col_names, target_pk, user_pks)
            props: dict[str, str] = {_to_camel(c): c for c in col_names}

            domain_id = getattr(table_meta, "domain_id", None) or None
            domain_label = _pascal(domain_id) if domain_id else None
            _, table_label = _split_cypher_labels(field_name)
            cypher_label = f"{domain_label}:{table_label}" if domain_label else table_label
            # logical table name: domain initials prefix stripped (lowercase of table_label parts)
            logical_table = _strip_domain_prefix(table_meta.table_name, domain_id)
            physical_table = table_meta.table_name
            physical_kwarg = {"physical_table_name": physical_table} if physical_table != logical_table else {}

            nf_cols = ctx_typed.native_filter_columns.get(table_meta.table_id, set())
            nodes[table_meta.type_name] = NodeMapping(
                label=cypher_label,
                type_name=table_meta.type_name,
                domain_label=domain_label,
                table_label=table_label,
                table_id=table_meta.table_id,
                source_id=table_meta.source_id,
                id_column=id_col,
                pk_columns=user_pks,
                catalog_name=table_meta.catalog_name,
                schema_name=table_meta.schema_name,
                table_name=logical_table,
                properties=props,
                native_filter_columns=nf_cols,
                **physical_kwarg,
            )

            # Populate domain index
            if domain_label:
                domains.setdefault(domain_label, []).append(table_meta.type_name)

            # Populate table index
            nodes_by_table.setdefault(table_label, []).append(table_meta.type_name)

        # Build relationship mappings from join metadata
        aliases: dict[str, list[RelationshipMapping]] = {}
        for (source_type_name, gql_field_name), join_meta in ctx_typed.joins.items():
            if getattr(join_meta, "disable_cypher", False):
                continue
            # Cypher rel type: use explicit alias (e.g. OPENED_BY) else derive from GraphQL field name
            cypher_alias = getattr(join_meta, "cypher_alias", None)
            rel_type = cypher_alias if cypher_alias else _to_rel_type(gql_field_name)
            rm = RelationshipMapping(
                rel_type=rel_type,
                source_label=source_type_name,
                target_label=join_meta.target.type_name,
                join_source_column=join_meta.source_column,
                join_target_column=join_meta.target_column,
                field_name=gql_field_name,
                alias=cypher_alias,
            )
            relationships[rel_type] = rm
            aliases.setdefault(rel_type, []).append(rm)

        # Add REGISTERED_TABLE edges: every non-meta user node → Meta:RegisteredTables
        meta_rt = next(
            (nm for nm in nodes.values() if nm.domain_label == "Meta" and nm.table_name == "registered_tables"),
            None,
        )
        if meta_rt:
            for type_name, nm in list(nodes.items()):
                if nm.domain_label == "Meta":
                    continue
                rm = RelationshipMapping(
                    rel_type="REGISTERED_TABLE",
                    source_label=type_name,
                    target_label=meta_rt.type_name,
                    join_source_column="__table_id__",
                    join_target_column="id",
                    field_name="_meta",
                    source_constant=nm.table_id,
                )
                relationships[f"REGISTERED_TABLE::{type_name}"] = rm
                aliases.setdefault("REGISTERED_TABLE", []).append(rm)

        return cls(nodes=nodes, relationships=relationships, domains=domains, nodes_by_table=nodes_by_table, aliases=aliases)


_ID_EXACT = {"id", "_id", "pk", "oid"}
_ID_SUFFIX = ("_id", "_pk", "_oid")
_ID_PREFIX = ("id_",)


def _resolve_id_column(
    type_name: str,
    col_names: list[str],
    target_pk: dict[str, str],
    user_pks: list[str] | None = None,
) -> str:
    """Return the primary-key column name for a node type.

    Resolution order (first match wins):
    0. User-designated PK columns (first entry if multiple).
    1. The column named in a JoinMeta.target_column — only set for many-to-one joins
       where the target column is the actual PK of the target table.
    2. Exact match against known id names: id, _id, pk, oid.
    3. Single column ending in _id / _pk / _oid (unambiguous).
    4. Single column starting with id_.
    5. First column in the column list.
    6. Fallback: "id".
    """
    # 0. User-designated PK
    if user_pks:
        return user_pks[0]

    # 1. Explicit join target — only populated for many-to-one cardinality, so
    # target_column is the actual PK (not a FK from a one-to-many join).
    if type_name in target_pk:
        return target_pk[type_name]

    # 2. Exact known names
    for col in col_names:
        if col.lower() in _ID_EXACT:
            return col

    # 3. Unambiguous suffix match
    suffix_matches = [c for c in col_names if c.lower().endswith(_ID_SUFFIX)]
    if len(suffix_matches) == 1:
        return suffix_matches[0]

    # 4. Unambiguous prefix match
    prefix_matches = [c for c in col_names if c.lower().startswith(_ID_PREFIX)]
    if len(prefix_matches) == 1:
        return prefix_matches[0]

    # 5. First column
    if col_names:
        return col_names[0]

    # 6. Hard fallback
    return "id"


def _to_rel_type(field_name: str) -> str:
    """Convert a snake_case GraphQL field name to UPPER_SNAKE relationship type."""
    return field_name.upper()


def _pascal(s: str) -> str:
    parts = [p for p in re.split(r"[_\-]+", s) if p]
    if len(parts) == 1:
        # No separators: uppercase first letter only, preserving existing casing.
        return (s[0].upper() + s[1:]) if s else s
    return "".join(p.capitalize() for p in parts)


def _to_camel(s: str) -> str:
    """Convert snake_case column name to camelCase Cypher property name."""
    pascal = _pascal(s)
    return pascal[0].lower() + pascal[1:] if pascal else s


def _domain_initials(domain_id: str) -> str:
    """Return lowercase initials of a domain_id (first letter of each word segment).

    "sales_analytics" → "sa", "human-resources" → "hr"
    """
    parts = re.split(r"[^a-zA-Z0-9]+", domain_id)
    return "".join(p[0] for p in parts if p and p[0].isalpha()).lower()


def _strip_domain_prefix(table_name: str, domain_id: str | None) -> str:
    """Strip domain initials prefix from table_name, returning the raw (lowercase) logical name.

    "sa_orders"  (domain "sales_analytics", initials "sa") → "orders"
    "orders"     (no domain or no matching prefix)          → "orders"
    """
    if domain_id:
        prefix = _domain_initials(domain_id) + "_"
        if table_name.lower().startswith(prefix):
            return table_name[len(prefix):]
    return table_name


def _table_label_from_table_name(table_name: str, domain_id: str | None) -> str:
    """Derive PascalCase table label by stripping domain initials prefix.

    "sa_orders"  (domain "sales_analytics", initials "sa") → "Orders"
    "orders"     (no domain or no matching prefix)          → "Orders"
    """
    if domain_id:
        prefix = _domain_initials(domain_id) + "_"
        if table_name.lower().startswith(prefix):
            table_name = table_name[len(prefix):]
    return _pascal(table_name)


def _split_cypher_labels(field_name: str) -> tuple[str | None, str]:
    """Derive (domain_label, table_label) from a GQL field name.

    "sales_analytics__orders" → ("SalesAnalytics", "Orders")
    "orders"                  → (None, "Orders")
    """
    if "__" in field_name:
        domain_part, table_part = field_name.split("__", 1)
        return _pascal(domain_part), _pascal(table_part)
    return None, _pascal(field_name)
