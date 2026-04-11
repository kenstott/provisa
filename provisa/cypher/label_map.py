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
    catalog_name: str
    schema_name: str
    table_name: str
    properties: dict[str, str]  # cypher prop name → SQL column name


@dataclass
class RelationshipMapping:
    rel_type: str          # Cypher relationship type (UPPER_SNAKE)
    source_label: str
    target_label: str
    join_source_column: str
    join_target_column: str
    field_name: str        # GraphQL field name that defines this join


class CypherLabelMap:
    """Graph schema derived from registered Provisa tables and relationships."""

    def __init__(
        self,
        nodes: dict[str, NodeMapping],
        relationships: dict[str, RelationshipMapping],
        domains: dict[str, list[str]] | None = None,
        nodes_by_table: dict[str, list[str]] | None = None,
    ) -> None:
        self.nodes = nodes
        # keyed by rel_type (can map multiple if different source/target pairs)
        self.relationships = relationships
        # domain_label (PascalCase) → [type_name, ...]
        self.domains: dict[str, list[str]] = domains or {}
        # table_label (PascalCase) → [type_name, ...]
        self.nodes_by_table: dict[str, list[str]] = nodes_by_table or {}

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
    ) -> list[list[RelationshipMapping]]:
        """BFS over relationship schema graph.

        Returns all paths from start_label to end_label within max_hops.
        Each path is a list of RelationshipMapping (one per hop).
        Cycle-free within each path; bounded by max_hops.
        """
        results: list[list[RelationshipMapping]] = []
        # queue: (current_label, path_so_far, used_rel_types)
        # Cypher path semantics: no repeated relationships within a path.
        # Tracking used rel_types per path prevents cycles (A→B→A→B→...)
        # while allowing the same node label to appear more than once
        # (A→B→A is valid: two distinct A rows joined through B).
        queue: list[tuple[str, list[RelationshipMapping], frozenset[str]]] = [
            (start_label, [], frozenset())
        ]
        while queue:
            cur_label, path, used_rels = queue.pop(0)
            if cur_label == end_label and path:
                results.append(list(path))
                continue  # don't expand further from end_label
            if len(path) >= max_hops:
                continue
            for rel in self.relationships.values():
                if rel.source_label != cur_label:
                    continue
                if rel_types is not None and rel.rel_type not in rel_types:
                    continue
                if rel.rel_type in used_rels:
                    continue  # no repeated edges
                queue.append((rel.target_label, path + [rel], used_rels | {rel.rel_type}))
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

        # Build target_pk_columns: type_name → target_column from any JoinMeta
        # where this type appears as the join target.  The target_column is the
        # PK (or unique key) used on that side of the join — the most reliable
        # source of truth available without a separate schema introspection call.
        target_pk: dict[str, str] = {}
        for join_meta in ctx_typed.joins.values():
            tname = join_meta.target.type_name
            if tname not in target_pk:
                target_pk[tname] = join_meta.target_column

        # Build node mappings from table metadata
        # Skip _connection and _aggregate synthetic variants registered for GraphQL pagination
        for field_name, table_meta in ctx_typed.tables.items():
            if field_name.endswith("_connection") or field_name.endswith("_aggregate"):
                continue
            col_list = ctx_typed.aggregate_columns.get(table_meta.table_id, [])
            col_names = [c for c, _ in col_list]
            id_col = _resolve_id_column(table_meta.type_name, col_names, target_pk)
            props: dict[str, str] = {c: c for c in col_names}

            domain_label, table_label = _split_cypher_labels(field_name)
            cypher_label = f"{domain_label}:{table_label}" if domain_label else table_label

            nodes[table_meta.type_name] = NodeMapping(
                label=cypher_label,
                type_name=table_meta.type_name,
                domain_label=domain_label,
                table_label=table_label,
                table_id=table_meta.table_id,
                source_id=table_meta.source_id,
                id_column=id_col,
                catalog_name=table_meta.catalog_name,
                schema_name=table_meta.schema_name,
                table_name=table_meta.table_name,
                properties=props,
            )

            # Populate domain index
            if domain_label:
                domains.setdefault(domain_label, []).append(table_meta.type_name)

            # Populate table index
            nodes_by_table.setdefault(table_label, []).append(table_meta.type_name)

        # Build relationship mappings from join metadata
        for (source_type_name, gql_field_name), join_meta in ctx_typed.joins.items():
            rel_type = _to_rel_type(gql_field_name)
            relationships[rel_type] = RelationshipMapping(
                rel_type=rel_type,
                source_label=source_type_name,
                target_label=join_meta.target.type_name,
                join_source_column=join_meta.source_column,
                join_target_column=join_meta.target_column,
                field_name=gql_field_name,
            )

        return cls(nodes=nodes, relationships=relationships, domains=domains, nodes_by_table=nodes_by_table)


_ID_EXACT = {"id", "_id", "pk", "oid"}
_ID_SUFFIX = ("_id", "_pk", "_oid")
_ID_PREFIX = ("id_",)


def _resolve_id_column(
    type_name: str,
    col_names: list[str],
    target_pk: dict[str, str],
) -> str:
    """Return the primary-key column name for a node type.

    Resolution order (first match wins):
    1. The column named in a JoinMeta.target_column for this type — explicit FK target.
    2. Exact match against known id names: id, _id, pk, oid.
    3. Single column ending in _id / _pk / _oid (unambiguous).
    4. Single column starting with id_.
    5. First column in the column list.
    6. Fallback: "id".
    """
    # 1. Explicit join target
    if type_name in target_pk:
        return target_pk[type_name]

    # 2. Exact known names (preserve declaration order)
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


def _split_cypher_labels(field_name: str) -> tuple[str | None, str]:
    """Derive (domain_label, table_label) from a GQL field name.

    "sales_analytics__orders" → ("SalesAnalytics", "Orders")
    "orders"                  → (None, "Orders")
    """
    import re

    def _pascal(s: str) -> str:
        return "".join(p.capitalize() for p in re.split(r"[_\-]+", s) if p)

    if "__" in field_name:
        domain_part, table_part = field_name.split("__", 1)
        return _pascal(domain_part), _pascal(table_part)
    return None, _pascal(field_name)
