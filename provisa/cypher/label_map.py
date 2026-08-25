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
from typing import TYPE_CHECKING

from provisa.compiler.naming import (
    apply_cql_label as _apply_cql_label,
    apply_cql_property as _apply_cql_property,
)
from provisa.compiler.sql_types import key_list
from provisa.core import domain_policy

if TYPE_CHECKING:
    from provisa.compiler.sql_gen import CompilationContext
    from provisa.compiler.sql_types import JunctionMeta

# Requirements: REQ-351, REQ-392, REQ-394, REQ-467, REQ-471, REQ-574

# Catalog domain id — local constant (kept in sync with provisa.security.rights.META_DOMAIN_ID) to
# avoid importing a higher layer into the cypher package.
_META_DOMAIN_ID = "meta"

# REQ-1320: star-schema modeling role → the additional node label Neo4j clients see.
_ROLE_LABELS = {"fact": "Fact", "dimension": "Dimension"}


@dataclass
class NodeMapping:
    label: str  # Cypher label string, e.g. "SalesAnalytics:Orders" or "Orders"
    type_name: str  # internal lookup key, e.g. "SalesAnalytics_Orders"
    domain_label: str | None  # PascalCase domain part, e.g. "SalesAnalytics"; None if no domain
    table_label: str  # PascalCase table part, e.g. "Orders"
    table_id: int
    source_id: str
    id_column: str  # primary key column (first column if no explicit pk)
    pk_columns: list[str]  # user-designated PK columns (informational; empty = heuristic only)
    catalog_name: str
    schema_name: str
    table_name: str  # logical name — domain initials prefix stripped (e.g. "orders")
    properties: dict[
        str, str
    ]  # cypher prop name → SQL alias name; used everywhere in generated SQL (WHERE/ON/SELECT)
    physical_properties: dict[str, str] = field(
        default_factory=dict
    )  # cypher prop name → physical DB column name; used ONLY in _node_table_expr to build
    # the SELECT *, phys AS sql_alias subquery wrapper — never in WHERE/ON conditions
    native_filter_columns: dict[str, str] = field(
        default_factory=dict
    )  # SQL column name → data_type for native API params
    physical_table_name: str = ""  # physical DB table name; "" means same as table_name
    traversal_only: bool = False  # True = cross-domain node; may not be a MATCH starting node
    domain_id: str | None = None  # raw domain id, e.g. "pet-store"; None if no domain
    modeling_role: str | None = None  # REQ-1320: "fact" | "dimension" | None (star-schema role)

    @property
    def sql_table_name(self) -> str:
        return self.physical_table_name or self.table_name

    @property
    def role_label(self) -> str | None:  # REQ-1320
        """Additional node label for the table's star-schema role: Fact / Dimension."""
        return _ROLE_LABELS.get(self.modeling_role or "")


@dataclass
class JunctionMapping:  # REQ-1586
    """The associative table a junction-backed relationship traverses through.

    ``source_columns``/``target_columns`` are the junction's own two foreign keys, each an ordered
    list of one or more columns, paired positionally against the relationship's own source and
    target key lists. ``type_column``/``type_value`` add the
    discriminator predicate for a junction carrying several relationship types. ``attributes``
    maps Cypher property name to physical column for every other junction column — those are the
    relationship's attributes, readable as ``r.attr`` and filterable in WHERE.
    """

    catalog_name: str
    schema_name: str
    table_name: str
    # REQ-1586: the node type name the junction table WOULD have occupied — the key
    # _drop_junction_nodes removes from `nodes`, and with it every FK edge that named it.
    type_name: str
    source_columns: tuple[str, ...]
    target_columns: tuple[str, ...]
    type_column: str | None = None
    type_value: str | None = None
    attributes: dict[str, str] = field(default_factory=dict)
    # REQ-1586: which nomination names the exposed type — see junction_rel_type.
    label_source: str = ""
    label_fixed: str = ""


@dataclass
class RelationshipMapping:
    rel_type: str  # Cypher relationship type (UPPER_SNAKE)
    source_label: str
    target_label: str
    join_source_column: str
    join_target_column: str
    field_name: str  # GraphQL field name that defines this join
    alias: str | None = None  # relationship alias from config (e.g. WORKS_FOR)
    source_constant: int | str | None = (
        None  # when set, use as literal join value instead of source column
    )
    source_expr: str | None = (
        None  # when set, use as raw SQL expression on source side; {alias} replaced with join alias
    )
    target_expr: str | None = (
        None  # when set, use as raw SQL expression on target side; {alias} replaced with join alias
    )
    many: bool = False  # True when cardinality is one-to-many (source is parent, target is array)
    via: JunctionMapping | None = None  # REQ-1586: set on a junction-backed edge, None on FK/PK

    @property
    def properties(self) -> dict[str, str]:
        """REQ-1586: the relationship's attributes. Empty unless it is junction-backed."""
        return self.via.attributes if self.via else {}


class CypherLabelMap:  # REQ-351, REQ-392, REQ-574
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

    def display_label(self, nm: "NodeMapping") -> str:  # REQ-572
        """Return the shortest unambiguous label for a node.

        Uses just the table label unless multiple nodes share that table label
        across different domains, in which case the full compound label is needed.
        """
        if len(self.nodes_by_table.get(nm.table_label, [])) > 1:
            return nm.label
        return nm.table_label

    def canonical_label(self, label: str) -> str:  # REQ-572
        """Return the canonical-cased label, falling back to input if not found."""
        return (
            self._nodes_ci.get(label.lower())
            or self._domains_ci.get(label.lower())
            or self._nodes_by_table_ci.get(label.lower())
            or label
        )

    def node(self, label: str) -> NodeMapping:  # REQ-572
        try:
            return self.nodes[label]
        except KeyError:
            raise KeyError(f"Unknown Cypher node label: {label!r}")

    def find_paths(  # REQ-572
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
                # Exclude synthetic constant-join rels (e.g. HAS_TABLE) from
                # implicit traversal — they anchor every data row to a single
                # meta row via a constant, not a real FK, so they cannot serve
                # as intermediate hops without contradictory join conditions.
                if rel_types is None and rel.source_constant is not None:
                    continue
                # Forward edge
                if rel.source_label == cur_label:
                    key = f"{rel.rel_type}:fwd"
                    if key not in used_rel_keys:
                        tgt_nm = self.nodes.get(rel.target_label)
                        if tgt_nm and tgt_nm.native_filter_columns:
                            continue
                        queue.append((rel.target_label, path + [rel], used_rel_keys | {key}))
                # Reverse edge (only when bidirectional)
                if bidirectional and rel.target_label == cur_label:
                    key = f"{rel.rel_type}:rev"
                    if key not in used_rel_keys:
                        src_nm = self.nodes.get(rel.source_label)
                        if src_nm and src_nm.native_filter_columns:
                            continue
                        # When forward uses source_constant + target_expr, the reverse
                        # must express: target_expr(source) = source_constant, not
                        # join_target_column = join_source_column (which is synthetic).
                        if rel.source_constant is not None and rel.target_expr is not None:
                            escaped = str(rel.source_constant).replace("'", "''")
                            rev = RelationshipMapping(
                                rel_type=rel.rel_type,
                                source_label=rel.target_label,
                                target_label=rel.source_label,
                                join_source_column=rel.join_target_column,
                                join_target_column=rel.join_source_column,
                                field_name=rel.field_name,
                                alias=rel.alias,
                                source_expr=rel.target_expr,
                                target_expr=f"'{escaped}'",
                            )
                        else:
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

    def relationships_for(  # REQ-572
        self, source_label: str, target_label: str | None = None
    ) -> list[RelationshipMapping]:
        result = []
        for rel in self.relationships.values():
            if rel.source_label == source_label:
                if target_label is None or rel.target_label == target_label:
                    result.append(rel)
        return result

    @classmethod
    def from_schema(  # REQ-351, REQ-471
        cls,
        ctx: object,  # object-ok: circular import boundary — CompilationContext imported inside method body
        domain_access: list[str] | None = None,
        all_tables: list[dict] | None = None,
        all_relationships: list[dict] | None = None,
        all_column_types: dict | None = None,
        source_catalogs: dict[str, str] | None = None,
    ) -> "CypherLabelMap":
        """Build CypherLabelMap from an existing CompilationContext.

        When domain_access/all_tables/all_relationships/all_column_types are supplied,
        cross-domain nodes reachable via registered relationships are included and
        marked traversal_only=True — they cannot be used as MATCH starting nodes.
        """

        nodes: dict[str, NodeMapping] = {}
        relationships: dict[str, RelationshipMapping] = {}
        domains: dict[str, list[str]] = {}
        nodes_by_table: dict[str, list[str]] = {}

        ctx_typed: CompilationContext = ctx  # type: ignore[assignment]

        target_pk = _build_target_pk(ctx_typed)
        _build_node_mappings(ctx_typed, target_pk, nodes, domains, nodes_by_table)
        aliases = _build_relationship_mappings(ctx_typed, relationships)

        _all_access = domain_access is not None and "*" in domain_access
        if (
            not _all_access
            and all_tables is not None
            and all_relationships is not None
            and all_column_types is not None
        ):
            _add_cross_domain_nodes(
                all_tables,
                all_relationships,
                all_column_types,
                source_catalogs,
                nodes,
                relationships,
                domains,
                nodes_by_table,
                aliases,
            )

        # REQ-1586: after cross-domain expansion, so a junction reached from another domain is
        # dropped too rather than re-entering as a traversal node.
        _drop_junction_nodes(nodes, relationships, domains, nodes_by_table, aliases)

        # REQ-1320: tag every node with its table's star-schema modeling role so role-tagged
        # tables additionally expose the Fact/Dimension labels to Neo4j clients. all_tables is
        # the raw table registry (schema_build_cache) — the only place modeling_role lives.
        if all_tables is not None:
            _roles_by_id = {t["id"]: t.get("modeling_role") for t in all_tables}
            for nm in nodes.values():
                nm.modeling_role = _roles_by_id.get(nm.table_id) or nm.modeling_role

        # REQ-1132: meta (catalog) is DISCOVERABLE ONLY BY TRAVERSAL for a role without a meta grant
        # — it may not be a bare MATCH (n) root (which would emit a direct meta FROM and be V001-blocked,
        # the same rule SQL enforces). Mark meta nodes traversal_only so MATCH (mine)-[]->(meta) still
        # works while MATCH (n) roots on the role's own domains only. A meta grant / "*" keeps it direct.
        if (
            domain_access is not None
            and "*" not in domain_access
            and _META_DOMAIN_ID not in domain_access
        ):
            for nm in nodes.values():
                if nm.domain_id == _META_DOMAIN_ID:
                    nm.traversal_only = True

        return cls(
            nodes=nodes,
            relationships=relationships,
            domains=domains,
            nodes_by_table=nodes_by_table,
            aliases=aliases,
        )


def _drop_junction_nodes(  # REQ-1586
    nodes: dict[str, NodeMapping],
    relationships: dict[str, RelationshipMapping],
    domains: dict[str, list[str]],
    nodes_by_table: dict[str, list[str]],
    aliases: dict[str, list[RelationshipMapping]],
) -> None:
    """Remove every declared junction table from the node side of the Cypher schema.

    A declared junction is an edge, not an entity: it must not appear as a label a pattern can
    match, as a pill a client can drag onto a canvas, or under a domain's label list. It stays a
    registered table and is still queryable in SQL and GraphQL — this drops it from the graph
    schema only.

    The junction's own foreign keys also produced ordinary FK edges into and out of it. Those
    edges are what the junction edge replaces, and they name a label that no longer exists, so
    they go with the node — otherwise any consumer resolving an endpoint label (the graph-schema
    endpoint, a label count) reads a key that was just removed. Mutates every dict passed in.
    """
    junction_types = {rm.via.type_name for rm in relationships.values() if rm.via is not None}
    if not junction_types:
        return
    for rel_key, rm in list(relationships.items()):
        if rm.via is not None:
            continue
        if rm.source_label in junction_types or rm.target_label in junction_types:
            del relationships[rel_key]
            surviving = [r for r in aliases.get(rm.rel_type, []) if r is not rm]
            if surviving:
                aliases[rm.rel_type] = surviving
            else:
                aliases.pop(rm.rel_type, None)
    for type_name in junction_types:
        nm = nodes.pop(type_name, None)
        if nm is None:
            continue
        if nm.domain_label and nm.domain_label in domains:
            domains[nm.domain_label] = [t for t in domains[nm.domain_label] if t != type_name]
            if not domains[nm.domain_label]:
                del domains[nm.domain_label]
        if nm.table_label in nodes_by_table:
            nodes_by_table[nm.table_label] = [
                t for t in nodes_by_table[nm.table_label] if t != type_name
            ]
            if not nodes_by_table[nm.table_label]:
                del nodes_by_table[nm.table_label]


def _build_target_pk(ctx_typed: "CompilationContext") -> dict[str, str]:
    """Return type_name → target_column for many-to-one joins only."""
    target_pk: dict[str, str] = {}
    for join_meta in ctx_typed.joins.values():
        tname = join_meta.target.type_name
        if tname not in target_pk and getattr(join_meta, "cardinality", None) == "many-to-one":
            target_pk[tname] = join_meta.target_column
    return target_pk


def _build_node_mappings(
    ctx_typed: "CompilationContext",
    target_pk: dict[str, str],
    nodes: dict[str, NodeMapping],
    domains: dict[str, list[str]],
    nodes_by_table: dict[str, list[str]],
) -> None:
    """Populate nodes/domains/nodes_by_table from ctx tables. Mutates all three dicts."""
    for field_name, table_meta in ctx_typed.tables.items():
        if (
            field_name.endswith("_connection")
            or field_name.endswith("_aggregate")
            or field_name.endswith("_group_by")
            or field_name.endswith("GroupBy")
            or field_name.endswith("Aggregate")
        ):
            continue
        col_list = ctx_typed.aggregate_columns.get(table_meta.table_id, [])
        col_names = [c for c, _ in col_list]
        user_pks = ctx_typed.pk_columns.get(table_meta.table_id, [])
        _phys_id = _resolve_id_column(table_meta.type_name, col_names, target_pk, user_pks)
        id_col = ctx_typed.physical_to_sql.get((table_meta.table_id, _phys_id), _phys_id)
        _gov_obj = ctx_typed.gql_governed_object_cols
        props: dict[str, str] = {
            _apply_cql_property(c): ctx_typed.physical_to_sql.get((table_meta.table_id, c), c)
            for c in col_names
            if (table_meta.table_id, c) not in _gov_obj
        }
        phys_props: dict[str, str] = {
            _apply_cql_property(c): c for c in col_names if (table_meta.table_id, c) not in _gov_obj
        }

        domain_id = getattr(table_meta, "domain_id", None) or None
        if domain_policy.single_domain():
            # Single-domain mode: the default domain is implicit — never label with it.
            domain_id = None
        domain_label = _apply_cql_label(domain_id) if domain_id else None
        _, table_label = _split_cypher_labels(field_name)
        cypher_label = f"{domain_label}:{table_label}" if domain_label else table_label
        logical_table = _strip_domain_prefix(table_meta.table_name, domain_id)
        physical_table = table_meta.table_name
        physical_table_name = physical_table if physical_table != logical_table else ""

        nf_cols = ctx_typed.native_filter_columns.get(table_meta.table_id, {})
        nodes[table_meta.type_name] = NodeMapping(
            label=cypher_label,
            type_name=table_meta.type_name,
            domain_label=domain_label,
            domain_id=domain_id,
            table_label=table_label,
            table_id=table_meta.table_id,
            source_id=table_meta.source_id,
            id_column=id_col,
            pk_columns=user_pks,
            catalog_name=table_meta.catalog_name,
            schema_name=table_meta.schema_name,
            table_name=logical_table,
            properties=props,
            physical_properties=phys_props,
            native_filter_columns=nf_cols,
            physical_table_name=physical_table_name,
        )

        if domain_label:
            domains.setdefault(domain_label, []).append(table_meta.type_name)
        nodes_by_table.setdefault(table_label, []).append(table_meta.type_name)


def _upper_snake(text: str) -> str:  # REQ-1586
    """Normalise a nominated label to the UPPER_SNAKE form every Cypher relationship type takes."""
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    return re.sub(r"_+", "_", re.sub(r"[^A-Za-z0-9]+", "_", spaced)).strip("_").upper()


def junction_rel_type(via: JunctionMapping) -> str:  # REQ-1586
    """The Cypher type name of a junction-backed edge, from its nominated source.

    One of three nominations, upper-snake-cased: ``column`` takes the discriminator value the edge
    is pinned to, ``table`` takes the junction table's own name, ``fixed`` takes the cypher_alias
    declared on the relationship row. The registry CHECK constraints guarantee the nominated source
    is present, so an unknown or empty nomination is a declaration that never should have been
    stored, not something to name around.
    """
    if via.label_source == "column":
        if via.type_value is None:
            raise ValueError(f"Junction on {via.table_name!r} nominates a column with no value")
        return _upper_snake(via.type_value)
    if via.label_source == "table":
        return _upper_snake(via.table_name)
    if via.label_source == "fixed":
        return _upper_snake(via.label_fixed)
    raise ValueError(f"Junction on {via.table_name!r} has no label source nomination")


def _junction_mapping(via: "JunctionMeta | None") -> JunctionMapping | None:  # REQ-1586
    """Convert a compiler JunctionMeta into the Cypher-side JunctionMapping."""
    if via is None:
        return None
    return JunctionMapping(
        catalog_name=via.table.catalog_name,
        schema_name=via.table.schema_name,
        table_name=via.table.table_name,
        type_name=via.table.type_name,
        source_columns=via.source_columns,
        target_columns=via.target_columns,
        type_column=via.type_column,
        type_value=via.type_value,
        attributes=dict(via.attributes),
        label_source=via.label_source,
        label_fixed=via.label_fixed,
    )


def _build_relationship_mappings(
    ctx_typed: "CompilationContext",
    relationships: dict[str, RelationshipMapping],
) -> dict[str, list[RelationshipMapping]]:
    """Populate relationships from ctx joins; return aliases dict."""
    aliases: dict[str, list[RelationshipMapping]] = {}
    for (source_type_name, gql_field_name), join_meta in ctx_typed.joins.items():
        if getattr(join_meta, "disable_cypher", False):
            continue
        cypher_alias = getattr(join_meta, "cypher_alias", None)
        cardinality = getattr(join_meta, "cardinality", None)
        via = _junction_mapping(getattr(join_meta, "via", None))
        # REQ-1586: a junction edge is named by its nomination, not by the GraphQL field it would
        # have had — the field name describes the junction table, the nomination describes the edge.
        rel_type = (
            junction_rel_type(via)
            if via is not None
            else (cypher_alias if cypher_alias else _to_rel_type(gql_field_name, cardinality))
        )
        src_json_key = getattr(join_meta, "source_json_key", None)
        _base_source_expr = getattr(join_meta, "source_expr", None)
        source_expr = (
            f"JSON_EXTRACT_SCALAR({{alias}}.\"{join_meta.source_column}\", '$.{src_json_key}')"
            if src_json_key and _base_source_expr is None
            else _base_source_expr
        )
        rm = RelationshipMapping(
            rel_type=rel_type,
            source_label=source_type_name,
            target_label=join_meta.target.type_name,
            join_source_column=join_meta.source_column,
            join_target_column=join_meta.target_column,
            field_name=gql_field_name,
            alias=cypher_alias,
            source_constant=getattr(join_meta, "source_constant", None),
            source_expr=source_expr,
            target_expr=getattr(join_meta, "target_expr", None),
            many=(cardinality == "one-to-many"),
            via=via,
        )
        rel_key = f"{rel_type}::{source_type_name}→{join_meta.target.type_name}"
        relationships[rel_key] = rm
        aliases.setdefault(rel_type, []).append(rm)
    return aliases


def _via_type_name(raw_table_name: str, domain_id: str | None) -> str:  # REQ-1586
    """The node type name a table occupies — the same composition _make_traversal_node uses."""
    table_label = _apply_cql_label(_strip_domain_prefix(raw_table_name, domain_id))
    domain_label = _apply_cql_label(domain_id) if domain_id else None
    return f"{domain_label}_{table_label}" if domain_label else table_label


def _junction_from_rel_dict(  # REQ-1586
    rel: dict,
    all_tables_by_id: dict[int, dict],
    all_column_types: dict,
    source_catalogs: dict[str, str] | None,
) -> JunctionMapping | None:
    """Build a JunctionMapping from a raw relationship row on the cross-domain traversal path."""
    from provisa.compiler.naming import source_to_catalog as _s2c

    via_id = rel.get("via_table_id")
    if not via_id:
        return None
    via_table = all_tables_by_id[via_id]
    via_source_id = via_table["source_id"]
    via_domain_id = via_table.get("domain_id") or None
    src_keys = key_list(rel["via_source_column"])
    tgt_keys = key_list(rel["via_target_column"])
    keys = {*src_keys, *tgt_keys}
    type_column = rel.get("via_type_column") or None
    if type_column:
        keys.add(type_column)
    return JunctionMapping(
        catalog_name=(source_catalogs or {}).get(via_source_id) or _s2c(via_source_id),
        schema_name=via_table["schema_name"],
        table_name=_strip_domain_prefix(via_table["table_name"], via_domain_id),
        type_name=_via_type_name(via_table["table_name"], via_domain_id),
        source_columns=src_keys,
        target_columns=tgt_keys,
        type_column=type_column,
        type_value=rel.get("via_type_value") or None,
        attributes={
            _apply_cql_property(c.column_name): c.column_name
            for c in all_column_types.get(via_id, [])
            if c.column_name not in keys
        },
        label_source=rel["via_label_source"],
        label_fixed=rel.get("alias") or "",
    )


def _make_traversal_node(
    tgt_id: int,
    tgt_table: dict,
    col_metas: list,
    source_catalogs: dict[str, str] | None,
) -> NodeMapping:
    """Build a traversal_only NodeMapping for a cross-domain target table."""
    from provisa.compiler.naming import source_to_catalog as _s2c, apply_sql_name as _apply_sql_name

    tgt_domain_id = tgt_table.get("domain_id") or None
    tgt_domain_label = _apply_cql_label(tgt_domain_id) if tgt_domain_id else None
    tgt_raw_name = tgt_table["table_name"]
    tgt_table_label = _apply_cql_label(_strip_domain_prefix(tgt_raw_name, tgt_domain_id))
    tgt_logical = _strip_domain_prefix(tgt_raw_name, tgt_domain_id)
    tgt_type_name = f"{tgt_domain_label}_{tgt_table_label}" if tgt_domain_label else tgt_table_label
    tgt_cypher_label = (
        f"{tgt_domain_label}:{tgt_table_label}" if tgt_domain_label else tgt_table_label
    )
    col_names = [c.column_name for c in col_metas]
    _col_alias: dict[str, str] = {
        col["column_name"]: col["alias"] for col in tgt_table.get("columns", []) if col.get("alias")
    }
    props: dict[str, str] = {
        _apply_cql_property(c): (_col_alias.get(c) or _apply_sql_name(c)) for c in col_names
    }
    phys_props: dict[str, str] = {_apply_cql_property(c): c for c in col_names}
    _phys_id = _resolve_id_column(tgt_type_name, col_names, {}, [])
    id_col = _col_alias.get(_phys_id) or _apply_sql_name(_phys_id)
    tgt_source_id = tgt_table.get("source_id")
    if not tgt_source_id:
        raise ValueError(f"Target table {tgt_type_name!r} missing source_id")
    tgt_schema = tgt_table.get("schema_name")
    if not tgt_schema:
        raise ValueError(f"Target table {tgt_type_name!r} missing schema_name")
    tgt_catalog = (source_catalogs or {}).get(tgt_source_id) or (
        _s2c(tgt_source_id) if tgt_source_id else ""
    )
    return NodeMapping(
        label=tgt_cypher_label,
        type_name=tgt_type_name,
        domain_label=tgt_domain_label,
        domain_id=tgt_domain_id,
        table_label=tgt_table_label,
        table_id=tgt_id,
        source_id=tgt_source_id,
        id_column=id_col,
        pk_columns=[],
        catalog_name=tgt_catalog,
        schema_name=tgt_schema,
        table_name=tgt_logical,
        properties=props,
        physical_properties=phys_props,
        traversal_only=True,
    )


def _add_cross_domain_nodes(
    all_tables: list[dict],
    all_relationships: list[dict],
    all_column_types: dict,
    source_catalogs: dict[str, str] | None,
    nodes: dict[str, NodeMapping],
    relationships: dict[str, RelationshipMapping],
    domains: dict[str, list[str]],
    nodes_by_table: dict[str, list[str]],
    aliases: dict[str, list[RelationshipMapping]],
) -> None:
    """Add cross-domain traversal-only nodes reachable via all_relationships. Mutates all dicts."""
    table_id_to_type: dict[int, str] = {nm.table_id: tn for tn, nm in nodes.items()}
    all_tables_by_id: dict[int, dict] = {t["id"]: t for t in all_tables}
    owned_ids: set[int] = set(table_id_to_type)

    for rel in all_relationships:
        if rel.get("disable_cypher"):
            continue
        src_id: int = rel["source_table_id"]
        tgt_id: int = rel["target_table_id"]
        src_type = table_id_to_type.get(src_id)
        if src_type is None or tgt_id in owned_ids:
            continue
        tgt_table = all_tables_by_id.get(tgt_id)
        if tgt_table is None:
            continue
        # REQ-1586: a junction-backed edge is untraversable without its junction table.
        if rel.get("via_table_id") and rel["via_table_id"] not in all_tables_by_id:
            continue
        col_metas = all_column_types.get(tgt_id, [])
        if not col_metas:
            continue

        tgt_domain_id = tgt_table.get("domain_id") or None
        tgt_domain_label = _apply_cql_label(tgt_domain_id) if tgt_domain_id else None
        tgt_raw_name = tgt_table["table_name"]
        tgt_table_label = _apply_cql_label(_strip_domain_prefix(tgt_raw_name, tgt_domain_id))
        tgt_type_name = (
            f"{tgt_domain_label}_{tgt_table_label}" if tgt_domain_label else tgt_table_label
        )

        if tgt_type_name not in nodes:
            node = _make_traversal_node(tgt_id, tgt_table, col_metas, source_catalogs)
            nodes[tgt_type_name] = node
            if tgt_domain_label:
                domains.setdefault(tgt_domain_label, []).append(tgt_type_name)
            nodes_by_table.setdefault(tgt_table_label, []).append(tgt_type_name)
            owned_ids.add(tgt_id)
            table_id_to_type[tgt_id] = tgt_type_name

        cypher_alias = rel.get("alias") or rel.get("computed_cypher_alias")
        rel_cardinality = rel.get("cardinality")
        xvia = _junction_from_rel_dict(rel, all_tables_by_id, all_column_types, source_catalogs)
        rel_type = (  # REQ-1586: the nomination names a junction edge
            junction_rel_type(xvia)
            if xvia is not None
            else (
                cypher_alias
                if cypher_alias
                else _to_rel_type(rel.get("graphql_alias") or tgt_raw_name, rel_cardinality)
            )
        )
        rel_key = f"{rel_type}::{src_type}→{tgt_type_name}"
        if rel_key not in relationships:
            _src_json_key = rel.get("source_json_key")
            _xsource_expr = (
                f"JSON_EXTRACT_SCALAR({{alias}}.\"{rel['source_column']}\", '$.{_src_json_key}')"
                if _src_json_key
                else None
            )
            xrel = RelationshipMapping(
                rel_type=rel_type,
                source_label=src_type,
                target_label=tgt_type_name,
                join_source_column=rel["source_column"],
                join_target_column=rel["target_column"],
                field_name=rel.get("graphql_alias") or "",
                alias=cypher_alias,
                source_expr=_xsource_expr,
                many=(rel_cardinality == "one-to-many"),
                via=xvia,
            )
            relationships[rel_key] = xrel
            aliases.setdefault(rel_type, []).append(xrel)


_ID_EXACT = {"id", "_id", "pk", "oid"}
_ID_SUFFIX = ("_id", "_pk", "_oid")
_ID_PREFIX = ("id_",)


def _resolve_id_column(  # REQ-392, REQ-394
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


def _to_rel_type(field_name: str, cardinality: str | None = None) -> str:
    """Convert a camelCase or snake_case GraphQL field name to a verb-prefixed UPPER_SNAKE relationship type.

    many-to-one → IS_ prefix (e.g. animalBreed → IS_ANIMAL_BREED)
    one-to-many / unknown → HAS_ prefix (e.g. tableColumns → HAS_TABLE_COLUMNS, _queries → HAS_QUERIES)
    """
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", field_name).upper().lstrip("_")
    prefix = "IS_" if cardinality in ("many-to-one", "one-to-one") else "HAS_"
    return f"{prefix}{s}"


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
            return table_name[len(prefix) :]
    return table_name


def _table_label_from_table_name(table_name: str, domain_id: str | None) -> str:  # pyright: ignore[reportUnusedFunction]
    """Derive PascalCase table label by stripping domain initials prefix.

    "sa_orders"  (domain "sales_analytics", initials "sa") → "Orders"
    "orders"     (no domain or no matching prefix)          → "Orders"
    """
    if domain_id:
        prefix = _domain_initials(domain_id) + "_"
        if table_name.lower().startswith(prefix):
            table_name = table_name[len(prefix) :]
    return _apply_cql_label(table_name)


def _split_cypher_labels(field_name: str) -> tuple[str | None, str]:
    """Derive (domain_label, table_label) from a GQL field name.

    "sales_analytics__orders" → ("SalesAnalytics", "Orders")
    "orders"                  → (None, "Orders")
    """
    if "__" in field_name:
        domain_part, table_part = field_name.split("__", 1)
        return _apply_cql_label(domain_part), _apply_cql_label(table_part)
    return None, _apply_cql_label(field_name)


# ---------------------------------------------------------------------------
# Backward-compat aliases (REQ-398)
# ---------------------------------------------------------------------------

#: Alias for NodeMapping — the public name used in /data/graph-schema context.
NodeLabel = NodeMapping


def build_label_map(node_labels: list[NodeMapping]) -> dict[str, NodeMapping]:
    """Build a label → NodeMapping index from a list of node labels (REQ-398)."""
    return {n.label: n for n in node_labels}
