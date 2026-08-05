# Copyright (c) 2026 Kenneth Stott
# Canary: 5e5f0d2a-1c9b-4b6e-9a5e-1d3f7b2c8a41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Build the vendor-neutral :class:`MetadataSnapshot` from a governed config (REQ-1070).

The snapshot is derived from what Provisa already knows and enforces — sources, domains and
their stewards, tables, columns, approved relationships — plus column-level lineage resolved
from the compiled view SQL of every materialized/derived table (REQ-939, REQ-862). Nothing
here scans a warehouse or infers lineage statistically: an edge exists because a compiled
query produces that column from that column.

Every asset reference a relationship or a lineage leaf names must resolve to a table in the
config. An unresolvable or ambiguous name raises — publishing a snapshot with a silently
dropped edge would tell the external catalog that a governed derivation does not exist.
"""

# Requirements: REQ-609, REQ-862, REQ-939, REQ-1070

from __future__ import annotations

from provisa.api.metadata_egress.governance import build_governance_tags
from provisa.api.metadata_egress.model import (
    ColumnAsset,
    DomainAsset,
    LineageEdge,
    MetadataSnapshot,
    OwnerRef,
    RelationshipEdge,
    SourceAsset,
    TableAsset,
)
from provisa.api.metadata_egress.refs import (
    TableIndex,
    UnqualifiedLineageError,
    column_ref,
    source_ref,
    table_ref,
)
from provisa.core.models import ProvisaConfig, Table
from provisa.lineage.graph import Edge, LineageGraph, Node, build_column_graph


def _source_assets(config: ProvisaConfig) -> list[SourceAsset]:
    return [
        SourceAsset(
            ref=source_ref(source),
            id=source.id,
            source_type=source.type.value,
            description=source.description,
        )
        for source in config.sources
    ]


def _domain_assets(config: ProvisaConfig) -> list[DomainAsset]:  # REQ-609
    assets: list[DomainAsset] = []
    for domain in config.domains:
        steward = (
            OwnerRef(id=domain.steward, kind="steward") if domain.steward is not None else None
        )
        # REQ-609: a domain with no steward is published as pending rather than omitted — the
        # catalog must show the governance gap, not a shorter list of domains.
        assets.append(
            DomainAsset(
                id=domain.id,
                description=domain.description,
                steward=steward,
                pending=steward is None,
            )
        )
    return assets


def _column_asset(table: Table, column) -> ColumnAsset:
    return ColumnAsset(
        ref=column_ref(table, column.name),
        name=column.name,
        data_type=column.data_type or "",
        description=column.description or "",
        aliases=(column.alias,) if column.alias else (),
    )


def _table_assets(config: ProvisaConfig) -> list[TableAsset]:
    return [
        TableAsset(
            ref=table_ref(table),
            name=table.table_name,
            source_id=table.source_id,
            domain_id=table.domain_id,
            description=table.description or "",
            aliases=(table.alias,) if table.alias else (),
            columns=[_column_asset(table, column) for column in table.columns],
        )
        for table in config.tables
    ]


def _relationship_edges(config: ProvisaConfig, index: TableIndex) -> list[RelationshipEdge]:
    edges: list[RelationshipEdge] = []
    for rel in config.relationships:
        context = f"relationship {rel.id!r}"
        source = index.resolve(rel.source_table_id, context)
        # A computed (function-target) relationship has no target table by design (REQ-019).
        target = index.resolve(rel.target_table_id, context) if rel.target_table_id else None
        edges.append(
            RelationshipEdge(
                id=rel.id,
                source=table_ref(source),
                target=table_ref(target) if target is not None else None,
                source_column=rel.source_column,
                target_column=rel.target_column,
                cardinality=rel.cardinality.value,
                alias=rel.alias,
                # REQ-020: the defining steward owns the approved join.
                owner=OwnerRef(id=rel.owner, kind="relationship_owner") if rel.owner else None,
                version=rel.version,
                needs_review=rel.needs_review,
            )
        )
    return edges


def _upstream_sources(graph: LineageGraph, output_id: str) -> list[tuple[Node, tuple[str, ...]]]:
    """Walk one output column back to the real base-table columns it derives from.

    Nodes of kind ``derived`` are in-statement intermediates (a CTE or subquery column). They are
    not publishable assets, so the walk passes through them and keeps the transforms it crossed —
    the composed formula, not a bare dependency. A walk that ends on a derived node with no
    upstream is a constant projection and contributes no edge at all: it has no upstream asset,
    and inventing one would publish a derivation that does not exist.
    """
    incoming: dict[str, list[Edge]] = {}
    for edge in graph.edges:
        incoming.setdefault(edge.target, []).append(edge)

    found: list[tuple[Node, tuple[str, ...]]] = []
    # (node id, transforms crossed). ``seen`` guards a diamond (one intermediate feeding two
    # paths) from re-expanding, which a wide CTE graph would otherwise do exponentially.
    stack: list[tuple[str, tuple[str, ...]]] = [(output_id, ())]
    seen: set[tuple[str, tuple[str, ...]]] = set()
    while stack:
        node_id, transforms = stack.pop()
        if (node_id, transforms) in seen:
            continue
        seen.add((node_id, transforms))
        node = graph.nodes.get(node_id)
        if node is not None and node.kind == "source":
            found.append((node, transforms))
            continue
        for edge in incoming.get(node_id, ()):
            stack.append((edge.source, (*transforms, edge.transform)))
    return found


def _lineage_edges(
    config: ProvisaConfig, index: TableIndex, dialect: str
) -> list[LineageEdge]:  # REQ-862, REQ-939
    """Column-level lineage from each view's compiled SELECT.

    Each view is resolved on its own, so a view whose input is another view yields an edge into
    that view's column — a multi-hop derivation publishes as a chain of edges, each carrying its
    own transform, rather than one flattened claim no single query performs.
    """
    edges: list[LineageEdge] = []
    for table in config.tables:
        if table.view_sql is None:
            continue
        graph = build_column_graph(table.view_sql, dialect=dialect)
        for output_id in graph.outputs:
            output = graph.nodes[output_id]
            downstream = column_ref(table, output.column)
            for upstream_node, transforms in _upstream_sources(graph, output_id):
                if upstream_node.relation is None:
                    raise UnqualifiedLineageError(table.table_name, output.column, upstream_node.id)
                upstream_table = index.resolve(
                    upstream_node.relation,
                    f"view {table.table_name!r} column {output.column!r}",
                )
                edges.append(
                    LineageEdge(
                        upstream=column_ref(upstream_table, upstream_node.column),
                        downstream=downstream,
                        transforms=transforms,
                    )
                )
    return edges


def build_snapshot(
    config: ProvisaConfig, *, org_id: str, dialect: str
) -> MetadataSnapshot:  # REQ-1070
    """Project the governed config into the vendor-neutral snapshot every adapter publishes.

    ``dialect`` is the federation engine's SQLGlot dialect — the one the view SQL was compiled
    for. It is required rather than defaulted: parsing a view with the wrong dialect yields
    plausible, wrong lineage.
    """
    index = TableIndex(config.tables)
    return MetadataSnapshot(
        org_id=org_id,
        sources=_source_assets(config),
        domains=_domain_assets(config),
        tables=_table_assets(config),
        relationships=_relationship_edges(config, index),
        lineage=_lineage_edges(config, index, dialect),
        # REQ-1071: the restrictions ship with the assets. A snapshot carrying assets without
        # their governance tags is the dangerous half-truth — a consumer would read an
        # unannotated column as unrestricted.
        governance_tags=build_governance_tags(config),
    )
