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

# Requirements: REQ-609, REQ-862, REQ-939, REQ-1070, REQ-1375, REQ-1377, REQ-1378

from __future__ import annotations

from provisa.api.metadata_export.governance import build_governance_tags
from provisa.api.metadata_export.model import (
    ColumnAsset,
    DomainAsset,
    GlossaryTermAsset,
    GlossaryTermEdge,
    LineageEdge,
    MetadataSnapshot,
    ModelTag,
    OwnerRef,
    RelationshipEdge,
    SourceAsset,
    TableAsset,
)
from provisa.api.metadata_export.refs import (
    SnapshotBuildError,
    TableIndex,
    UnqualifiedLineageError,
    column_ref,
    column_uri,
    domain_uri,
    relationship_uri,
    source_ref,
    source_uri,
    table_ref,
    table_uri,
    term_uri,
)
from provisa.core.models import SYSTEM_TAG_IDS, ProvisaConfig, Table, TagAssignment
from provisa.lineage.graph import Edge, LineageGraph, Node, build_column_graph


def _source_assets(
    config: ProvisaConfig, org_id: str, published_source_ids: set[str]
) -> list[SourceAsset]:
    # The Data Product filter gates sources too: a source publishes only when at least one
    # of its tables does. Publishing the whole source inventory would hand the catalog
    # internal plumbing (admin store, telemetry) and sources the admin chose not to expose.
    return [
        SourceAsset(
            ref=source_ref(source),
            id=source.id,
            source_type=source.type.value,
            description=source.description,
            semantic_uri=source_uri(org_id, source.id),
        )
        for source in config.sources
        if source.id in published_source_ids
    ]


def _domain_assets(config: ProvisaConfig, org_id: str) -> list[DomainAsset]:  # REQ-609
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
                semantic_uri=domain_uri(org_id, domain.id),
            )
        )
    return assets


def _column_asset(table: Table, column, org_id: str) -> ColumnAsset:
    return ColumnAsset(
        ref=column_ref(table, column.name),
        name=column.name,
        data_type=column.data_type or "",
        description=column.description or "",
        aliases=(column.alias,) if column.alias else (),
        semantic_uri=column_uri(org_id, table, column.name, column.alias),
    )


def _table_assets(
    tables: list[Table],
    org_id: str,
    technical_columns: frozenset[tuple[tuple[str, ...], str]] = frozenset(),
) -> list[TableAsset]:
    return [
        TableAsset(
            ref=table_ref(table),
            name=table.table_name,
            source_id=table.source_id,
            domain_id=table.domain_id,
            description=table.description or "",
            aliases=(table.alias,) if table.alias else (),
            columns=[
                _column_asset(table, column, org_id)
                for column in table.columns
                # REQ-1375: a column tagged 'technical' is classified out of the Data Product.
                if (table_ref(table).parts, column.name) not in technical_columns
            ],
            semantic_uri=table_uri(org_id, table),
        )
        for table in tables
    ]


def _relationship_edges(
    config: ProvisaConfig, index: TableIndex, org_id: str
) -> list[RelationshipEdge]:
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
                semantic_uri=relationship_uri(org_id, source, rel.alias, rel.id),
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


def _resolve_assignment_table(
    assignment: TagAssignment, index: TableIndex
) -> Table | None:
    """Resolve a table/column assignment's qualified address; None if unresolvable.

    A tag assignment can outlive its target only transiently (the DB cascades deletes);
    an unresolvable address here means the config snapshot predates the cascade, so the
    assignment is withheld rather than published against a phantom asset.
    """
    if assignment.table_ref is None:
        return None
    try:
        return index.resolve(assignment.table_ref, f"tag {assignment.tag_id!r}")
    except SnapshotBuildError:
        return None


def _technical_columns(
    config: ProvisaConfig, index: TableIndex
) -> frozenset[tuple[tuple[str, ...], str]]:
    """The (table, column) pairs tagged 'technical' (REQ-1375).

    Column-only by design: the table-level export control is the Data Product flag itself,
    so 'technical' never applies to tables.
    """
    columns: set[tuple[tuple[str, ...], str]] = set()
    for assignment in config.tag_assignments:
        if assignment.tag_id != "technical" or assignment.object_type != "column":
            continue
        target = _resolve_assignment_table(assignment, index)
        if target is not None and assignment.column_name:
            columns.add((table_ref(target).parts, assignment.column_name))
    return frozenset(columns)


def _model_tags(
    config: ProvisaConfig,
    index: TableIndex,
    keep: set[tuple[str, ...]],
    published_columns: set[tuple[str, ...]],
    published_relationships: set[str],
    published_source_ids: set[str],
) -> list[ModelTag]:  # REQ-1377, REQ-1378
    """Registry tags on published assets. Tags on withheld assets are withheld with them."""
    tags: list[ModelTag] = []
    for assignment in config.tag_assignments:
        common = {
            "tag_id": assignment.tag_id,
            "is_system": assignment.tag_id in SYSTEM_TAG_IDS,
            "reason": assignment.reason,
            "expires_on": assignment.expires_on,
        }
        if assignment.object_type == "source":
            source = next(
                (
                    s
                    for s in config.sources
                    if s.id == assignment.source_id and s.id in published_source_ids
                ),
                None,
            )
            if source is not None:
                tags.append(ModelTag(asset=source_ref(source), **common))
        elif assignment.object_type == "table":
            target = _resolve_assignment_table(assignment, index)
            if target is not None and table_ref(target).parts in keep:
                tags.append(ModelTag(asset=table_ref(target), **common))
        elif assignment.object_type == "column":
            target = _resolve_assignment_table(assignment, index)
            if target is not None and assignment.column_name:
                ref = column_ref(target, assignment.column_name)
                if ref.parts in published_columns:
                    tags.append(ModelTag(asset=ref, **common))
        elif assignment.object_type == "relationship":
            if assignment.relationship_id in published_relationships:
                tags.append(ModelTag(relationship_id=assignment.relationship_id, **common))
    return tags


def _glossary_assets(
    glossary: dict,
    exported: list[Table],
    published_columns: set[tuple[str, ...]],
    org_id: str,
) -> tuple[list[GlossaryTermAsset], list[GlossaryTermEdge]]:  # REQ-1387
    """Project the term graph onto the published assets.

    A ref publishes only when its column does (data_product + technical filters); a rooted
    term whose refs are all withheld is withheld with them, exactly as model tags are. An
    abstract term publishes unconditionally — it is user vocabulary, not physical binding.
    Edges publish when both endpoints do.
    """
    by_identity = {(t.source_id, t.schema_name, t.table_name): t for t in exported}
    refs_by_term: dict[int, list] = {}
    for ref in glossary.get("refs", []):
        table = by_identity.get((ref["source_id"], ref["schema_name"], ref["table_name"]))
        if table is None:
            continue
        asset_ref = column_ref(table, ref["column_name"])
        if asset_ref.parts not in published_columns:
            continue
        refs_by_term.setdefault(ref["term_id"], []).append(asset_ref)
    experts_by_term: dict[int, list[str]] = {}
    for expert in glossary.get("experts", []):
        experts_by_term.setdefault(expert["term_id"], []).append(expert["user_id"])
    terms = []
    for term in glossary.get("terms", []):
        refs = refs_by_term.get(term["id"], [])
        if not refs and not term["is_abstract"]:
            continue
        terms.append(
            GlossaryTermAsset(
                term_id=term["id"],
                name=term["name"],
                definition=term.get("definition"),
                is_abstract=bool(term["is_abstract"]),
                deprecated=bool(term["deprecated"]),
                refs=tuple(refs),
                experts=tuple(experts_by_term.get(term["id"], [])),
                semantic_uri=term_uri(org_id, term["name"]),
            )
        )
    published_ids = {t.term_id for t in terms}
    edges = [
        GlossaryTermEdge(
            from_term_id=e["from_term_id"],
            to_term_id=e["to_term_id"],
            rel_type=e["rel_type"],
        )
        for e in glossary.get("edges", [])
        if e["from_term_id"] in published_ids and e["to_term_id"] in published_ids
    ]
    return terms, edges


def build_snapshot(
    config: ProvisaConfig, *, org_id: str, dialect: str, glossary: dict | None = None
) -> MetadataSnapshot:  # REQ-1070
    """Project the governed config into the vendor-neutral snapshot every adapter publishes.

    ``dialect`` is the federation engine's SQLGlot dialect — the one the view SQL was compiled
    for. It is required rather than defaulted: parsing a view with the wrong dialect yields
    plausible, wrong lineage.
    """
    # Name resolution sees EVERY table: a bare relationship/lineage name that is ambiguous
    # across the full config stays refused, whether or not both candidates publish.
    index = TableIndex(config.tables)
    # The Data Product checkbox is the export filter: only marked tables publish, and every
    # edge or tag touching an unmarked table is withheld with it — a dangling edge would hand
    # the catalog a reference to an asset it was never sent, and name the table the admin
    # chose not to publish. REQ-1375: the 'technical' system tag prunes COLUMNS from a
    # published table the same way; at table level the flag itself is the control.
    technical_columns = _technical_columns(config, index)
    exported = [table for table in config.tables if table.data_product]
    keep = {table_ref(table).parts for table in exported}
    published_source_ids = {table.source_id for table in exported}
    tables = _table_assets(exported, org_id, technical_columns)
    relationships = [
        edge
        for edge in _relationship_edges(config, index, org_id)
        if edge.source.parts in keep and (edge.target is None or edge.target.parts in keep)
    ]
    published_columns = {column.ref.parts for table in tables for column in table.columns}
    glossary_terms, glossary_edges = (
        _glossary_assets(glossary, exported, published_columns, org_id)
        if glossary is not None
        else ([], [])
    )
    return MetadataSnapshot(
        org_id=org_id,
        sources=_source_assets(config, org_id, published_source_ids),
        domains=_domain_assets(config, org_id),
        tables=tables,
        relationships=relationships,
        lineage=[
            edge
            for edge in _lineage_edges(config, index, dialect)
            if edge.upstream.parts[:3] in keep and edge.downstream.parts[:3] in keep
        ],
        # REQ-1071: the restrictions ship with the assets. A snapshot carrying assets without
        # their governance tags is the dangerous half-truth — a consumer would read an
        # unannotated column as unrestricted.
        governance_tags=[
            tag for tag in build_governance_tags(config, org_id) if tag.asset.parts[:3] in keep
        ],
        model_tags=_model_tags(
            config,
            index,
            keep,
            published_columns,
            {edge.id for edge in relationships},
            published_source_ids,
        ),
        glossary_terms=glossary_terms,
        glossary_edges=glossary_edges,
    )
