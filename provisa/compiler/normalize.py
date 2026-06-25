# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

# Requirements: REQ-049, REQ-009

"""Normalized (relational) result decomposition (REQ-049).

A nested query is shredded back into one relational table per entity type it projects,
with real PK/FK key columns preserved, so a consumer can load the small tables and replay
the *same* query against them to reconstruct the denormalized view locally.

Performance is the whole point: instead of materializing the denormalized join product
(1M x 1M -> 1B rows) and de-duplicating it inside Provisa, each entity table is produced by
its own scoped ``SELECT DISTINCT`` pushed to the engine. The product never forms.

Approach (IR re-emit): for each entity path we prune the original GraphQL document to that
path's branch — keeping ancestor filters so the table is correctly scoped — project only
that entity's scalar columns plus its PK and the join-key (FK) columns, recompile with
``flat=True`` (so one-to-many relations become flat joins, not aggregated arrays), and wrap
the result in ``SELECT DISTINCT``. Ancestor joins are many-to-one, so they bound — no fan-out.

Precondition: every relationship on a normalized path must join on real columns. A computed
join (``source_expr``/``source_constant``/``source_json_key``) has no key column to put in an
FK, so the query cannot be normalized and is rejected with a clear error.
"""

from __future__ import annotations

from dataclasses import dataclass

from graphql import (
    DocumentNode,
    FieldNode,
    NameNode,
    OperationDefinitionNode,
    OperationType,
    SelectionSetNode,
)

from provisa.compiler.sql_gen import CompilationContext, CompiledQuery, compile_query


class NormalizeError(ValueError):
    """A query cannot be decomposed into normalized relational tables."""


@dataclass
class NormalizedTable:
    """One relational table emitted for an entity type in the query."""

    table_name: str  # physical table name of the entity
    path: tuple[str, ...]  # GraphQL field-name chain from the root (() == root)
    compiled: CompiledQuery  # scoped SELECT DISTINCT for this table


@dataclass
class _PathNode:
    """An entity path discovered in the query: the field chain + its compile context."""

    path: tuple[str, ...]
    field_chain: list[FieldNode]  # FieldNode at each level (root..leaf)
    parent_type_chain: list[str]  # owning type name at each level (for ctx.joins lookup)
    table_id: int
    table_name: str


def _is_relationship(ctx: CompilationContext, parent_type: str, field_name: str) -> bool:
    return (parent_type, field_name) in ctx.joins


def _scalar_fields(field_node: FieldNode) -> list[FieldNode]:
    """Direct scalar (no-selection-set) child fields of a node."""
    if field_node.selection_set is None:
        return []
    return [
        s
        for s in field_node.selection_set.selections
        if isinstance(s, FieldNode) and s.selection_set is None
    ]


def _field(name: str) -> FieldNode:
    return FieldNode(name=NameNode(value=name), arguments=(), directives=())


def discover_entity_paths(document: DocumentNode, ctx: CompilationContext) -> list[_PathNode]:
    """Walk the document and return one _PathNode per projected entity type."""
    paths: list[_PathNode] = []

    def _walk(
        field_node: FieldNode,
        owning_type: str,
        path: tuple[str, ...],
        field_chain: list[FieldNode],
        type_chain: list[str],
    ) -> None:
        table_meta = None
        if not path:
            table_meta = ctx.tables.get(field_node.name.value)
        else:
            jm = ctx.joins.get((owning_type, field_node.name.value))
            table_meta = jm.target if jm else None
        if table_meta is None:
            return
        paths.append(
            _PathNode(
                path=path or (field_node.name.value,),
                field_chain=list(field_chain),
                parent_type_chain=list(type_chain),
                table_id=table_meta.table_id,
                table_name=table_meta.table_name,
            )
        )
        if field_node.selection_set is None:
            return
        for sel in field_node.selection_set.selections:
            if not isinstance(sel, FieldNode) or sel.selection_set is None:
                continue
            if _is_relationship(ctx, table_meta.type_name, sel.name.value):
                _walk(
                    sel,
                    table_meta.type_name,
                    (path or (field_node.name.value,)) + (sel.name.value,),
                    field_chain + [sel],
                    type_chain + [table_meta.type_name],
                )

    for defn in document.definitions:
        if not isinstance(defn, OperationDefinitionNode):
            continue
        for sel in defn.selection_set.selections:
            if isinstance(sel, FieldNode) and sel.name.value in ctx.tables:
                root_type = ctx.tables[sel.name.value].type_name
                _walk(sel, root_type, (), [sel], [])
    return paths


def check_normalizable(document: DocumentNode, ctx: CompilationContext) -> None:
    """Raise NormalizeError if any relationship on a path joins on a non-column expression."""
    for pn in discover_entity_paths(document, ctx):
        for depth in range(1, len(pn.field_chain)):
            parent_type = pn.parent_type_chain[depth - 1]
            rel_field = pn.field_chain[depth].name.value
            jm = ctx.joins.get((parent_type, rel_field))
            if jm is None:
                continue
            if (
                jm.source_expr is not None
                or jm.source_constant is not None
                or jm.source_json_key is not None
            ):
                raise NormalizeError(
                    f"cannot normalize: relationship {rel_field!r} joins on a computed "
                    f"expression, not a key column — every relationship must be a real "
                    f"FK/PK column join to produce normalized tables."
                )


def _required_columns_by_table(
    paths: list[_PathNode], ctx: CompilationContext
) -> dict[int, set[str]]:
    """Key columns each entity table must project so the export self-joins (REQ-049).

    Per table: its PK, plus each relationship join column that physically lives on it.
    A JoinMeta is defined from the parent's side — ``source_column`` lives on the parent
    (source) table, ``target_column`` on the child (target) table — so the FK ends up on the
    correct side for both many-to-one and one-to-many edges.
    """
    req: dict[int, set[str]] = {}
    for pn in paths:
        req.setdefault(pn.table_id, set()).update(ctx.pk_columns.get(pn.table_id, []))
    by_path = {pn.path: pn for pn in paths}
    for pn in paths:
        if len(pn.field_chain) < 2:
            continue
        jm = ctx.joins.get((pn.parent_type_chain[-1], pn.field_chain[-1].name.value))
        if jm is None:
            continue
        if jm.target_column:
            req.setdefault(pn.table_id, set()).add(jm.target_column)
        parent_pn = by_path.get(pn.path[:-1])
        if jm.source_column and parent_pn is not None:
            req.setdefault(parent_pn.table_id, set()).add(jm.source_column)
    return req


def _prune_document(pn: _PathNode, required_cols: set[str]) -> DocumentNode:
    """Build a document containing only this entity's path, scoped by ancestor filters."""
    leaf = pn.field_chain[-1]
    # leaf projection: its scalar fields + the key columns this table must carry.
    projected = {f.name.value for f in _scalar_fields(leaf)}
    projected.update(required_cols)
    leaf_selections: list[FieldNode] = [_field(name) for name in sorted(projected)]

    # Rebuild the branch bottom-up: each ancestor keeps its arguments (filters) and selects
    # only the next field in the chain.
    node = FieldNode(
        name=leaf.name,
        alias=leaf.alias,
        arguments=leaf.arguments,
        directives=(),
        selection_set=SelectionSetNode(selections=tuple(leaf_selections)),
    )
    for ancestor in reversed(pn.field_chain[:-1]):
        node = FieldNode(
            name=ancestor.name,
            alias=ancestor.alias,
            arguments=ancestor.arguments,
            directives=(),
            selection_set=SelectionSetNode(selections=(node,)),
        )

    op = OperationDefinitionNode(
        operation=OperationType.QUERY,
        selection_set=SelectionSetNode(selections=(node,)),
        variable_definitions=(),
        directives=(),
    )
    return DocumentNode(definitions=(op,))


def compile_normalized(  # REQ-049
    document: DocumentNode,
    ctx: CompilationContext,
    variables: dict | None = None,
    use_catalog: bool = True,
) -> list[NormalizedTable]:
    """Compile a query into one scoped SELECT DISTINCT per projected entity table (REQ-049)."""
    check_normalizable(document, ctx)
    paths = discover_entity_paths(document, ctx)
    required = _required_columns_by_table(paths, ctx)
    tables: list[NormalizedTable] = []
    for pn in paths:
        pruned = _prune_document(pn, required.get(pn.table_id, set()))
        compiled_list = compile_query(pruned, ctx, variables, use_catalog=use_catalog, flat=True)
        if not compiled_list:
            continue
        compiled = compiled_list[0]
        # DISTINCT-dedup at the engine so each entity row appears once.
        if not compiled.sql.lstrip().upper().startswith("SELECT DISTINCT"):
            compiled.sql = compiled.sql.replace("SELECT ", "SELECT DISTINCT ", 1)
        tables.append(NormalizedTable(table_name=pn.table_name, path=pn.path, compiled=compiled))
    return tables
