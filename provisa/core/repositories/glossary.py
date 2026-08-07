# Copyright (c) 2026 Kenneth Stott
# Canary: 8c2d4e91-6f3a-4b7c-9d05-1a8e7f2b6c44
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Business-glossary repository, via SQLAlchemy Core (dialect-portable).

Terms are the normalized vocabulary derived from physical field names (REQ-1387).
Lifecycle is derived from semantic-layer membership: ``sync_table_refs`` runs inside
the table repository's upsert (the single write path for ``table_columns``), and
``sweep_refless_terms`` runs after any deletion path whose FK cascade removed refs.
A term losing its last physical ref is REMOVED unless deleting it would leave an
abstract term dangling — with no path to any rooted term — in which case the term
is deprecated (kept) instead.
"""

# Requirements: REQ-1387

from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, select, update

from provisa.core.glossary import TERM_EDGE_TYPES, normalize_term
from provisa.core.schema_org import (
    glossary_term_edges,
    glossary_term_experts,
    glossary_term_refs,
    glossary_terms,
    registered_tables,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection


# ---------------------------------------------------------------------------
# Lifecycle (driven by the table repository)
# ---------------------------------------------------------------------------


async def sync_table_refs(
    conn: "Connection", table_id: int, column_names: list[str], *, table_context: str | None = None
) -> None:
    """Reconcile one table's refs with its current column set, in the caller's transaction.

    New columns create-or-link terms by deterministic normalization (relinking a
    deprecated term revives it); departed columns drop their refs and the affected
    terms are settled under the remove-or-deprecate rule. ``table_context`` is the
    table's business name — it qualifies TOO-GENERIC phrases (employees.first_name →
    "employee first name") so unrelated tables' name/date/id columns never over-merge.
    """
    existing = {
        r.column_name: r.term_id
        for r in (
            await conn.execute_core(
                select(glossary_term_refs.c.column_name, glossary_term_refs.c.term_id).where(
                    glossary_term_refs.c.table_id == table_id
                )
            )
        ).fetchall()
    }
    current = set(column_names)
    departed = [c for c in existing if c not in current]
    if departed:
        await conn.execute_core(
            _delete(glossary_term_refs).where(
                (glossary_term_refs.c.table_id == table_id)
                & (glossary_term_refs.c.column_name.in_(departed))
            )
        )
    for col in column_names:
        if col in existing:
            continue
        term_id = await _find_or_create_term(
            conn, normalize_term(col, table_context=table_context)
        )
        await conn.upsert(
            glossary_term_refs,
            {"term_id": term_id, "table_id": table_id, "column_name": col},
            index_elements=["table_id", "column_name"],
            update_columns=["term_id"],
        )
    if departed:
        await _settle_terms(conn, {existing[c] for c in departed})


async def sweep_refless_terms(conn: "Connection") -> None:
    """Settle every rooted-born term whose refs are all gone (FK-cascade deletion paths).

    Table/source deletion and config purges remove registered_tables rows out of this
    repository's sight; the deletion paths call this afterwards so those terms still get
    the remove-or-deprecate rule. Refs are pruned here by a departed-table probe rather
    than FK cascade, because the SQLite backend runs without PRAGMA foreign_keys and its
    cascades never fire.
    """
    await conn.execute_core(
        _delete(glossary_term_refs).where(
            ~glossary_term_refs.c.table_id.in_(select(registered_tables.c.id))
        )
    )
    rows = (
        await conn.execute_core(
            select(glossary_terms.c.id)
            .where(~glossary_terms.c.is_abstract, ~glossary_terms.c.deprecated)
            .where(
                ~glossary_terms.c.id.in_(select(glossary_term_refs.c.term_id).distinct())
            )
        )
    ).fetchall()
    if rows:
        await _settle_terms(conn, {r.id for r in rows})


async def _find_or_create_term(conn: "Connection", name: str) -> int:
    row = (
        await conn.execute_core(select(glossary_terms).where(glossary_terms.c.name == name))
    ).fetchone()
    if row is not None:
        if row.deprecated:
            await conn.execute_core(
                update(glossary_terms)
                .where(glossary_terms.c.id == row.id)
                .values(deprecated=False)
            )
        return row.id
    term_id = await conn.upsert_returning(
        glossary_terms,
        {"name": name, "is_abstract": False, "deprecated": False},
        index_elements=["name"],
        returning="id",
        update_columns=["deprecated"],
    )
    return term_id


async def _settle_terms(conn: "Connection", term_ids: set[int]) -> None:
    """Apply the remove-or-deprecate rule to each candidate that is refless and rooted-born."""
    if not term_ids:
        return
    graph = await _load_graph(conn)
    for term_id in sorted(term_ids):
        node = graph.terms.get(term_id)
        if node is None or node["is_abstract"] or node["deprecated"] or node["ref_count"] > 0:
            continue
        if _dangling_grows(graph, term_id):
            await conn.execute_core(
                update(glossary_terms)
                .where(glossary_terms.c.id == term_id)
                .values(deprecated=True)
            )
            node["deprecated"] = True
        else:
            await conn.execute_core(
                _delete(glossary_term_edges).where(
                    (glossary_term_edges.c.from_term_id == term_id)
                    | (glossary_term_edges.c.to_term_id == term_id)
                )
            )
            await conn.execute_core(
                _delete(glossary_term_experts).where(
                    glossary_term_experts.c.term_id == term_id
                )
            )
            await conn.execute_core(
                _delete(glossary_terms).where(glossary_terms.c.id == term_id)
            )
            graph.remove(term_id)


class _TermGraph:
    """In-memory term graph for the dangling-abstract-term reachability check."""

    def __init__(self, terms: dict[int, dict], edges: list[tuple[int, int]]):
        self.terms = terms
        self.edges = edges

    def remove(self, term_id: int) -> None:
        self.terms.pop(term_id, None)
        self.edges = [(a, b) for a, b in self.edges if term_id not in (a, b)]

    def dangling(
        self,
        excluded: frozenset[int] = frozenset(),
        extra_roots: frozenset[int] = frozenset(),
    ) -> set[int]:
        """Abstract terms with no undirected path to any rooted (ref-holding) term.

        ``extra_roots`` counts the named terms as rooted regardless of refs — the settle
        baseline treats the candidate term as still-anchoring (it was rooted until the
        operation being judged), so its dependents never read as "already dangling".
        """
        adjacency: dict[int, set[int]] = {}
        for a, b in self.edges:
            if a in excluded or b in excluded:
                continue
            adjacency.setdefault(a, set()).add(b)
            adjacency.setdefault(b, set()).add(a)
        frontier = [
            tid
            for tid, node in self.terms.items()
            if tid not in excluded and (node["ref_count"] > 0 or tid in extra_roots)
        ]
        reached = set(frontier)
        while frontier:
            nxt = frontier.pop()
            for neighbor in adjacency.get(nxt, ()):
                if neighbor not in reached:
                    reached.add(neighbor)
                    frontier.append(neighbor)
        return {
            tid
            for tid, node in self.terms.items()
            if node["is_abstract"] and tid not in excluded and tid not in reached
        }


async def _load_graph(conn: "Connection") -> _TermGraph:
    term_rows = (
        await conn.execute_core(
            select(
                glossary_terms.c.id,
                glossary_terms.c.is_abstract,
                glossary_terms.c.deprecated,
            )
        )
    ).fetchall()
    ref_rows = (await conn.execute_core(select(glossary_term_refs.c.term_id))).fetchall()
    counts: dict[int, int] = {}
    for r in ref_rows:
        counts[r.term_id] = counts.get(r.term_id, 0) + 1
    edge_rows = (
        await conn.execute_core(
            select(glossary_term_edges.c.from_term_id, glossary_term_edges.c.to_term_id)
        )
    ).fetchall()
    terms = {
        r.id: {
            "is_abstract": bool(r.is_abstract),
            "deprecated": bool(r.deprecated),
            "ref_count": counts.get(r.id, 0),
        }
        for r in term_rows
    }
    return _TermGraph(terms, [(r.from_term_id, r.to_term_id) for r in edge_rows])


def _dangling_grows(graph: _TermGraph, term_id: int) -> bool:
    """True when removing ``term_id`` would newly disconnect an abstract term from all roots."""
    kept = graph.dangling(extra_roots=frozenset({term_id}))
    removed = graph.dangling(excluded=frozenset({term_id}))
    return bool(removed - kept)


# ---------------------------------------------------------------------------
# Curation (admin surface)
# ---------------------------------------------------------------------------


async def list_terms(
    conn: "Connection", *, q: str | None = None, include_deprecated: bool = True
) -> list[dict]:
    stmt = select(glossary_terms)
    if q:
        pattern = f"%{q.lower()}%"
        stmt = stmt.where(
            glossary_terms.c.name.ilike(pattern) | glossary_terms.c.definition.ilike(pattern)
        )
    if not include_deprecated:
        stmt = stmt.where(~glossary_terms.c.deprecated)
    rows = (await conn.execute_core(stmt.order_by(glossary_terms.c.name))).fetchall()
    ref_rows = (await conn.execute_core(select(glossary_term_refs.c.term_id))).fetchall()
    counts: dict[int, int] = {}
    for r in ref_rows:
        counts[r.term_id] = counts.get(r.term_id, 0) + 1
    return [dict(r._mapping) | {"ref_count": counts.get(r.id, 0)} for r in rows]


async def get_term(conn: "Connection", term_id: int) -> dict | None:
    row = (
        await conn.execute_core(select(glossary_terms).where(glossary_terms.c.id == term_id))
    ).fetchone()
    if row is None:
        return None
    term = dict(row._mapping)
    refs = (
        await conn.execute_core(
            select(
                glossary_term_refs.c.table_id,
                glossary_term_refs.c.column_name,
                registered_tables.c.source_id,
                registered_tables.c.schema_name,
                registered_tables.c.table_name,
                registered_tables.c.alias,
                registered_tables.c.domain_id,
            )
            .join(registered_tables, glossary_term_refs.c.table_id == registered_tables.c.id)
            .where(glossary_term_refs.c.term_id == term_id)
            .order_by(registered_tables.c.table_name, glossary_term_refs.c.column_name)
        )
    ).fetchall()
    term["refs"] = [dict(r._mapping) for r in refs]
    other = glossary_terms.alias("other")
    outgoing = (
        await conn.execute_core(
            select(
                glossary_term_edges.c.to_term_id.label("term_id"),
                glossary_term_edges.c.rel_type,
                other.c.name,
            )
            .join(other, glossary_term_edges.c.to_term_id == other.c.id)
            .where(glossary_term_edges.c.from_term_id == term_id)
        )
    ).fetchall()
    incoming = (
        await conn.execute_core(
            select(
                glossary_term_edges.c.from_term_id.label("term_id"),
                glossary_term_edges.c.rel_type,
                other.c.name,
            )
            .join(other, glossary_term_edges.c.from_term_id == other.c.id)
            .where(glossary_term_edges.c.to_term_id == term_id)
        )
    ).fetchall()
    term["edges_out"] = [dict(r._mapping) for r in outgoing]
    term["edges_in"] = [dict(r._mapping) for r in incoming]
    experts = (
        await conn.execute_core(
            select(glossary_term_experts.c.user_id, glossary_term_experts.c.kind)
            .where(glossary_term_experts.c.term_id == term_id)
            .order_by(glossary_term_experts.c.user_id)
        )
    ).fetchall()
    term["experts"] = [dict(r._mapping) for r in experts]
    return term


async def create_abstract_term(
    conn: "Connection", name: str, *, definition: str | None = None
) -> int:
    name = name.strip()
    if not name:
        raise ValueError("term name is required")
    existing = (
        await conn.execute_core(
            select(glossary_terms.c.id).where(glossary_terms.c.name == name)
        )
    ).fetchone()
    if existing is not None:
        raise ValueError(f"term {name!r} already exists")
    return await conn.upsert_returning(
        glossary_terms,
        {"name": name, "definition": definition, "is_abstract": True, "deprecated": False},
        index_elements=["name"],
        returning="id",
        update_columns=["definition"],
    )


async def rename_term(conn: "Connection", term_id: int, new_name: str) -> bool:
    new_name = new_name.strip()
    if not new_name:
        raise ValueError("term name is required")
    taken = (
        await conn.execute_core(
            select(glossary_terms.c.id).where(
                (glossary_terms.c.name == new_name) & (glossary_terms.c.id != term_id)
            )
        )
    ).fetchone()
    if taken is not None:
        raise ValueError(f"term {new_name!r} already exists")
    result = await conn.execute_core(
        update(glossary_terms).where(glossary_terms.c.id == term_id).values(name=new_name)
    )
    return (result.rowcount or 0) > 0


async def set_definition(conn: "Connection", term_id: int, definition: str | None) -> bool:
    result = await conn.execute_core(
        update(glossary_terms).where(glossary_terms.c.id == term_id).values(definition=definition)
    )
    return (result.rowcount or 0) > 0


async def delete_term(conn: "Connection", term_id: int) -> bool:
    """Delete a term with no physical refs (abstract or deprecated). Rooted terms are
    lifecycle-managed: their removal happens only when the schema element departs."""
    refs = (
        await conn.execute_core(
            select(glossary_term_refs.c.id).where(glossary_term_refs.c.term_id == term_id)
        )
    ).fetchall()
    if refs:
        raise ValueError("term has physical refs; remove or move them first")
    await conn.execute_core(
        _delete(glossary_term_edges).where(
            (glossary_term_edges.c.from_term_id == term_id)
            | (glossary_term_edges.c.to_term_id == term_id)
        )
    )
    await conn.execute_core(
        _delete(glossary_term_experts).where(glossary_term_experts.c.term_id == term_id)
    )
    result = await conn.execute_core(
        _delete(glossary_terms).where(glossary_terms.c.id == term_id)
    )
    return (result.rowcount or 0) > 0


async def move_ref(
    conn: "Connection", table_id: int, column_name: str, to_term_id: int
) -> bool:
    """Move one physical ref to another term (consolidation); the losing term is settled."""
    row = (
        await conn.execute_core(
            select(glossary_term_refs.c.id, glossary_term_refs.c.term_id).where(
                (glossary_term_refs.c.table_id == table_id)
                & (glossary_term_refs.c.column_name == column_name)
            )
        )
    ).fetchone()
    if row is None:
        return False
    target = (
        await conn.execute_core(
            select(glossary_terms.c.id).where(glossary_terms.c.id == to_term_id)
        )
    ).fetchone()
    if target is None:
        raise ValueError(f"term {to_term_id} does not exist")
    if row.term_id == to_term_id:
        return True
    await conn.execute_core(
        update(glossary_term_refs)
        .where(glossary_term_refs.c.id == row.id)
        .values(term_id=to_term_id)
    )
    await _settle_terms(conn, {row.term_id})
    return True


async def add_edge(conn: "Connection", from_term_id: int, to_term_id: int, rel_type: str) -> None:
    if rel_type not in TERM_EDGE_TYPES:
        raise ValueError(f"rel_type must be one of {TERM_EDGE_TYPES}")
    if from_term_id == to_term_id:
        raise ValueError("a term cannot relate to itself")
    for tid in (from_term_id, to_term_id):
        exists = (
            await conn.execute_core(
                select(glossary_terms.c.id).where(glossary_terms.c.id == tid)
            )
        ).fetchone()
        if exists is None:
            raise ValueError(f"term {tid} does not exist")
    await conn.upsert(
        glossary_term_edges,
        {"from_term_id": from_term_id, "to_term_id": to_term_id, "rel_type": rel_type},
        index_elements=["from_term_id", "to_term_id", "rel_type"],
        update_columns=["rel_type"],
    )


async def remove_edge(
    conn: "Connection", from_term_id: int, to_term_id: int, rel_type: str
) -> bool:
    result = await conn.execute_core(
        _delete(glossary_term_edges).where(
            (glossary_term_edges.c.from_term_id == from_term_id)
            & (glossary_term_edges.c.to_term_id == to_term_id)
            & (glossary_term_edges.c.rel_type == rel_type)
        )
    )
    return (result.rowcount or 0) > 0


async def add_expert(
    conn: "Connection", term_id: int, user_id: str, *, kind: str = "expert"
) -> None:
    if kind not in ("expert", "author"):
        raise ValueError("kind must be 'expert' or 'author'")
    exists = (
        await conn.execute_core(
            select(glossary_terms.c.id).where(glossary_terms.c.id == term_id)
        )
    ).fetchone()
    if exists is None:
        raise ValueError(f"term {term_id} does not exist")
    await conn.upsert(
        glossary_term_experts,
        {"term_id": term_id, "user_id": user_id, "kind": kind},
        index_elements=["term_id", "user_id"],
        update_columns=["kind"],
    )


async def remove_expert(conn: "Connection", term_id: int, user_id: str) -> bool:
    result = await conn.execute_core(
        _delete(glossary_term_experts).where(
            (glossary_term_experts.c.term_id == term_id)
            & (glossary_term_experts.c.user_id == user_id)
        )
    )
    return (result.rowcount or 0) > 0


async def export_graph(conn: "Connection") -> dict:
    """The whole term graph in one shape for the metadata-export builder (REQ-1387):
    terms, refs joined to their table's config identity, edges, and experts."""
    terms = [
        dict(r._mapping)
        for r in (
            await conn.execute_core(select(glossary_terms).order_by(glossary_terms.c.id))
        ).fetchall()
    ]
    refs = [
        dict(r._mapping)
        for r in (
            await conn.execute_core(
                select(
                    glossary_term_refs.c.term_id,
                    glossary_term_refs.c.column_name,
                    registered_tables.c.source_id,
                    registered_tables.c.schema_name,
                    registered_tables.c.table_name,
                ).join(
                    registered_tables,
                    glossary_term_refs.c.table_id == registered_tables.c.id,
                )
            )
        ).fetchall()
    ]
    edges = [
        dict(r._mapping)
        for r in (
            await conn.execute_core(
                select(
                    glossary_term_edges.c.from_term_id,
                    glossary_term_edges.c.to_term_id,
                    glossary_term_edges.c.rel_type,
                )
            )
        ).fetchall()
    ]
    experts = [
        dict(r._mapping)
        for r in (
            await conn.execute_core(
                select(
                    glossary_term_experts.c.term_id,
                    glossary_term_experts.c.user_id,
                    glossary_term_experts.c.kind,
                )
            )
        ).fetchall()
    ]
    return {"terms": terms, "refs": refs, "edges": edges, "experts": experts}


async def get_term_by_ref(conn: "Connection", table_id: int, column_name: str) -> dict | None:
    """The term one physical column resolves to — the hover-summary lookup (REQ-1387)."""
    row = (
        await conn.execute_core(
            select(glossary_term_refs.c.term_id).where(
                (glossary_term_refs.c.table_id == table_id)
                & (glossary_term_refs.c.column_name == column_name)
            )
        )
    ).fetchone()
    if row is None:
        return None
    return await get_term(conn, row.term_id)


async def search_terms(conn: "Connection", query: str, *, limit: int = 25) -> list[dict]:
    """Term lookup for the MCP surface: match on name or definition, refs included."""
    pattern = f"%{query.lower()}%"
    rows = (
        await conn.execute_core(
            select(glossary_terms)
            .where(
                glossary_terms.c.name.ilike(pattern)
                | glossary_terms.c.definition.ilike(pattern)
            )
            .order_by(glossary_terms.c.name)
            .limit(limit)
        )
    ).fetchall()
    out = []
    for r in rows:
        term = await get_term(conn, r.id)
        if term is not None:
            out.append(term)
    return out
