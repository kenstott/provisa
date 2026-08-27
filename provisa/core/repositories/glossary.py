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
A term losing its last physical ref is REMOVED unless it carries curator work (a
definition, a relationship, an expert, a retirement) or deleting it would leave an
abstract term dangling — with no path to any rooted term — in which case the term is
deprecated (kept, out of service, revived if its column returns) instead.

What any consuming surface may then offer is one rule, ``core.glossary.live_term_ids``:
in service, defined, and grounded in a physical column.
"""

# Requirements: REQ-1387, REQ-1591

from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, select, update

from provisa.core.glossary import (
    TERM_EDGE_TYPES,
    live_term_ids,
    normalize_term,
    readable_term,
)
from provisa.core.schema_org import (
    glossary_term_domains,
    glossary_term_edges,
    glossary_term_experts,
    glossary_term_refs,
    glossary_terms,
    registered_tables,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection


# ---------------------------------------------------------------------------
# Domain scope (REQ-1591)
# ---------------------------------------------------------------------------


async def term_domains(conn: "Connection") -> dict[int, set[str]]:
    """Every term's effective domains, by the one rule (REQ-1591).

    A term's domains are the distinct domains of the tables its refs point at while it HOLDS
    refs, and its declared rows otherwise. Derivation is preferred where it is possible because
    a derived answer cannot drift out of sync with the model; the declared rows carry the two
    cases derivation cannot reach — an abstract term, which has no refs by definition, and a
    rooted term whose last ref has departed, stamped at that moment by ``_settle_terms`` so a
    deprecated term stays curatable by the people who owned it.

    A term absent from the result has NO domains, which is not the same as no access: an
    unscoped term is reachable by any holder of the glossary rights (REQ-1591), and the create
    path is what prevents an empty scope being minted deliberately.
    """
    derived: dict[int, set[str]] = {}
    rows = (
        await conn.execute_core(
            select(glossary_term_refs.c.term_id, registered_tables.c.domain_id)
            .join(registered_tables, glossary_term_refs.c.table_id == registered_tables.c.id)
            .distinct()
        )
    ).fetchall()
    for r in rows:
        derived.setdefault(r.term_id, set()).add(r.domain_id)
    declared: dict[int, set[str]] = {}
    for r in (
        await conn.execute_core(
            select(glossary_term_domains.c.term_id, glossary_term_domains.c.domain_id)
        )
    ).fetchall():
        declared.setdefault(r.term_id, set()).add(r.domain_id)
    return {**declared, **derived}


async def set_declared_domains(conn: "Connection", term_id: int, domain_ids: "set[str]") -> None:
    """Replace a term's declared domains outright — the abstract term's scope, and the stamp."""
    await conn.execute_core(
        _delete(glossary_term_domains).where(glossary_term_domains.c.term_id == term_id)
    )
    for domain_id in sorted(domain_ids):
        await conn.upsert(
            glossary_term_domains,
            {"term_id": term_id, "domain_id": domain_id},
            index_elements=["term_id", "domain_id"],
            update_columns=["domain_id"],
        )


# ---------------------------------------------------------------------------
# Lifecycle (driven by the table repository)
# ---------------------------------------------------------------------------


async def sync_table_refs(
    conn: "Connection",
    table_id: int,
    columns: "list[tuple[str, str]]",
    *,
    table_context: str | None = None,
) -> None:
    """Reconcile one table's refs with its current column set, in the caller's transaction.

    ``columns`` pairs each column's physical name -- the ref's identity, which is what a query
    compiles against -- with its BUSINESS name, the alias when the modeller gave it one. The term
    derives from the business name (REQ-1581): aliasing ``usr_nm`` to ``user name`` is the stronger
    move than renaming the term it produced, because the alias travels with the data to every
    surface, while a term rename corrects one catalog entry and leaves the column still reading
    ``usr_nm`` to the next agent.

    New columns create-or-link terms by deterministic normalization (relinking a
    deprecated term revives it); departed columns drop their refs and the affected
    terms are settled under the remove-or-deprecate rule, which keeps any term a
    curator has worked on. A re-aliased column re-derives, so the glossary follows the model --
    but only while the term it currently points at is still an untouched proposal. Once a curator
    has defined, related, or staffed that term, the link is their work and an alias edit does not
    move it. ``table_context`` is the
    table's business name -- it qualifies TOO-GENERIC phrases (employees.first_name ->
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
    # REQ-1591: taken BEFORE any ref is dropped. A term's domains are derived from its refs, so
    # by the time the settle decides to deprecate one there is nothing left to derive them from.
    domains_before = await term_domains(conn)
    current = {physical for physical, _ in columns}
    departed = [c for c in existing if c not in current]
    if departed:
        await conn.execute_core(
            _delete(glossary_term_refs).where(
                (glossary_term_refs.c.table_id == table_id)
                & (glossary_term_refs.c.column_name.in_(departed))
            )
        )
    orphaned = {existing[c] for c in departed}
    graph = await _load_graph(conn) if existing else None
    for physical, business in columns:
        wanted = normalize_term(business, table_context=table_context)
        held = existing.get(physical)
        if held is not None:
            assert graph is not None
            node = graph.terms.get(held)
            if node is None or _is_curated(graph, node) or node["name"] == wanted:
                continue
            orphaned.add(held)
        term_id = await _find_or_create_term(conn, wanted)
        await conn.upsert(
            glossary_term_refs,
            {"term_id": term_id, "table_id": table_id, "column_name": physical},
            index_elements=["table_id", "column_name"],
            update_columns=["term_id"],
        )
        orphaned.discard(term_id)
    if orphaned:
        await _settle_terms(conn, orphaned, domains_before=domains_before)


async def sweep_refless_terms(conn: "Connection", *, domains_before: "dict[int, set[str]]") -> None:
    """Settle every rooted-born term whose refs are all gone (FK-cascade deletion paths).

    Table/source deletion and config purges remove registered_tables rows out of this
    repository's sight; the deletion paths call this afterwards so those terms still get
    the remove-or-deprecate rule. Refs are pruned here by a departed-table probe rather
    than FK cascade, because the SQLite backend runs without PRAGMA foreign_keys and its
    cascades never fire.

    ``domains_before`` is :func:`term_domains` taken BEFORE the caller deleted the rows, and is
    required rather than computed here (REQ-1591): a term's domains are derived by joining its
    refs to ``registered_tables``, so once the caller has removed the table row there is nothing
    left to derive from and a snapshot taken here would already be empty. Requiring it makes the
    ordering a caller cannot skip — a new deletion path has to take the snapshot to compile.
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
            .where(~glossary_terms.c.id.in_(select(glossary_term_refs.c.term_id).distinct()))
        )
    ).fetchall()
    if rows:
        await _settle_terms(conn, {r.id for r in rows}, domains_before=domains_before)


async def _find_or_create_term(conn: "Connection", name: str) -> int:
    row = (
        await conn.execute_core(select(glossary_terms).where(glossary_terms.c.name == name))
    ).fetchone()
    if row is not None:
        if row.deprecated:
            await conn.execute_core(
                update(glossary_terms).where(glossary_terms.c.id == row.id).values(deprecated=False)
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


async def _settle_terms(
    conn: "Connection", term_ids: set[int], *, domains_before: dict[int, set[str]]
) -> set[int]:
    """Apply the remove-or-deprecate rule to each candidate that is refless and rooted-born.

    Returns the ids that were removed, so a caller holding a reference to one of them
    (the admin surface, whose client has the losing term open) can be told it is gone
    rather than discovering it as a 404 on the next read.

    ``domains_before`` is the caller's snapshot of every term's domains taken BEFORE it dropped
    the refs that brought us here (REQ-1591). A term kept by this rule has no refs left to derive
    its domains from, so the snapshot is stamped onto it here; without that stamp a deprecated or
    retired term becomes unscoped, and the curators who owned it lose the right to touch it.
    """
    removed: set[int] = set()
    if not term_ids:
        return removed
    graph = await _load_graph(conn)
    for term_id in sorted(term_ids):
        node = graph.terms.get(term_id)
        if node is None or node["is_abstract"] or node["deprecated"] or node["ref_count"] > 0:
            continue
        if _is_curated(graph, node) or _dangling_grows(graph, term_id):
            await conn.execute_core(
                update(glossary_terms).where(glossary_terms.c.id == term_id).values(deprecated=True)
            )
            await set_declared_domains(conn, term_id, domains_before.get(term_id, set()))
            node["deprecated"] = True
        else:
            await conn.execute_core(
                _delete(glossary_term_edges).where(
                    (glossary_term_edges.c.from_term_id == term_id)
                    | (glossary_term_edges.c.to_term_id == term_id)
                )
            )
            await conn.execute_core(
                _delete(glossary_term_experts).where(glossary_term_experts.c.term_id == term_id)
            )
            await conn.execute_core(_delete(glossary_terms).where(glossary_terms.c.id == term_id))
            graph.remove(term_id)
            removed.add(term_id)
    return removed


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
                glossary_terms.c.name,
                glossary_terms.c.is_abstract,
                glossary_terms.c.deprecated,
                glossary_terms.c.retired,
                glossary_terms.c.definition,
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
    expert_rows = (
        await conn.execute_core(select(glossary_term_experts.c.term_id).distinct())
    ).fetchall()
    with_experts = {r.term_id for r in expert_rows}
    terms = {
        r.id: {
            "id": r.id,
            "name": r.name,
            "is_abstract": bool(r.is_abstract),
            "deprecated": bool(r.deprecated),
            "retired": bool(r.retired),
            "definition": r.definition,
            "has_expert": r.id in with_experts,
            "ref_count": counts.get(r.id, 0),
        }
        for r in term_rows
    }
    return _TermGraph(terms, [(r.from_term_id, r.to_term_id) for r in edge_rows])


async def live_ids(conn: "Connection") -> set[int]:
    """Ids of the terms a consuming surface may offer, by the shared admission rule.

    Every term holding a ref is rooted here; the exporter runs the same rule over its own,
    narrower notion of rooted (only refs whose column actually publishes).
    """
    graph = await _load_graph(conn)
    return live_term_ids(
        graph.terms.values(),
        graph.edges,
        {tid for tid, node in graph.terms.items() if node["ref_count"] > 0},
    )


def _is_curated(graph: _TermGraph, node: dict) -> bool:
    """True when a term carries curator work that losing its last column must not destroy.

    A derived term is born blank, so a definition, a relationship, or a named expert can only
    have come from a person; deleting the row would discard that silently and the next
    registration of the same column would put a blank term in its place. Such a term is
    deprecated instead — kept, out of service, and revived by ``_find_or_create_term`` if its
    column comes back. ``retired`` counts too: a curator's withdrawal is itself the work.
    """
    return bool(
        (node["definition"] or "").strip()
        or node["retired"]
        or node["has_expert"]
        or any(node["id"] in edge for edge in graph.edges)
    )


def _dangling_grows(graph: _TermGraph, term_id: int) -> bool:
    """True when removing ``term_id`` would newly disconnect an abstract term from all roots."""
    kept = graph.dangling(extra_roots=frozenset({term_id}))
    removed = graph.dangling(excluded=frozenset({term_id}))
    return bool(removed - kept)


# ---------------------------------------------------------------------------
# Curation (admin surface)
# ---------------------------------------------------------------------------


async def list_terms(
    conn: "Connection",
    *,
    q: str | None = None,
    include_deprecated: bool = True,
    domains: "frozenset[str] | None" = None,
) -> list[dict]:
    """List terms, optionally narrowed to those touching ``domains`` (REQ-1591).

    ``None`` means no narrowing — the caller's role is unlimited, the deployment is in
    single-domain mode, or every domain is selected — and is a different answer from an empty
    set, which admits only the unscoped terms and the enterprise-wide ones. Narrowing happens here
    rather than in the router because ``q`` searches over the same rows and the two must not
    disagree about the population.

    REQ-1592: the rule is ``readable_term``, not ``within_domains`` — a term scoped to ``*`` is the
    org's shared vocabulary and is listed for everyone, whatever the caller's domains.
    """
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
    # ``live`` rides the row so the admin surface shows the same admission rule the agent and
    # export surfaces enforce, rather than re-deriving it from the flags and guessing at
    # groundedness, which is a property of the graph and not of any one row.
    live = await live_ids(conn)
    scope = await term_domains(conn)
    return [
        dict(r._mapping)
        | {
            "ref_count": counts.get(r.id, 0),
            "live": r.id in live,
            "domains": sorted(scope.get(r.id, set())),
        }
        for r in rows
        if readable_term(domains, scope.get(r.id, set()))
    ]


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
    term["live"] = term_id in await live_ids(conn)
    # REQ-1591: the same rule the list and the gates use, rather than reading the refs above —
    # those are empty for an abstract term and for a deprecated one whose columns have gone,
    # which are exactly the two cases the declared rows exist to answer.
    term["domains"] = sorted((await term_domains(conn)).get(term_id, set()))
    return term


async def create_abstract_term(
    conn: "Connection", name: str, *, definition: str | None = None, domains: "set[str]"
) -> int:
    """Create an abstract term, declaring the domains it belongs to (REQ-1591).

    ``domains`` is required rather than defaulted: an abstract term holds no refs, so nothing
    derives its scope, and a term minted with an empty set is UNSCOPED — reachable by every
    holder of the glossary rights. That is a legitimate state in a single-domain deployment and
    a way around the gate in a multi-domain one, so the caller (the router, which knows the
    deployment's domain policy) decides, and the decision is never made here by omission.
    """
    name = name.strip()
    if not name:
        raise ValueError("term name is required")
    existing = (
        await conn.execute_core(select(glossary_terms.c.id).where(glossary_terms.c.name == name))
    ).fetchone()
    if existing is not None:
        raise ValueError(f"term {name!r} already exists")
    term_id = await conn.upsert_returning(
        glossary_terms,
        {"name": name, "definition": definition, "is_abstract": True, "deprecated": False},
        index_elements=["name"],
        returning="id",
        update_columns=["definition"],
    )
    await set_declared_domains(conn, term_id, domains)
    return term_id


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


async def set_retired(conn: "Connection", term_id: int, retired: bool) -> bool:
    """Retire (or un-retire) a term: the soft delete for rooted terms.

    A rooted term cannot be deleted — ``sync_table_refs`` would recreate it from the same
    column on the next registration — so retiring is how a curator takes one out of service.
    The term keeps its refs and stays editable here; ``search_terms`` and the metadata export
    both skip it, so no agent or downstream catalog can bind to it. Nothing in the derived
    lifecycle writes this column, so the retirement survives a column departing and returning.
    """
    result = await conn.execute_core(
        update(glossary_terms).where(glossary_terms.c.id == term_id).values(retired=retired)
    )
    return (result.rowcount or 0) > 0


async def set_export_excluded(conn: "Connection", term_id: int, excluded: bool) -> bool:
    """Opt a term out of (or back into) metadata export; curation is untouched."""
    result = await conn.execute_core(
        update(glossary_terms)
        .where(glossary_terms.c.id == term_id)
        .values(export_excluded=excluded)
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
        raise ValueError(
            "term has physical refs; retire it, or move its refs to another term, first"
        )
    await conn.execute_core(
        _delete(glossary_term_edges).where(
            (glossary_term_edges.c.from_term_id == term_id)
            | (glossary_term_edges.c.to_term_id == term_id)
        )
    )
    await conn.execute_core(
        _delete(glossary_term_experts).where(glossary_term_experts.c.term_id == term_id)
    )
    result = await conn.execute_core(_delete(glossary_terms).where(glossary_terms.c.id == term_id))
    return (result.rowcount or 0) > 0


async def move_ref(
    conn: "Connection", table_id: int, column_name: str, to_term_id: int
) -> dict | None:
    """Move one physical ref to another term (consolidation); the losing term is settled.

    Returns ``None`` when no such ref exists, otherwise ``{"source_term_removed": bool}`` —
    moving a term's last ref is the retire path, so the losing term is frequently deleted by
    the settle and the caller must not read it again.
    """
    row = (
        await conn.execute_core(
            select(glossary_term_refs.c.id, glossary_term_refs.c.term_id).where(
                (glossary_term_refs.c.table_id == table_id)
                & (glossary_term_refs.c.column_name == column_name)
            )
        )
    ).fetchone()
    if row is None:
        return None
    target = (
        await conn.execute_core(
            select(glossary_terms.c.id).where(glossary_terms.c.id == to_term_id)
        )
    ).fetchone()
    if target is None:
        raise ValueError(f"term {to_term_id} does not exist")
    if row.term_id == to_term_id:
        return {"source_term_removed": False}
    # REQ-1591: before the move, so a losing term the settle keeps is stamped with the domains it
    # held rather than the empty set it is left with.
    domains_before = await term_domains(conn)
    await conn.execute_core(
        update(glossary_term_refs)
        .where(glossary_term_refs.c.id == row.id)
        .values(term_id=to_term_id)
    )
    removed = await _settle_terms(conn, {row.term_id}, domains_before=domains_before)
    return {"source_term_removed": row.term_id in removed}


async def add_edge(conn: "Connection", from_term_id: int, to_term_id: int, rel_type: str) -> None:
    if rel_type not in TERM_EDGE_TYPES:
        raise ValueError(f"rel_type must be one of {TERM_EDGE_TYPES}")
    if from_term_id == to_term_id:
        raise ValueError("a term cannot relate to itself")
    for tid in (from_term_id, to_term_id):
        exists = (
            await conn.execute_core(select(glossary_terms.c.id).where(glossary_terms.c.id == tid))
        ).fetchone()
        if exists is None:
            raise ValueError(f"term {tid} does not exist")
    await conn.upsert(
        glossary_term_edges,
        {"from_term_id": from_term_id, "to_term_id": to_term_id, "rel_type": rel_type},
        index_elements=["from_term_id", "to_term_id", "rel_type"],
        update_columns=["rel_type"],
    )


async def retype_edge(
    conn: "Connection", from_term_id: int, to_term_id: int, rel_type: str, new_rel_type: str
) -> bool:
    """Change an existing relationship's type, keeping its direction and endpoints.

    ``rel_type`` is part of the edge's identity, so this is a delete and an insert rather
    than an UPDATE; doing it here keeps the pair atomic within the caller's transaction. A
    curator retyping SYNONYM_OF to KIND_OF is correcting the same statement about the same
    two terms, not withdrawing one relationship and asserting another, so the UI must not
    make them delete and re-add it.
    """
    if new_rel_type not in TERM_EDGE_TYPES:
        raise ValueError(f"rel_type must be one of {TERM_EDGE_TYPES}")
    if not await remove_edge(conn, from_term_id, to_term_id, rel_type):
        return False
    await add_edge(conn, from_term_id, to_term_id, new_rel_type)
    return True


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
        await conn.execute_core(select(glossary_terms.c.id).where(glossary_terms.c.id == term_id))
    ).fetchone()
    if exists is None:
        raise ValueError(f"term {term_id} does not exist")
    await conn.upsert(
        glossary_term_experts,
        {"term_id": term_id, "user_id": user_id, "kind": kind},
        index_elements=["term_id", "user_id"],
        update_columns=["kind"],
    )


async def term_authors(conn: "Connection", term_id: int) -> set[str]:
    """The user ids that AUTHORED this term — the stewardship half of the curation gate (REQ-1592).

    ``kind='author'`` only. An expert is a contact ("ask me about this"), and turning that list
    into an access-control list would make naming a knowledgeable colleague a hostile act: it would
    hand them the term and cost the person who named them nothing but their own authority. An
    author claimed the definition, so an author owns it.

    An empty result is the UNCLAIMED state, in which the ordinary domain rule decides — that is the
    window in which a term's first author claims it.
    """
    rows = (
        await conn.execute_core(
            select(glossary_term_experts.c.user_id).where(
                (glossary_term_experts.c.term_id == term_id)
                & (glossary_term_experts.c.kind == "author")
            )
        )
    ).fetchall()
    return {r.user_id for r in rows}


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


async def search_terms(
    conn: "Connection",
    query: str,
    *,
    limit: int = 25,
    domains: "frozenset[str] | None" = None,
) -> list[dict]:
    """Term lookup for the MCP surface: match on name or definition, refs included.

    Only live terms are returned — see ``live_term_ids`` for the admission rule. This is the
    surface an agent binds a question to a column through, so anything it offers must be
    in service, defined, and grounded in a physical column: an undefined term is a proposal,
    a retired one was withdrawn, and an ungrounded one names no data.

    The gate runs after the match rather than inside it because groundedness is a property of
    the term graph, not of any one row.

    ``domains`` narrows to the terms the caller's role may reach (REQ-1591), by the same ANY rule
    the admin list uses; ``None`` is an unlimited caller and narrows nothing. The narrowing runs
    before the limit is honoured in spirit — the row limit is applied to the match, and a caller
    seeing fewer than ``limit`` results is seeing its own scope, not the end of the vocabulary.
    """
    live = await live_ids(conn)
    if not live:
        return []
    pattern = f"%{query.lower()}%"
    rows = (
        await conn.execute_core(
            select(glossary_terms.c.id)
            .where(glossary_terms.c.id.in_(live))
            .where(
                glossary_terms.c.name.ilike(pattern) | glossary_terms.c.definition.ilike(pattern)
            )
            .order_by(glossary_terms.c.name)
            .limit(limit)
        )
    ).fetchall()
    scope = await term_domains(conn)
    out = []
    for r in rows:
        if not readable_term(domains, scope.get(r.id, set())):
            continue
        term = await get_term(conn, r.id)
        if term is not None:
            out.append(term)
    return out
