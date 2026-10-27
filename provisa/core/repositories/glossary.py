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

# Requirements: REQ-1387, REQ-1591, REQ-2130

from provisa.core import model_change
from typing import TYPE_CHECKING, Any

import logging
import re
from dataclasses import dataclass
from datetime import date

from sqlalchemy import delete as _delete, func, or_, select, update

from provisa.core.glossary import (
    TERM_EDGE_TYPES,
    grounded_term_ids,
    live_term_ids,
    normalize_term,
    readable_term,
    table_concept,
)
from provisa.compiler.sql_types import key_list
from provisa.core.models import Cardinality
from provisa.core.repositories import identity
from provisa.core.repositories.integrity import Dependent, ObjectRef, guard, remove_parts
from provisa.core.schema_org import (
    glossary_term_domains,
    glossary_term_edges,
    glossary_term_experts,
    glossary_term_refs,
    glossary_terms,
    registered_tables,
    relationships,
    table_columns,
    tag_assignments,
)

log = logging.getLogger(__name__)

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
    # REQ-1387 (GROUNDED TO A TABLE): a class term holds its tables as a rooted term holds its
    # columns, so their domains derive its own the same way.
    rows += (
        await conn.execute_core(
            select(
                registered_tables.c.class_term_id.label("term_id"), registered_tables.c.domain_id
            )
            .where(registered_tables.c.class_term_id.is_not(None))
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


def _class_term_ids():
    """The class term of every grounded table, one row per table (REQ-2130)."""
    return select(registered_tables.c.class_term_id.label("term_id")).where(
        registered_tables.c.class_term_id.is_not(None)
    )


async def _class_term_of(conn: "Connection", table_id: int) -> int | None:
    row = (
        await conn.execute_core(
            select(registered_tables.c.class_term_id).where(registered_tables.c.id == table_id)
        )
    ).first()
    return None if row is None else row.class_term_id


async def _primary_keys(conn: "Connection", table_id: int) -> set[str]:
    rows = (
        await conn.execute_core(
            select(table_columns.c.column_name).where(
                (table_columns.c.table_id == table_id) & table_columns.c.is_primary_key
            )
        )
    ).fetchall()
    return {r.column_name for r in rows}


async def _references(conn: "Connection", table_id: int) -> dict[str, int]:
    """Each foreign-key column of ``table_id`` -- every part of a composite one -- with the table
    it refers to, as declared relationships say: the many side of a many-to-one (its source
    column) or a one-to-many (its target column), and a junction's two keys."""
    rows = (
        await conn.execute_core(
            select(relationships).where(
                (relationships.c.source_table_id == table_id)
                | (relationships.c.target_table_id == table_id)
                | (relationships.c.via_table_id == table_id)
            )
        )
    ).fetchall()
    out: dict[str, int] = {}
    for r in rows:
        if r.via_table_id is not None:
            if r.via_table_id == table_id and r.target_table_id is not None:
                out.update({c: r.source_table_id for c in key_list(r.via_source_column or "")})
                out.update({c: r.target_table_id for c in key_list(r.via_target_column or "")})
            continue
        if r.target_table_id is None:
            continue
        if r.cardinality == Cardinality.many_to_one.value and r.source_table_id == table_id:
            out.update({c: r.target_table_id for c in key_list(r.source_column)})
        elif r.cardinality == Cardinality.one_to_many.value and r.target_table_id == table_id:
            out.update({c: r.source_table_id for c in key_list(r.target_column or "")})
    return out


async def _key_targets(
    conn: "Connection", table_id: int, *, assume: "dict[int, int] | None" = None
) -> dict[str, int]:
    """Where each key column of ``table_id`` puts its ref (REQ-2141, KEYS ON THE CLASS): a
    primary-key column on its table's Class term, a foreign key -- by its declared many-to-one
    relationship, whatever the column is called -- on the target table's Class term, which wins
    for a primary key that is also a foreign key. A table with no Class contributes nothing.
    ``assume`` maps a table to the Class it is about to have."""
    assume = assume or {}

    async def class_of(tid: int) -> int | None:
        return assume[tid] if tid in assume else await _class_term_of(conn, tid)

    out: dict[str, int] = {}
    own = await class_of(table_id)
    if own is not None:
        out = {col: own for col in await _primary_keys(conn, table_id)}
    for col, target in (await _references(conn, table_id)).items():
        parent = await class_of(target)
        if parent is not None:
            out[col] = parent
    return out


async def _property_term(conn: "Connection", business: str, table_context: str | None) -> int:
    """The Property term a column that is no key derives (REQ-1387): its normalized phrase; when
    that names a Class, the phrase qualified with its own table's concept -- a column with no
    declared relationship is not its Class's (REQ-2141). TermIsClass when that too is a Class."""
    wanted = normalize_term(business, table_context=table_context)
    term_id = await _find_or_create_term(conn, wanted)
    if table_context is None or not await _term_table_ids(conn, term_id):
        return term_id
    concept = table_concept(table_context)
    qualified = wanted if wanted.startswith(f"{concept} ") else f"{concept} {wanted}"
    if qualified == wanted:
        await refuse_property_on_class(conn, term_id)
    term_id = await _find_or_create_term(conn, qualified)
    await refuse_property_on_class(conn, term_id)
    return term_id


async def draft_class_terms(conn: "Connection") -> set[int]:
    """The terms grounding a table only as sync's drafts and carrying no curator work: derived
    (REQ-2141). A term a configuration declared or a person grounded, or a curated one, is not."""
    rows = (
        await conn.execute_core(
            select(registered_tables.c.class_term_id, registered_tables.c.class_term_derived).where(
                registered_tables.c.class_term_id.is_not(None)
            )
        )
    ).fetchall()
    drafts = {r.class_term_id for r in rows if r.class_term_derived} - {
        r.class_term_id for r in rows if not r.class_term_derived
    }
    if not drafts:
        return set()
    graph = await _load_graph(conn)
    return {t for t in drafts if t in graph.terms and not _is_curated(graph, graph.terms[t])}


async def _concept_of(conn: "Connection", table_id: int) -> str:
    row = (
        await conn.execute_core(
            select(registered_tables.c.alias, registered_tables.c.table_name).where(
                registered_tables.c.id == table_id
            )
        )
    ).fetchone()
    return table_concept(row.alias or row.table_name)


async def _derive_class(conn: "Connection", table_id: int, concept: str) -> int | None:
    """Ground a keyed table in a draft Class term named for its concept (REQ-2141): the term of
    that name when it holds no non-key column ref (it may already ground other tables of the same
    concept), else a new one. None -- no draft -- when the concept is already a Property with
    non-key refs; the class_and_attribute finding reports that table."""
    row = (
        await conn.execute_core(select(glossary_terms.c.id).where(glossary_terms.c.name == concept))
    ).fetchone()
    if row is None:
        term_id = await _find_or_create_term(conn, concept)
    elif await _non_key_refs(conn, row.id, also_grounding=table_id):
        return None
    else:
        term_id = row.id
    await conn.execute_core(
        update(registered_tables)
        .where(registered_tables.c.id == table_id)
        .values(class_term_id=term_id, class_term_derived=True)
    )
    return term_id


async def rehome_keys(
    conn: "Connection", table_ids: "set[int]", *, keep: "frozenset[int]" = frozenset()
) -> None:
    """Re-settle the key refs of ``table_ids`` and of every table referring to one of them
    (REQ-2141): a key column's ref goes to the Class its key targets name; a ref on a Class that
    is no longer its key goes back to the Property its name derives. A ref on a curated Property
    stays, as a curated column term does. The terms refs leave are settled, but for ``keep``.
    Run when a table's Class changes and when a relationship is saved or removed."""
    referring = (
        await conn.execute_core(
            select(relationships.c.source_table_id).where(
                relationships.c.target_table_id.in_(table_ids)
            )
        )
    ).fetchall()
    tables = set(table_ids) | {r.source_table_id for r in referring}
    domains_before = await term_domains(conn)
    graph = await _load_graph(conn)
    left: set[int] = set()
    for table_id in sorted(tables):
        targets = await _key_targets(conn, table_id)
        rows = (
            await conn.execute_core(
                select(
                    glossary_term_refs.c.id,
                    glossary_term_refs.c.term_id,
                    glossary_term_refs.c.column_name,
                    table_columns.c.alias,
                    registered_tables.c.alias.label("table_alias"),
                    registered_tables.c.table_name,
                )
                .join(
                    table_columns,
                    (table_columns.c.table_id == glossary_term_refs.c.table_id)
                    & (table_columns.c.column_name == glossary_term_refs.c.column_name),
                )
                .join(registered_tables, glossary_term_refs.c.table_id == registered_tables.c.id)
                .where(glossary_term_refs.c.table_id == table_id)
            )
        ).fetchall()
        for r in rows:
            node = graph.terms.get(r.term_id)
            # a term just ungrounded (``keep``) held its refs as a Class
            held_is_class = r.term_id in keep or bool(await _term_table_ids(conn, r.term_id))
            target = targets.get(r.column_name)
            if target is None:
                if not held_is_class:
                    continue  # a Property, where a column with no key belongs
                target = await _property_term(
                    conn, r.alias or r.column_name, r.table_alias or r.table_name
                )
            elif not held_is_class and node is not None and _is_curated(graph, node):
                continue
            if target != r.term_id:
                await conn.execute_core(
                    update(glossary_term_refs)
                    .where(glossary_term_refs.c.id == r.id)
                    .values(term_id=target)
                )
                left.add(r.term_id)
    if left - keep:
        await _settle_terms(conn, left - keep, domains_before=domains_before)


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
    but only while the term it currently points at is still an untouched draft. Once a curator
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
    # REQ-2141 (TABLE PROPOSALS, KEYS ON THE CLASS): the table's Class term -- its grounding, or a
    # draft derived now from its concept -- and the key columns whose refs belong on a Class.
    if await _class_term_of(conn, table_id) is None and table_context is not None:
        if await _primary_keys(conn, table_id):
            await _derive_class(conn, table_id, table_concept(table_context))
    targets = await _key_targets(conn, table_id)
    graph = await _load_graph(conn) if existing else None
    for physical, business in columns:
        target = targets.get(physical)
        wanted = normalize_term(business, table_context=table_context)
        held = existing.get(physical)
        if held is not None:
            assert graph is not None
            node = graph.terms.get(held)
            if node is None or held == target:
                continue
            on_class = held in {t for t in targets.values()} or bool(
                await _term_table_ids(conn, held)
            )
            if (
                target is None
                and not on_class
                and (_is_curated(graph, node) or node["name"] == wanted)
            ):
                continue
            if target is not None and not on_class and _is_curated(graph, node):
                continue
            orphaned.add(held)
        # a key is its Class's, whatever the column is called; any other column is a Property
        term_id = (
            target if target is not None else await _property_term(conn, business, table_context)
        )
        await conn.upsert(
            glossary_term_refs,
            {"term_id": term_id, "table_id": table_id, "column_name": physical},
            index_elements=["table_id", "column_name"],
            update_columns=["term_id"],
        )
        orphaned.discard(term_id)
    if orphaned:
        await _settle_terms(conn, orphaned, domains_before=domains_before)


@dataclass(frozen=True)
class RemovalSnapshot:
    """What a deletion path reads BEFORE it removes rows its refs depend on (REQ-1591): every
    term's domains, and the terms holding column refs. The sweep after it settles the terms that
    held refs and hold none now; a term that never held one is a Concept and is not settled."""

    domains: dict[int, set[str]]
    held_refs: frozenset[int]


async def before_ref_removal(conn: "Connection") -> RemovalSnapshot:
    held = {
        r.term_id
        for r in (
            await conn.execute_core(select(glossary_term_refs.c.term_id).distinct())
        ).fetchall()
    }
    # REQ-2141: a Class holds its tables' keys, but only a draft is derived and goes with
    # them; a declared or curated Class is kept, a Concept again.
    grounded = {
        r.term_id for r in (await conn.execute_core(_class_term_ids().distinct())).fetchall()
    }
    curated = grounded - await draft_class_terms(conn)
    return RemovalSnapshot(await term_domains(conn), frozenset(held - curated))


async def sweep_refless_terms(conn: "Connection", *, before: RemovalSnapshot) -> None:
    """Settle every term that held refs before a deletion and holds none now (FK-cascade paths).

    Table/source deletion and config purges remove registered_tables rows out of this
    repository's sight; the deletion paths call this afterwards so those terms still get
    the remove-or-deprecate rule. Refs are pruned here by a departed-table probe rather
    than FK cascade, because the SQLite backend runs without PRAGMA foreign_keys and its
    cascades never fire.

    ``before`` is :func:`before_ref_removal`, taken BEFORE the caller deleted the rows, and is
    required rather than computed here (REQ-1591): a term's domains are derived by joining its
    refs to ``registered_tables``, so once the caller has removed the table row there is nothing
    left to derive from and a snapshot taken here would already be empty. It also says which
    terms held refs: a term with none is a Concept unless it lost them (REQ-2141). Requiring it
    makes the ordering a caller cannot skip — a new deletion path has to take the snapshot to
    compile.
    """
    await conn.execute_core(
        _delete(glossary_term_refs).where(
            ~glossary_term_refs.c.table_id.in_(select(registered_tables.c.id))
        )
    )
    still = {
        r.term_id
        for r in (
            await conn.execute_core(select(glossary_term_refs.c.term_id).distinct())
        ).fetchall()
    }
    lost = set(before.held_refs) - still
    if lost:
        await _settle_terms(conn, lost, domains_before=before.domains)


async def _find_or_create_term(conn: "Connection", name: str) -> int:
    """Attach a column ref's term by name, creating it if needed; a deprecated one is revived.
    (Its role follows from the ref it gains: REQ-2141.)"""
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
        {"name": name, "deprecated": False},
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
        if node is None or node["deprecated"] or node["rooted"]:
            continue
        if _is_curated(graph, node) or _dangling_grows(graph, term_id):
            await conn.execute_core(
                update(glossary_terms).where(glossary_terms.c.id == term_id).values(deprecated=True)
            )
            await set_declared_domains(conn, term_id, domains_before.get(term_id, set()))
            node["deprecated"] = True
        else:
            # The term lost its last ref, so nothing blocks it; its edges, domain links and
            # experts go with it (the inventory's parts — no cascade is relied on).
            await remove_parts(conn, ObjectRef("glossary_term", term_id))
            await conn.execute_core(_delete(glossary_terms).where(glossary_terms.c.id == term_id))
            graph.remove(term_id)
            removed.add(term_id)
    return removed


def _is_concept(node: dict) -> bool:
    """A Concept, in service: rooted in no column and no table, and not a deprecated term that
    lost its columns (REQ-2141)."""
    return not node["rooted"] and not node["deprecated"]


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
        """Concept terms (in service, rooted in nothing) with no undirected path to any rooted term.

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
            if tid not in excluded and (node["rooted"] or tid in extra_roots)
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
            if _is_concept(node) and tid not in excluded and tid not in reached
        }


async def _load_graph(conn: "Connection") -> _TermGraph:
    term_rows = (
        await conn.execute_core(
            select(
                glossary_terms.c.id,
                glossary_terms.c.name,
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
    # REQ-1387 (GROUNDED TO A TABLE): a term grounding a table is rooted as one holding a column.
    classes = {
        r.term_id for r in (await conn.execute_core(_class_term_ids().distinct())).fetchall()
    }
    edge_rows = (
        await conn.execute_core(
            select(glossary_term_edges.c.from_term_id, glossary_term_edges.c.to_term_id)
        )
    ).fetchall()
    expert_rows = (
        await conn.execute_core(select(glossary_term_experts.c.term_id).distinct())
    ).fetchall()
    with_experts = {r.term_id for r in expert_rows}
    # REQ-2145: a tag on a term is a curator's.
    tagged = {
        r.term_id
        for r in (
            await conn.execute_core(
                select(tag_assignments.c.term_id)
                .where(tag_assignments.c.object_type == "term")
                .distinct()
            )
        ).fetchall()
    }
    terms = {
        r.id: {
            "id": r.id,
            "name": r.name,
            "deprecated": bool(r.deprecated),
            "retired": bool(r.retired),
            "definition": r.definition,
            "has_expert": r.id in with_experts,
            "tagged": r.id in tagged,
            "ref_count": counts.get(r.id, 0),
            "rooted": counts.get(r.id, 0) > 0 or r.id in classes,
        }
        for r in term_rows
    }
    return _TermGraph(terms, [(r.from_term_id, r.to_term_id) for r in edge_rows])


async def live_ids(conn: "Connection") -> set[int]:
    """Ids of the terms a consuming surface may offer, by the shared admission rule.

    Every term holding a ref -- to a column or a table -- is rooted here; the exporter runs the same rule over its own,
    narrower notion of rooted (only refs whose column actually publishes).
    """
    graph = await _load_graph(conn)
    return live_term_ids(
        graph.terms.values(),
        graph.edges,
        {tid for tid, node in graph.terms.items() if node["rooted"]},
    )


async def grounded_ids(conn: "Connection") -> set[int]:
    """Ids of terms with a structural path to a column, ignoring ``definition``.

    Used by the curation surface to flag a dangling abstract term -- one with no edge chain
    reaching any physical ref -- distinctly from one that is merely undefined.
    """
    graph = await _load_graph(conn)
    return grounded_term_ids(
        graph.terms.values(),
        graph.edges,
        {tid for tid, node in graph.terms.items() if node["rooted"]},
    )


def _is_curated(graph: _TermGraph, node: dict) -> bool:
    """True when a term carries curator work that losing its last column must not destroy.

    A derived term is born blank, so a definition, a relationship, or a named expert can only
    have come from a person; deleting the row would discard that silently and the next
    registration of the same column would put a blank term in its place. Such a term is
    deprecated instead — kept, out of service, and revived by ``_find_or_create_term`` if its
    column comes back. ``retired`` counts too: a curator's withdrawal is itself the work, and so
    does a tag on the term (REQ-2145): sync never re-derives the term's columns away from it.
    """
    return bool(
        (node["definition"] or "").strip()
        or node["retired"]
        or node["has_expert"]
        or node["tagged"]
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


_SEARCH_ITEM = re.compile(r'"([^"]*)"|(\S+)')


def search_items(q: str) -> list[str]:
    """The items a glossary search is made of: a phrase in double quotes is one item, spaces and
    all; every other word is one item; stray quote marks are dropped. A term matches the search
    when any item is in it -- the same rule the Ontology View's graph applies (searchItems.ts)."""
    out: list[str] = []
    for phrase, word in _SEARCH_ITEM.findall(q):
        item = (phrase if phrase else word.replace('"', "")).strip().lower()
        if item:
            out.append(item)
    return out


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
    items = search_items(q or "")
    if items:
        # Any item matching the name or the definition admits the term (the search is an OR).
        stmt = stmt.where(
            or_(
                *(
                    glossary_terms.c.name.ilike(f"%{i}%")
                    | glossary_terms.c.definition.ilike(f"%{i}%")
                    for i in items
                )
            )
        )
    if not include_deprecated:
        stmt = stmt.where(~glossary_terms.c.deprecated)
    rows = (await conn.execute_core(stmt.order_by(glossary_terms.c.name))).fetchall()
    ref_rows = (await conn.execute_core(select(glossary_term_refs.c.term_id))).fetchall()
    counts: dict[int, int] = {}
    for r in ref_rows:
        counts[r.term_id] = counts.get(r.term_id, 0) + 1
    # REQ-2141: the tables a class term grounds, counted beside its columns in one query.
    table_rows = (await conn.execute_core(_class_term_ids())).fetchall()
    table_counts: dict[int, int] = {}
    for r in table_rows:
        table_counts[r.term_id] = table_counts.get(r.term_id, 0) + 1
    # ``live`` rides the row so the admin surface shows the same admission rule the agent and
    # export surfaces enforce, rather than re-deriving it from the flags and guessing at
    # groundedness, which is a property of the graph and not of any one row.
    live = await live_ids(conn)
    grounded = await grounded_ids(conn)
    gaps = await identity.identity_gap_ids(conn)  # REQ-2130: grounded in tables not all joined
    from provisa.core.glossary_findings import findings as _findings

    found = await _findings(conn)  # REQ-2146: counted on the row, so a list filters on it
    scope = await term_domains(conn)
    return [
        dict(r._mapping)
        | {
            "ref_count": counts.get(r.id, 0),
            "table_count": table_counts.get(r.id, 0),
            # REQ-2141: derived -- a term rooted in no column and no table is a Concept.
            "is_abstract": not counts.get(r.id) and not table_counts.get(r.id),
            "status": status_of(r, live),  # REQ-2144
            "findings": len(found.get(r.id, [])),  # REQ-2146
            "identity_gap": r.id in gaps,
            "live": r.id in live,
            "grounded": r.id in grounded,
            "domains": sorted(scope.get(r.id, set())),
        }
        for r in rows
        if readable_term(domains, scope.get(r.id, set()))
    ]


async def bindings(conn: "Connection") -> list[tuple[int, int, str | None]]:
    """Every (term id, table id, column) a term is bound to: its column refs, and with column
    None the tables it grounds as a class (REQ-2141)."""
    refs = (
        await conn.execute_core(
            select(
                glossary_term_refs.c.term_id,
                glossary_term_refs.c.table_id,
                glossary_term_refs.c.column_name,
            )
        )
    ).fetchall()
    classes = (
        await conn.execute_core(
            select(registered_tables.c.class_term_id, registered_tables.c.id).where(
                registered_tables.c.class_term_id.is_not(None)
            )
        )
    ).fetchall()
    return [(r[0], r[1], r[2]) for r in refs] + [(r[0], r[1], None) for r in classes]


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
    term["ref_count"] = len(term["refs"])
    # REQ-2141: the tables this term grounds as a class.
    term["table_count"] = (
        await conn.execute_core(
            select(func.count()).where(registered_tables.c.class_term_id == term_id)
        )
    ).scalar_one()
    term["is_abstract"] = not term["ref_count"] and not term["table_count"]  # REQ-2141: derived
    term["status"] = status_of(row, await live_ids(conn))  # REQ-2144
    term["identity_gap"] = term_id in await identity.identity_gap_ids(conn)
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
    term["tags"] = await term_tags(conn, term_id)  # REQ-2145
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
    # REQ-1844: every term name in this catalog is lowercase — auto-derived ones already are
    # (normalize_term case-folds), so a manually-created one that isn't would be the one
    # inconsistent entry, and silently invite a same-word duplicate differing only in case
    # ("Sepal Length" alongside "sepal length"). Never left to the caller to remember.
    model_change.name("create", "glossary term", name)  # REQ-1524
    name = name.strip().lower()
    if not name:
        raise ValueError("term name is required")
    existing = (
        await conn.execute_core(select(glossary_terms.c.id).where(glossary_terms.c.name == name))
    ).fetchone()
    if existing is not None:
        raise ValueError(f"term {name!r} already exists")
    term_id = await conn.upsert_returning(
        glossary_terms,
        {
            "name": name,
            "definition": definition,
            "deprecated": False,
        },
        index_elements=["name"],
        returning="id",
        update_columns=["definition"],
    )
    await set_declared_domains(conn, term_id, domains)
    return term_id


async def upsert_declared_term(
    conn: "Connection",
    name: str,
    *,
    definition: str | None = None,
    domains: "set[str]",
) -> int:
    """Upsert a config-declared glossary term by name (REQ-1641).

    Config reload is idempotent, unlike the curator's "new term" action: a name already
    present in the table -- whether auto-derived from a column ref or declared by a prior
    load of this same config -- gets its definition (re)set in place rather than rejected.
    This is also how a config entry grounds an already-physical term: naming an existing
    rooted term adds a definition without touching its column refs.
    """
    model_change.name("upsert", "glossary term", name)  # REQ-1524
    name = name.strip().lower()  # REQ-1844: every term name in this catalog is lowercase
    if not name:
        raise ValueError("term name is required")
    held = (
        await conn.execute_core(select(glossary_terms.c.id).where(glossary_terms.c.name == name))
    ).fetchone()
    if held is not None:
        await _refuse_clearing_tagged(conn, held.id, definition)  # REQ-2145
    term_id = await conn.upsert_returning(
        glossary_terms,
        {
            "name": name,
            "definition": definition,
            "deprecated": False,
        },
        index_elements=["name"],
        returning="id",
        update_columns=["definition"],
    )
    await set_declared_domains(conn, term_id, domains)
    return term_id


async def rename_term(conn: "Connection", term_id: int, new_name: str) -> bool:
    new_name = new_name.strip().lower()  # REQ-1844: every term name in this catalog is lowercase
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


class TermIsTagged(ValueError):
    """A definition cleared on a term that carries tags (REQ-2145): an undefined term governs
    nothing, so clearing it would take the tags off every column the term means, unannounced."""

    code = "tag.term_is_tagged"

    def __init__(self, term: str, tags: list[str]) -> None:
        super().__init__(f"Unmark {', '.join(tags)} first, or keep a definition")
        self.term = term
        self.tags = tags


async def _refuse_clearing_tagged(conn: "Connection", term_id: int, definition: str | None) -> None:
    if (definition or "").strip():
        return
    tags = [t["tag_id"] for t in await term_tags(conn, term_id)]
    if tags:
        raise TermIsTagged(await _term_name(conn, term_id), tags)


async def set_definition(conn: "Connection", term_id: int, definition: str | None) -> bool:
    await _refuse_clearing_tagged(conn, term_id, definition)  # REQ-2145
    result = await conn.execute_core(
        update(glossary_terms).where(glossary_terms.c.id == term_id).values(definition=definition)
    )
    return (result.rowcount or 0) > 0


# ---------------------------------------------------------------------------
# Status (REQ-2144): draft | live | deprecated | retired
# ---------------------------------------------------------------------------

TERM_STATUSES = ("draft", "live", "deprecated", "retired")


class StatusRefused(ValueError):
    """A status change refused by name: ``code`` (glossary.<code>) and its ``params``."""

    def __init__(self, code: str, message: str, **params: Any) -> None:
        super().__init__(message)
        self.code = code
        self.params = params


def status_of(row: Any, live: "set[int]") -> str:
    """A term's status from its row and the admission rule: retired and deprecated (automatic or
    asserted) as stored, else live when admitted, else draft."""
    if row.retired:
        return "retired"
    if row.deprecated or row.deprecation_asserted:
        return "deprecated"
    return "live" if row.id in live else "draft"


async def term_statuses(conn: "Connection") -> dict[int, str]:
    live = await live_ids(conn)
    rows = (
        await conn.execute_core(
            select(
                glossary_terms.c.id,
                glossary_terms.c.retired,
                glossary_terms.c.deprecated,
                glossary_terms.c.deprecation_asserted,
            )
        )
    ).fetchall()
    return {r.id: status_of(r, live) for r in rows}


def _reason(reason: "str | None") -> str:
    text = (reason or "").strip()
    if not text:
        raise StatusRefused("reason_required", "a reason is required")
    return text


async def deprecate_term(
    conn: "Connection",
    term_id: int,
    *,
    reason: "str | None",
    retire_on: "date | None",
    by: "str | None",
    today: "date",
) -> None:
    """A curator deprecates a live term (REQ-2144): it stays in service -- it still conducts the
    admission rule -- until ``retire_on``, when the scheduler retires it."""
    text = _reason(reason)
    if retire_on is not None and retire_on < today:
        raise StatusRefused(
            "retire_on_past",
            f"the planned retirement date {retire_on.isoformat()} is in the past",
            retire_on=retire_on.isoformat(),
        )
    status = (await term_statuses(conn)).get(term_id)
    if status is None:
        raise TermNotFound(f"term {term_id} not found")
    if status != "live":
        raise StatusRefused(
            "term_not_live", f"only a live term is deprecated; this one is {status}", status=status
        )
    await conn.execute_core(
        update(glossary_terms)
        .where(glossary_terms.c.id == term_id)
        .values(
            deprecation_asserted=True,
            status_reason=text,
            retire_on=retire_on,
            status_by=by,
            status_at=func.now(),
        )
    )
    model_change.name("deprecate", "glossary term", str(term_id))


#: A retired term's successor takes its role (REQ-2144): a Class's is a Class or a Concept, a
#: Property's a Property or a Concept.
_SUCCESSOR_ROLES = {"class": {"class", "concept"}, "property": {"property", "concept"}}


async def _move_to_successor(conn: "Connection", term_id: int, successor: int, role: str) -> None:
    """Everything attached to a retiring term goes to its successor (REQ-2144): a Class's table
    groundings, whose keys follow their tables, or a Property's column refs. THE move a retirement
    makes; the move check on tags (ci-fix's) is called here."""
    if role == "class":
        tables = await _term_table_ids(conn, term_id)
        await conn.execute_core(
            update(registered_tables)
            .where(registered_tables.c.class_term_id == term_id)
            .values(class_term_id=successor, class_term_derived=False)
        )
        # the retiring term is kept, as the record of what it was, not settled away
        await rehome_keys(conn, tables, keep=frozenset({term_id}))
    else:
        await conn.execute_core(
            update(glossary_term_refs)
            .where(glossary_term_refs.c.term_id == term_id)
            .values(term_id=successor)
        )


async def retire_term(
    conn: "Connection",
    term_id: int,
    *,
    reason: "str | None",
    successor: "int | None",
    by: "str | None",
) -> None:
    """A curator retires a term (REQ-2144). Its column refs, or its table groundings with their
    keys, move to ``successor`` -- required when anything is attached, and of a role that fits --
    and the term is kept as an inert record: its definition, edges and experts, the REPLACES edge
    successor -> term, and the reason. A successor that is the term itself, or retired, is
    refused."""
    text = _reason(reason)
    statuses = await term_statuses(conn)
    if term_id not in statuses:
        raise TermNotFound(f"term {term_id} not found")
    role = await term_role(conn, term_id)
    if successor is None and role != "concept":
        raise StatusRefused(
            "successor_required",
            f"the term holds {'tables' if role == 'class' else 'columns'}; retiring it moves them "
            "to a successor, which is required",
            role=role,
        )
    if successor is not None:
        if successor == term_id:
            raise StatusRefused("successor_is_self", "a term cannot succeed itself")
        if successor not in statuses:
            raise TermNotFound(f"term {successor} not found")
        name = await _term_name(conn, successor)
        if statuses[successor] == "retired":
            raise StatusRefused(
                "successor_retired", f"the successor {name!r} is retired", successor=name
            )
        successor_role = await term_role(conn, successor)
        if role != "concept" and successor_role not in _SUCCESSOR_ROLES[role]:
            raise StatusRefused(
                "successor_role",
                f"a {role}'s successor is a {role} or a concept; {name!r} is a {successor_role}",
                role=role,
                successor=name,
                successor_role=successor_role,
            )
        if role != "concept":
            await _move_to_successor(conn, term_id, successor, role)
    await conn.execute_core(
        update(glossary_terms)
        .where(glossary_terms.c.id == term_id)
        .values(
            retired=True,
            deprecation_asserted=False,
            retire_on=None,
            status_reason=text,
            status_by=by,
            status_at=func.now(),
        )
    )
    if successor is not None:
        await add_edge(conn, successor, term_id, "REPLACES")
    model_change.name("retire", "glossary term", str(term_id))


async def restore_term(conn: "Connection", term_id: int, *, by: "str | None") -> bool:
    """Clear what a curator asserted -- a retirement or a deprecation (REQ-2144); the term's
    status is computed again. A retired term comes back with its record -- definition, edges,
    experts -- and nothing attached (its columns and tables went to its successor), less the
    REPLACES edge its retirement wrote: an empty Concept, a draft. False when there is no such
    term."""
    result = await conn.execute_core(
        update(glossary_terms)
        .where(glossary_terms.c.id == term_id)
        .values(
            retired=False,
            deprecation_asserted=False,
            retire_on=None,
            status_reason=None,
            status_by=by,
            status_at=func.now(),
        )
    )
    # Back in service, it is replaced by nothing: the succession its retirement wrote goes, so
    # it does not reach data through its successor -- it comes back an empty Concept, a draft.
    await conn.execute_core(
        _delete(glossary_term_edges).where(
            (glossary_term_edges.c.to_term_id == term_id)
            & (glossary_term_edges.c.rel_type == "REPLACES")
        )
    )
    model_change.name("restore", "glossary term", str(term_id))
    return (result.rowcount or 0) > 0


async def retire_due(conn: "Connection", today: "date") -> list[int]:
    """Retire every deprecated term whose planned retirement date has come (REQ-2144), keeping
    the deprecation's reason. A term that still holds columns or tables needs a successor to
    take them, which a date cannot name: it stays deprecated, a retirement_blocked finding
    (REQ-2146). Returns the ids retired."""
    due = (
        await conn.execute_core(
            select(glossary_terms.c.id, glossary_terms.c.name)
            .where(glossary_terms.c.deprecation_asserted & ~glossary_terms.c.retired)
            .where(glossary_terms.c.retire_on <= today)
        )
    ).fetchall()
    retired = []
    for r in due:
        if await term_role(conn, r.id) != "concept":
            log.warning(
                "term %r is due for retirement but still holds columns or tables; it stays "
                "deprecated, a retirement_blocked finding (REQ-2146)",
                r.name,
            )
            continue
        await conn.execute_core(
            update(glossary_terms)
            .where(glossary_terms.c.id == r.id)
            .values(retired=True, deprecation_asserted=False, status_at=func.now())
        )
        retired.append(r.id)
    return retired


async def set_export_excluded(conn: "Connection", term_id: int, excluded: bool) -> bool:
    """Opt a term out of (or back into) metadata export; curation is untouched."""
    result = await conn.execute_core(
        update(glossary_terms)
        .where(glossary_terms.c.id == term_id)
        .values(export_excluded=excluded)
    )
    return (result.rowcount or 0) > 0


class TermDeleteRefused(ValueError):
    """A term that may not be deleted because it has physical refs; ``dependents`` lists the
    tables whose columns it is rooted in."""

    def __init__(self, term_id: int, dependents: list[Dependent]) -> None:
        self.term_id = term_id
        self.dependents = dependents
        # What a refusal says before naming its dependents (REQ-2152: the API layer names only
        # those its caller may read).
        self.lead = "The term has physical refs in"
        super().__init__(
            "term has physical refs; retire it, or move its refs to another term, first"
        )


async def delete_term(conn: "Connection", term_id: int) -> bool:  # REQ-1918
    """Delete a term with no physical refs (abstract or deprecated): THE delete, for every
    surface. False when there is no such term.

    Rooted terms are lifecycle-managed: their removal happens only when the schema element
    departs. So a term's physical refs block it (:class:`TermDeleteRefused`, naming the tables)
    — something that points an object at real data is a dependent, not a part. Its edges, in
    either direction, its domain links and its experts go with it. One transaction."""
    ref = ObjectRef("glossary_term", term_id)
    async with conn.transaction():
        found = await conn.execute_core(
            select(glossary_terms.c.id).where(glossary_terms.c.id == term_id)
        )
        if found.fetchone() is None:
            return False
        # REQ-2141: a Class's tables are ungrounded first, so the primary-key refs it holds for
        # them go back to their own terms rather than blocking the delete.
        for table_id in sorted(await _term_table_ids(conn, term_id)):
            await unground_table(conn, term_id, table_id)
        blocking = await guard(conn, ref)
        if blocking:
            raise TermDeleteRefused(term_id, blocking)
        await remove_parts(conn, ref)
        await conn.execute_core(_delete(glossary_terms).where(glossary_terms.c.id == term_id))
    return True


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
    # REQ-2141: a key's ref is placed by derivation alone, and a Class takes no ref by hand.
    key_class = (await _key_targets(conn, table_id)).get(column_name)
    if key_class is not None:
        table = (
            await conn.execute_core(
                select(*_TABLE_ADDRESS[:3]).where(registered_tables.c.id == table_id)
            )
        ).fetchone()
        raise KeyColumnDerived(column_name, _address(table), await _term_name(conn, key_class))
    await refuse_property_on_class(conn, to_term_id)
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


async def term_edges(conn: "Connection") -> list[tuple[int, int, str]]:  # REQ-2135
    """Every edge between terms: (from term id, to term id, relationship type)."""
    rows = await conn.execute_core(
        select(
            glossary_term_edges.c.from_term_id,
            glossary_term_edges.c.to_term_id,
            glossary_term_edges.c.rel_type,
        )
    )
    return [(r.from_term_id, r.to_term_id, r.rel_type) for r in rows.fetchall()]


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


async def axioms(conn: "Connection") -> list[dict]:  # REQ-2163
    """The ontology's RDFS/OWL/SKOS statements, one per row of the ontology schema's _axioms
    view: the view states them, so its rule is the one every reader of axioms gets."""
    from sqlalchemy import text

    rows = await conn.execute_core(
        text(
            "SELECT subject_term_id, predicate, object_term_id, object_iri, object_literal "
            "FROM _axioms_ontology ORDER BY id"
        )
    )
    return [dict(r._mapping) for r in rows.fetchall()]


async def export_graph(conn: "Connection") -> dict:
    """The whole term graph in one shape for the metadata-export builder (REQ-1387):
    terms, column and table refs joined to their table's config identity, edges, and experts."""
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
    # REQ-1387 (GROUNDED TO A TABLE): a class term's tables, by their config identity.
    table_refs = [
        dict(r._mapping)
        for r in (
            await conn.execute_core(
                select(
                    registered_tables.c.class_term_id.label("term_id"),
                    registered_tables.c.source_id,
                    registered_tables.c.schema_name,
                    registered_tables.c.table_name,
                ).where(registered_tables.c.class_term_id.is_not(None))
            )
        ).fetchall()
    ]
    # REQ-2141: abstract is derived -- a term rooted in no column and no table is a Concept.
    rooted = {r["term_id"] for r in refs} | {r["term_id"] for r in table_refs}
    statuses = await term_statuses(conn)  # REQ-2144
    for term in terms:
        term["is_abstract"] = term["id"] not in rooted
        term["status"] = statuses[term["id"]]
    # REQ-1591: each term's domains, by the one rule -- a catalog that files terms by domain
    # (Purview's Unified Catalog) places them from these.
    domains = [
        {"term_id": term_id, "domain_id": domain_id}
        for term_id, held in sorted((await term_domains(conn)).items())
        for domain_id in sorted(held)
    ]
    return {
        "terms": terms,
        "refs": refs,
        "table_refs": table_refs,
        "edges": edges,
        "experts": experts,
        "domains": domains,
    }


# ---------------------------------------------------------------------------
# Class grounding: a term names what a whole table's rows are (REQ-2130)
# ---------------------------------------------------------------------------


class TermNotFound(LookupError):
    pass


class TableNotFound(LookupError):
    pass


class TableHasNoPrimaryKey(ValueError):
    def __init__(self, table: str) -> None:
        super().__init__(f"table {table} has no primary key")
        self.table = table


class TableAlreadyGrounded(ValueError):
    def __init__(self, table: str, term_id: int, term_name: str) -> None:
        super().__init__(f"table {table} is already grounded by term {term_name!r}")
        self.table = table
        self.term_id = term_id
        self.term_name = term_name


# REQ-2141 (amended 2026-10-23): a term's ontology role is exclusive. It is a Class, grounded in
# tables, or a Property, owning column refs -- never both, so a term names one thing.
_ROLES_EXCLUSIVE = (
    "a term is either a Class, grounded in tables, or a Property, owning column refs, never both"
)


class TermIsProperty(ValueError):
    """A table grounding refused: the term owns column refs (it is a Property)."""

    def __init__(self, term: str, columns: list[str]) -> None:
        super().__init__(
            f"term {term!r} owns column refs ({', '.join(columns)}), so it cannot ground a table: "
            + _ROLES_EXCLUSIVE
        )
        self.term = term
        self.columns = columns


class KeyColumnDerived(ValueError):
    """A move refused: the column is a key, whose ref is placed by derivation on its table's (or
    its target's) Class term, never by hand (REQ-2141)."""

    def __init__(self, column: str, table: str, cls: str) -> None:
        super().__init__(
            f"column {table}.{column} is a key: it follows its table's class ({cls!r}), and is "
            "not moved by hand"
        )
        self.column = column
        self.table = table
        self.cls = cls


class TermIsClass(ValueError):
    """A column ref refused: the term is grounded in tables (it is a Class)."""

    def __init__(self, term: str, tables: list[str]) -> None:
        super().__init__(
            f"term {term!r} grounds tables ({', '.join(tables)}), so it cannot own a column ref: "
            + _ROLES_EXCLUSIVE
        )
        self.term = term
        self.tables = tables


_TABLE_ADDRESS = (
    registered_tables.c.source_id,
    registered_tables.c.schema_name,
    registered_tables.c.table_name,
    registered_tables.c.domain_id,
)


def _address(row: Any) -> str:
    return f"{row.source_id}.{row.schema_name}.{row.table_name}"


async def _term_name(conn: "Connection", term_id: int) -> str:
    row = (
        await conn.execute_core(select(glossary_terms.c.name).where(glossary_terms.c.id == term_id))
    ).fetchone()
    if row is None:
        raise TermNotFound(f"term {term_id} not found")
    return row.name


async def _term_table_ids(conn: "Connection", term_id: int) -> set[int]:
    rows = (
        await conn.execute_core(
            select(registered_tables.c.id).where(registered_tables.c.class_term_id == term_id)
        )
    ).fetchall()
    return {r.id for r in rows}


async def _non_key_refs(
    conn: "Connection", term_id: int, *, also_grounding: int | None = None
) -> list[str]:
    """The column refs ``term_id`` holds that a Class may not (REQ-2141): every ref but the key
    refs that target it -- its tables' primary keys (with ``also_grounding``, a table it is about
    to ground) and the foreign keys referring to them. As source.schema.table.column."""
    assume = {} if also_grounding is None else {also_grounding: term_id}
    rows = (
        await conn.execute_core(
            select(
                glossary_term_refs.c.table_id,
                glossary_term_refs.c.column_name,
                *_TABLE_ADDRESS[:3],
            )
            .join(registered_tables, glossary_term_refs.c.table_id == registered_tables.c.id)
            .where(glossary_term_refs.c.term_id == term_id)
            .order_by(*_TABLE_ADDRESS[:3], glossary_term_refs.c.column_name)
        )
    ).fetchall()
    targets: dict[int, dict[str, int]] = {}
    out = []
    for r in rows:
        if r.table_id not in targets:
            targets[r.table_id] = await _key_targets(conn, r.table_id, assume=assume)
        if targets[r.table_id].get(r.column_name) != term_id:
            out.append(f"{_address(r)}.{r.column_name}")
    return out


async def _term_table_addresses(conn: "Connection", term_id: int) -> list[str]:
    rows = (
        await conn.execute_core(
            select(*_TABLE_ADDRESS[:3])
            .where(registered_tables.c.class_term_id == term_id)
            .order_by(*_TABLE_ADDRESS[:3])
        )
    ).fetchall()
    return [_address(r) for r in rows]


async def refuse_class_on_property(conn: "Connection", term_id: int, table_id: int) -> None:
    """TermIsProperty when grounding ``table_id`` on ``term_id`` would leave it a Class holding
    non-key column refs (REQ-2141)."""
    columns = await _non_key_refs(conn, term_id, also_grounding=table_id)
    if columns:
        raise TermIsProperty(await _term_name(conn, term_id), columns)


async def refuse_property_on_class(conn: "Connection", term_id: int) -> None:
    """TermIsClass when ``term_id`` grounds tables: a non-key column ref is not a Class's
    (REQ-2141)."""
    tables = await _term_table_addresses(conn, term_id)
    if tables:
        raise TermIsClass(await _term_name(conn, term_id), tables)


async def role_conflicts(conn: "Connection") -> dict:
    """What breaks the roles' exclusivity (REQ-2141), reported and never changed:

    - ``terms``: each Class holding column refs a Class may not -- refs other than its tables'
      primary keys and the key columns landing on it by name -- with its tables and those columns;
    - ``tables``: each keyed table with no Class term because its concept is already a Property
      holding non-key refs, with that term and its columns."""
    classes = (
        await conn.execute_core(
            select(glossary_terms.c.id, glossary_terms.c.name)
            .where(glossary_terms.c.id.in_(_class_term_ids()))
            .order_by(glossary_terms.c.name)
        )
    ).fetchall()
    terms = []
    for r in classes:
        columns = await _non_key_refs(conn, r.id)
        if columns:
            terms.append(
                {
                    "term_id": r.id,
                    "term": r.name,
                    "tables": await _term_table_addresses(conn, r.id),
                    "columns": columns,
                }
            )
    keyed = select(table_columns.c.table_id).where(table_columns.c.is_primary_key)
    ungrounded = (
        await conn.execute_core(
            select(registered_tables.c.id, registered_tables.c.alias, *_TABLE_ADDRESS)
            .where(registered_tables.c.id.in_(keyed))
            .where(registered_tables.c.class_term_id.is_(None))
            .order_by(*_TABLE_ADDRESS[:3])
        )
    ).fetchall()
    by_name = {
        r.name: r.id
        for r in (
            await conn.execute_core(select(glossary_terms.c.id, glossary_terms.c.name))
        ).fetchall()
    }
    tables = []
    for t in ungrounded:
        concept = table_concept(t.alias or t.table_name)
        term_id = by_name.get(concept)
        if term_id is None:
            continue
        columns = await _non_key_refs(conn, term_id, also_grounding=t.id)
        if columns:
            tables.append(
                {
                    "table_id": t.id,
                    "table": _address(t),
                    "domain_id": t.domain_id,
                    "concept": concept,
                    "term_id": term_id,
                    "columns": columns,
                }
            )
    # REQ-2145: a tag stays on a term that has stopped being a Property, and is reported.
    tag_rows = (
        await conn.execute_core(
            select(tag_assignments.c.tag_id, glossary_terms.c.id, glossary_terms.c.name)
            .join(glossary_terms, glossary_terms.c.id == tag_assignments.c.term_id)
            .where(tag_assignments.c.object_type == "term")
            .order_by(glossary_terms.c.name, tag_assignments.c.tag_id)
        )
    ).fetchall()
    tagged = []
    for r in tag_rows:
        role = await term_role(conn, r.id)
        if role != "property":
            tagged.append({"term_id": r.id, "term": r.name, "tag_id": r.tag_id, "role": role})
    return {"terms": terms, "tables": tables, "tags": tagged}


async def term_role(conn: "Connection", term_id: int) -> str:
    """The term's role (REQ-2141): ``class`` when it grounds tables, ``property`` when it holds
    column refs, ``concept`` when it does neither."""
    if await _term_table_ids(conn, term_id):
        return "class"
    held = (
        await conn.execute_core(select(func.count()).where(glossary_term_refs.c.term_id == term_id))
    ).scalar_one()
    return "property" if held else "concept"


async def term_tags(conn: "Connection", term_id: int) -> list[dict]:
    """The tags on the term (REQ-2145), each with its reason."""
    rows = (
        await conn.execute_core(
            select(tag_assignments.c.tag_id, tag_assignments.c.reason)
            .where(tag_assignments.c.object_type == "term", tag_assignments.c.term_id == term_id)
            .order_by(tag_assignments.c.tag_id)
        )
    ).fetchall()
    return [dict(r._mapping) for r in rows]


async def term_column_count(conn: "Connection", term_id: int) -> int:
    """How many columns the term holds."""
    return (
        await conn.execute_core(select(func.count()).where(glossary_term_refs.c.term_id == term_id))
    ).scalar_one()


async def is_key_column(conn: "Connection", table_id: int, column_name: str) -> bool:
    """Whether the column is a key -- a primary key's, or a declared foreign key's -- whose ref is
    placed on a Class by derivation (REQ-2141)."""
    return column_name in await _primary_keys(conn, table_id) or column_name in (
        await _references(conn, table_id)
    )


async def term_exists(conn: "Connection", term_id: int) -> bool:
    return (
        await conn.execute_core(select(glossary_terms.c.id).where(glossary_terms.c.id == term_id))
    ).fetchone() is not None


async def term_tables(conn: "Connection", term_id: int) -> list[dict]:
    """The tables and views ``term_id`` grounds, by their address."""
    if not await term_exists(conn, term_id):
        raise TermNotFound(f"term {term_id} not found")
    rows = (
        await conn.execute_core(
            select(registered_tables.c.id.label("table_id"), *_TABLE_ADDRESS)
            .where(registered_tables.c.class_term_id == term_id)
            .order_by(*_TABLE_ADDRESS[:3])
        )
    ).fetchall()
    return [dict(r._mapping) for r in rows]


async def _keyed_table(conn: "Connection", table_id: int) -> str:
    """The address of ``table_id``, refused when it has no primary key -- its rows are then not
    identifiable instances of a class."""
    table = (
        await conn.execute_core(select(*_TABLE_ADDRESS).where(registered_tables.c.id == table_id))
    ).fetchone()
    if table is None:
        raise TableNotFound(f"table {table_id} not found")
    address = f"{table.source_id}.{table.schema_name}.{table.table_name}"
    keyed = (
        await conn.execute_core(
            select(table_columns.c.id).where(
                (table_columns.c.table_id == table_id) & table_columns.c.is_primary_key
            )
        )
    ).first()
    if keyed is None:
        raise TableHasNoPrimaryKey(address)
    return address


async def _home_key_refs(
    conn: "Connection", table_id: int, *, keep: "frozenset[int]" = frozenset()
) -> None:
    """A table's Class changed: its key refs, and the foreign keys of the tables referring to
    it, go where REQ-2141 puts them; the terms they leave are settled but for ``keep``."""
    await rehome_keys(conn, {table_id}, keep=keep)


async def ground_table(conn: "Connection", term_id: int, table_id: int) -> None:
    """Make ``term_id`` the class term of ``table_id``.

    Refused by name: a table without a primary key, whose rows are not identifiable instances,
    a term holding column refs a Class may not (a Property, REQ-2141), and a table a curated
    class (or this term) already grounds -- a table has one class term. A table's draft gives
    way. The table's key refs, and the foreign keys referring to it, move onto the term.
    """
    if not await term_exists(conn, term_id):
        raise TermNotFound(f"term {term_id} not found")
    address = await _keyed_table(conn, table_id)
    await refuse_class_on_property(conn, term_id, table_id)
    held = (
        await conn.execute_core(
            select(glossary_terms.c.id, glossary_terms.c.name)
            .join(registered_tables, registered_tables.c.class_term_id == glossary_terms.c.id)
            .where(registered_tables.c.id == table_id)
        )
    ).fetchone()
    if held is not None and (held.id == term_id or held.id not in await draft_class_terms(conn)):
        raise TableAlreadyGrounded(address, held.id, held.name)
    if held is not None:
        # The table's draft gives way to the curated class (REQ-2141).
        await identity.refuse_term_change(conn, table_id)
    await conn.execute_core(
        update(registered_tables)
        .where(registered_tables.c.id == table_id)
        .values(class_term_id=term_id, class_term_derived=False)
    )
    await _home_key_refs(conn, table_id)
    model_change.name("ground", "glossary term", f"{term_id} to table {address}")


async def declare_class_term(conn: "Connection", table_id: int, term_id: int) -> None:
    """A configuration's statement of ``table_id``'s class term (REQ-2141): the declarative model
    is the source of truth, so it replaces whatever term grounded the table before, and loading
    the same configuration again changes nothing. Another term in its place is refused while a
    same-entity relationship stands on the one it has (identity.TableInIdentity)."""
    address = await _keyed_table(conn, table_id)
    await refuse_class_on_property(conn, term_id, table_id)
    held = await _class_term_of(conn, table_id)
    if held is not None and held != term_id:
        await identity.refuse_term_change(conn, table_id)
    await conn.execute_core(
        update(registered_tables)
        .where(registered_tables.c.id == table_id)
        .values(class_term_id=term_id, class_term_derived=False)
    )
    await _home_key_refs(conn, table_id)
    if held is not None and held != term_id:
        # A draft the configuration replaced keeps what else it holds, or goes (REQ-2141).
        await _settle_terms(conn, {held}, domains_before=await term_domains(conn))
    model_change.name("ground", "glossary term", f"{term_id} to table {address}")


async def unground_table(conn: "Connection", term_id: int, table_id: int) -> bool:
    """Withdraw ``term_id`` as ``table_id``'s class term; False when it was not. Refused while a
    same-entity relationship stands on it (identity.TableInIdentity)."""
    await identity.refuse_term_change(conn, table_id)
    result = await conn.execute_core(
        update(registered_tables)
        .where(
            (registered_tables.c.class_term_id == term_id) & (registered_tables.c.id == table_id)
        )
        .values(class_term_id=None, class_term_derived=False)
    )
    ungrounded = (result.rowcount or 0) > 0
    if ungrounded:
        # REQ-2141: the table goes back to its draft class, as a sync would derive it.
        if await _primary_keys(conn, table_id):
            await _derive_class(conn, table_id, await _concept_of(conn, table_id))
        await _home_key_refs(conn, table_id, keep=frozenset({term_id}))
    return ungrounded


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


async def related_terms_for_tables(
    conn: "Connection", table_ids: "set[int]", *, domains: "frozenset[str] | None" = None
) -> list[dict]:
    """Terms tied to any column of ``table_ids``, plus abstract terms reachable from those via
    edges — the full transitive closure (REQ-1634 data product Related Terms panel).

    Traversal follows ``glossary_term_edges`` in both directions: a rooted term may name an
    abstract KIND_OF/PART_OF ancestor, or an abstract term may name a rooted one, and the
    relationship reads either way once curated. Only the directly-ref'd terms and the abstract
    terms found along the way are returned — a rooted term reached only transitively (e.g. a
    sibling under the same abstract parent) says nothing about this data product's own columns.

    ``domains`` applies the same REQ-1591 ``readable_term`` scoping ``list_terms`` uses, so a
    caller narrowed to a domain selection never sees a term outside it just by knowing a table id.
    """
    if not table_ids:
        return []
    direct_rows = (
        await conn.execute_core(
            select(glossary_term_refs.c.term_id)
            .where(glossary_term_refs.c.table_id.in_(table_ids))
            .distinct()
        )
    ).fetchall()
    direct_ids = {r.term_id for r in direct_rows}
    if not direct_ids:
        return []
    edge_rows = (
        await conn.execute_core(
            select(glossary_term_edges.c.from_term_id, glossary_term_edges.c.to_term_id)
        )
    ).fetchall()
    adjacency: dict[int, set[int]] = {}
    for r in edge_rows:
        adjacency.setdefault(r.from_term_id, set()).add(r.to_term_id)
        adjacency.setdefault(r.to_term_id, set()).add(r.from_term_id)
    visited = set(direct_ids)
    queue = list(direct_ids)
    while queue:
        current = queue.pop()
        for neighbor in adjacency.get(current, ()):
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)
    rows = (
        await conn.execute_core(select(glossary_terms).where(glossary_terms.c.id.in_(visited)))
    ).fetchall()
    live = await live_ids(conn)
    scope = await term_domains(conn)
    graph = await _load_graph(conn)
    return [
        dict(r._mapping)
        | {
            "live": r.id in live,
            "domains": sorted(scope.get(r.id, set())),
            "is_abstract": not graph.terms[r.id]["rooted"],  # REQ-2141: derived
        }
        for r in rows
        if (r.id in direct_ids or _is_concept(graph.terms[r.id]))
        and readable_term(domains, scope.get(r.id, set()))
    ]


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
    in service, defined, and grounded in a physical column: an undefined term is a draft,
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


async def vocabulary_refs(  # REQ-2135, REQ-2141
    conn: "Connection",
) -> tuple[dict[int, str], dict[tuple[int, str], list[str]]]:
    """What the live terms ground, for SPARQL's vocabulary: each grounded table's class term by
    table id, and the terms of each (table id, column) a term reference names, in name order. A
    term no consuming surface may offer (the shared admission rule) grounds nothing here."""
    live = await live_ids(conn)
    names = {
        r.id: r.name
        for r in (
            await conn.execute_core(select(glossary_terms.c.id, glossary_terms.c.name))
        ).fetchall()
        if r.id in live
    }
    table_terms = {
        r.id: names[r.class_term_id]
        for r in (
            await conn.execute_core(
                select(registered_tables.c.id, registered_tables.c.class_term_id)
            )
        ).fetchall()
        if r.class_term_id in names
    }
    column_terms: dict[tuple[int, str], list[str]] = {}
    for r in (
        await conn.execute_core(
            select(
                glossary_term_refs.c.table_id,
                glossary_term_refs.c.column_name,
                glossary_term_refs.c.term_id,
            )
        )
    ).fetchall():
        if r.term_id in names:
            column_terms.setdefault((r.table_id, r.column_name), []).append(names[r.term_id])
    return table_terms, {k: sorted(v) for k, v in column_terms.items()}
