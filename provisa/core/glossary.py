# Copyright (c) 2026 Kenneth Stott
# Canary: 3a7c1f52-9b0e-4c6d-8f21-5e4a9d0b7c33
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Deterministic physical-name → business-term normalization (REQ-1387).

The term unit is the phrase: the FULL normalized field name, never decomposed into
tokens. Normalization is rule-based over common enterprise DB naming conventions —
case folding, separator/camelCase tokenization, and a fixed abbreviation table — so
the same physical name always lands on the same term, which is what makes bottom-up
dedup (cust_id / customerId / CUSTOMER_KEY → one term) work.

A trailing proxy token (id, key, code, index, reference and their expansions) is
stripped: a column named for a key or code is referring to the underlying concept
through a proxy value, so cust_id and the customer concept land on the same term.
Only trailing tokens strip, and never the last one standing — a bare ``id`` column
still normalizes to "identifier".

Not a naming-convention converter: compiler/naming.py maps identifiers between SQL and
GraphQL conventions; this maps identifiers into human vocabulary (spaces, expansions),
so the boundary regexes here serve a different output alphabet.
"""

# Requirements: REQ-1387, REQ-1591

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Set as AbstractSet

TERM_EDGE_TYPES = (
    "KIND_OF",
    "RELATED_TO",
    "PART_OF",
    "SYNONYM_OF",
    "VALID_VALUE_OF",
    "DERIVED_FROM",
    "REPLACES",
    "PREFERRED_TERM_FOR",
    "TRANSLATION_OF",
    "ANTONYM_OF",
)

# Fixed, curated expansion table for pervasive enterprise abbreviations. Deliberately
# conservative: an ambiguous short form (st, min, no) stays as written rather than
# guessing. 'key' and 'id' both expand to 'identifier' so key/id spellings of the same
# concept converge on one term (the REQ-1387 scenario).
_ABBREVIATIONS: dict[str, str] = {
    "acct": "account",
    "addr": "address",
    "amt": "amount",
    "avg": "average",
    "bal": "balance",
    "cat": "category",
    "cd": "code",
    "cnt": "count",
    "cust": "customer",
    "curr": "currency",
    "desc": "description",
    "dept": "department",
    "dob": "date of birth",
    "dt": "date",
    "emp": "employee",
    "fname": "first name",
    "guid": "identifier",
    "id": "identifier",
    "ident": "identifier",
    "key": "identifier",
    "lname": "last name",
    "loc": "location",
    "mgr": "manager",
    "nbr": "number",
    "nm": "name",
    "num": "number",
    "org": "organization",
    "pct": "percent",
    "phn": "phone",
    "pk": "identifier",
    "prod": "product",
    "qty": "quantity",
    "ref": "reference",
    "seq": "sequence",
    "sk": "identifier",
    "ssn": "social security number",
    "tot": "total",
    "ts": "timestamp",
    "txn": "transaction",
    "uuid": "identifier",
    "yr": "year",
}

# Phrases too generic to be a concept on their own: a bare "name"/"date"/"identifier"
# column names an attribute of its TABLE's concept, and one shared term for every such
# column over-merges unrelated meanings (employees.name is not products.name). When the
# full normalized phrase lands here and a table context is supplied, the term becomes
# "<table concept> <phrase>" — connecting the column to its enclosing concept instead.
_GENERIC_TERMS = frozenset(
    {
        "amount",
        "available",
        "average",
        "category",
        "code",
        "complete",
        "contact",
        "count",
        "date",
        "day",
        "day of week",
        "description",
        "email",
        "first name",
        "identifier",
        "index",
        "last name",
        "location",
        "max",
        "maximum",
        "message",
        "min",
        "minimum",
        "name",
        "number",
        "phone",
        "price",
        "quantity",
        "reference",
        "role",
        "status",
        "sum",
        "tag",
        "text",
        "time",
        "timestamp",
        "title",
        "total",
        "type",
        "value",
        # Audit-trail phrases: every table carries them, so unqualified they over-merge
        # worse than any other family (created_by on orders is not created_by on invoices).
        *(
            f"{verb}{suffix}"
            for verb in ("created", "modified", "updated", "deleted", "submitted")
            for suffix in ("", " at", " date", " time", " timestamp")
        ),
    }
)

# Connective tokens carry no concept of their own — "pet by name" and "pet name" are the
# same term, so they drop anywhere in the phrase (unless nothing else remains). In a TABLE
# name the same token marks an access path rather than a compound noun: everything from
# "by" onward is the lookup key, so table_concept truncates there instead of dropping
# (REQ-1582).
_CONNECTIVE_TOKENS = frozenset({"by"})

# Proxy tokens (post-expansion): a trailing one means the column carries a stand-in value
# (key, code, index number) for the concept the preceding tokens name — drop it so the
# term IS the concept. Compared after abbreviation expansion, so id/key/pk/sk arrive
# here as 'identifier' and ref as 'reference'.
_PROXY_TOKENS = frozenset({"identifier", "code", "index", "reference"})

# camelCase word boundary: a lower/digit character followed by an Upper starts a new word.
_CAMEL_1 = re.compile(r"([a-z0-9])([A-Z])")
# Acronym boundary: a run of capitals followed by a capitalized word (XMLParser → XML Parser).
_CAMEL_2 = re.compile(r"([A-Z]+)([A-Z][a-z])")
_SEPARATORS = re.compile(r"[_\-.\s/]+")
_NON_ALNUM = re.compile(r"[^a-z0-9 ]+")


def _phrase(physical_name: str, *, cut_at_connective: bool = False) -> str:
    spaced = _CAMEL_2.sub(r"\1 \2", physical_name)
    spaced = _CAMEL_1.sub(r"\1 \2", spaced)
    spaced = _SEPARATORS.sub(" ", spaced).lower()
    spaced = _NON_ALNUM.sub("", spaced)
    tokens = [t for t in spaced.split(" ") if t]
    if not tokens:
        return physical_name.strip().lower() or physical_name
    expanded = [_ABBREVIATIONS.get(t, t) for t in tokens]
    if cut_at_connective:
        head = next((i for i, t in enumerate(expanded) if t in _CONNECTIVE_TOKENS), None)
        if head:  # a connective with nothing before it names no concept to cut back to
            expanded = expanded[:head]
    kept = [t for t in expanded if t not in _CONNECTIVE_TOKENS]
    if kept:
        expanded = kept
    while len(expanded) > 1 and expanded[-1] in _PROXY_TOKENS:
        expanded.pop()
    return " ".join(expanded)


_inflect_engine = None


def _singular(token: str) -> str:
    """Deterministic head-noun singularization (employees → employee), via inflect.

    The engine builds lazily and once — its construction cost dominates the inflect
    import (see compiler/naming.py, which defers it for the same reason). inflect
    returns false singulars for -ss nouns (address → addres); those keep the original
    token, matching the guard naming.py already uses.
    """
    from typing import Any, cast

    global _inflect_engine
    if _inflect_engine is None:
        import inflect

        _inflect_engine = inflect.engine()
    singular = _inflect_engine.singular_noun(cast(Any, token))
    if not singular or token.endswith("ss"):
        return token
    return str(singular)


def table_concept(table_name: str) -> str:
    """The table's concept phrase: normalized business name with a singular head noun.

    An access-path name is cut at its connective (REQ-1582): ``user_by_name`` and
    ``orders_by_customer`` are a user and an order reached through a lookup key, so their
    concepts are "user" and "order". Keeping the tail would make the surrogate key on
    ``user_by_name`` normalize to "user name" and collide with the genuine ``users.name``
    attribute -- one term holding both a thing and one of its fields.
    """
    tokens = _phrase(table_name, cut_at_connective=True).split(" ")
    tokens[-1] = _singular(tokens[-1])
    return " ".join(tokens)


def normalize_term(physical_name: str, *, table_context: str | None = None) -> str:
    """The deterministic term phrase for one physical field or table name.

    Tokenize on separators and camelCase boundaries, case-fold, expand the fixed
    abbreviation table, rejoin with single spaces. A name that tokenizes to nothing
    (all separators/punctuation) normalizes to its case-folded raw form — part of the
    normalization rule, so every registered name yields a non-empty term.

    A phrase in the TOO-GENERIC set names an attribute of its table's concept, not a
    concept of its own; with ``table_context`` supplied it qualifies to
    "<table concept> <phrase>" (employees.first_name → "employee first name") so
    unrelated tables' name/date/id columns never over-merge into one meaningless term.
    A qualified phrase then sheds trailing proxy tokens like any other: orders.id
    → "order", landing the PK column on the same term as every FK that references it
    (other tables' order_id → "order").
    """
    phrase = _phrase(physical_name)
    if table_context is not None and phrase in _GENERIC_TERMS:
        tokens = f"{table_concept(table_context)} {phrase}".split(" ")
        while len(tokens) > 1 and tokens[-1] in _PROXY_TOKENS:
            tokens.pop()
        return " ".join(tokens)
    return phrase


def within_domains(allowed: "AbstractSet[str] | None", term_domains: "AbstractSet[str]") -> bool:
    """May a caller whose domains are ``allowed`` reach a term scoped to ``term_domains``? (REQ-1591)

    ANY, not all. A term spanning sales and pet-store is reachable by either domain's people,
    which deliberately breaks the symmetry with REQ-1531's ``require_domains``: that rule guards
    acts reaching DATA in two domains, whereas a term is prose about a concept both domains
    already reference, and requiring all would leave a shared term curatable only by someone
    holding every domain it touches — a deadlock manufactured by a name collision.

    The same answer serves reading and curating; there is no second, stricter predicate. Where two
    domains genuinely mean different things by one phrase the remedy is a SPLIT — a new term with
    the refs moved onto it — not a narrower gate on the shared one.

    ``allowed`` of ``None`` is an unlimited role (or a deployment where domains gate nothing) and
    admits everything; that is an answer, not a missing value. An empty ``term_domains`` is an
    UNSCOPED term, which nothing scopes and everyone holding the glossary rights may reach.
    """
    return allowed is None or not term_domains or bool(term_domains & allowed)


# REQ-1593: the ENTERPRISE scope — a term that belongs to the whole org rather than to any domain.
# The same character means the OPPOSITE THING on a role's ``domain_access`` (``provisa/security/
# rights.py``, read by ``env_authority.domains_within``): there it is unlimited AUTHORITY, "this
# role reaches every domain". Here it is unlimited MEMBERSHIP, "this term is in every domain".
# Different table, different direction, and they must never be read across: a term's ``*`` grants
# its curators nothing, and the glossary router is the only place that maps one to the other —
# by REFUSING, so that only ``org_glossary_rw`` may declare or unset it.
ENTERPRISE_DOMAIN = "*"


def readable_term(allowed: "AbstractSet[str] | None", term_domains: "AbstractSet[str]") -> bool:
    """May a caller whose domains are ``allowed`` READ a term scoped to ``term_domains``? (REQ-1593)

    Reading and curating stopped asking the same question once ``*`` existed. An enterprise-wide
    term is the org's shared vocabulary — the phrase every domain uses and none owns — so it is
    visible to every holder of ``glossary_read``, while curating it takes ``org_glossary_rw``.
    Every other term reads by the ordinary ANY rule, which :func:`within_domains` still decides.
    """
    return ENTERPRISE_DOMAIN in term_domains or within_domains(allowed, term_domains)


# ---------------------------------------------------------------------------
# Status and grounding (REQ-1387, STATUS IS HUMAN, GROUNDING IS COMPUTED)
# ---------------------------------------------------------------------------

#: A term's status, set by people: draft (the default), approved (a steward's choice, which needs
#: a definition), deprecated or retired.
TERM_STATUSES = ("draft", "approved", "deprecated", "retired")

#: A term's grounding, computed and never set.
GROUNDED = "grounded"
GROUNDED_PENDING = "grounded_pending"
NOT_GROUNDED = "not_grounded"
GROUNDINGS = (GROUNDED, GROUNDED_PENDING, NOT_GROUNDED)


def term_status(term: "Mapping") -> str:
    """A term's status from its stored flags: retired, deprecated (the derivation lost its last
    column, or a curator deprecated it), approved, else draft."""
    if term.get("retired"):
        return "retired"
    if term.get("deprecated") or term.get("deprecation_asserted"):
        return "deprecated"
    return "approved" if term.get("approved") else "draft"


def _conducting(terms: "Iterable[Mapping]") -> dict[int, "Mapping"]:
    """The terms a chain passes through: every term in service. A deprecated or retired term
    breaks the chain."""
    return {t["id"]: t for t in terms if term_status(t) in ("draft", "approved")}


#: Edge types read from the broader ``b`` end down to the narrower ``a`` end: ``a`` is a kind of,
#: a part of, or a valid value of ``b`` (REQ-1387, DOWN IS FROM BROADER TO NARROWER).
NARROWING_EDGE_TYPES = ("KIND_OF", "PART_OF", "VALID_VALUE_OF")
#: Edge types grounding crosses both ways: the two words mean the same thing. Every other edge
#: type (related-to, derived-from, replaces, translation-of, antonym-of, preferred-term-for)
#: conducts no grounding.
EQUIVALENCE_EDGE_TYPES = ("SYNONYM_OF",)


def _down(
    conducting: "AbstractSet[int]", edges: "Iterable[tuple[int, int, str]]"
) -> dict[int, set[int]]:
    """Each in-service term's next terms down toward data: the terms narrower than it, and its
    synonyms. A term is never reached by going up to a shared parent and back down."""
    down: dict[int, set[int]] = {}
    for a, b, rel_type in edges:
        if a not in conducting or b not in conducting or a == b:
            continue
        if rel_type in NARROWING_EDGE_TYPES:
            down.setdefault(b, set()).add(a)
        elif rel_type in EQUIVALENCE_EDGE_TYPES:
            down.setdefault(a, set()).add(b)
            down.setdefault(b, set()).add(a)
    return down


def _up(down: "Mapping[int, set[int]]") -> dict[int, set[int]]:
    """``down`` reversed: each term's next terms up, the terms whose paths down run through it."""
    up: dict[int, set[int]] = {}
    for upper, lowers in down.items():
        for lower in lowers:
            up.setdefault(lower, set()).add(upper)
    return up


def grounded_term_ids(
    terms: "Iterable[Mapping]", edges: "Iterable[tuple[int, int, str]]", rooted: "AbstractSet[int]"
) -> set[int]:
    """Term ids with a path down, over in-service terms, to a term holding a physical ref: every
    term whose grounding is not ``not_grounded``. Down follows each edge type's direction
    (:data:`NARROWING_EDGE_TYPES`, :data:`EQUIVALENCE_EDGE_TYPES`).

    Structural only: status and definition do not matter, and a deprecated or retired term
    conducts nothing. ``rooted`` is the set of term ids holding at least one physical ref, to a
    column or to a whole table (REQ-1387, GROUNDED TO A TABLE). Callers supply it because they
    count refs differently: the repository counts every ref, the exporter counts only refs whose
    column actually publishes.
    """
    conducting = _conducting(terms)
    up = _up(_down(set(conducting), edges))
    frontier = [tid for tid in rooted if tid in conducting]
    grounded = set(frontier)
    while frontier:
        for neighbor in up.get(frontier.pop(), ()):
            if neighbor not in grounded:
                grounded.add(neighbor)
                frontier.append(neighbor)
    return grounded


def _clean_ids(
    conducting: "Mapping[int, Mapping]",
    up: "Mapping[int, set[int]]",
    rooted: "AbstractSet[int]",
    approved: "AbstractSet[int]",
) -> set[int]:
    """The terms with a path down to data that has no draft term strictly between the term and
    the term at the data end. A term holding a binding is its own data end; a term directly above
    one, or directly above an approved term that has such a path, has one too."""
    frontier = [tid for tid in rooted if tid in conducting]
    clean = set(frontier)
    while frontier:
        current = frontier.pop()
        # Only a binding (the data end) or an approved term may stand between a term and data.
        if current not in rooted and current not in approved:
            continue
        for neighbor in up.get(current, ()):
            if neighbor not in clean:
                clean.add(neighbor)
                frontier.append(neighbor)
    return clean


def term_grounding(
    terms: "Iterable[Mapping]", edges: "Iterable[tuple[int, int, str]]", rooted: "AbstractSet[int]"
) -> dict[int, str]:
    """Every term's grounding (REQ-1387, GROUNDING LOOKS DOWN, STRICTLY BETWEEN).

    A term's grounding looks only down its own paths to data, never at the term itself or at
    anything above it:

    * ``grounded`` -- some path down has no draft term strictly between the term and the term at
      the data end that holds the binding;
    * ``grounded_pending`` -- every path down has a draft term in between;
    * ``not_grounded`` -- no path reaches data, or the term is deprecated or retired.

    Down follows each edge type's direction (REQ-1387, DOWN IS FROM BROADER TO NARROWER): from a
    broader term to the terms that are a kind of it, a part of it or a valid value of it, and
    across a synonym either way; other edge types conduct nothing. Paths run over in-service
    terms; a deprecated or retired term breaks the chain. A draft term
    at either end of a path counts against nothing: a draft abstract term drafted above an
    approved chain is grounded, and an auto-derived draft term at the data end leaves the chain
    above it unchanged.
    """
    terms = list(terms)
    conducting = _conducting(terms)
    up = _up(_down(set(conducting), edges))
    approved = {tid for tid, t in conducting.items() if t.get("approved")}
    reached = grounded_term_ids(terms, edges, rooted)
    clean = _clean_ids(conducting, up, rooted, approved)
    return {
        t["id"]: GROUNDED
        if t["id"] in clean
        else GROUNDED_PENDING
        if t["id"] in reached
        else NOT_GROUNDED
        for t in terms
    }


def approvals_that_ground(
    terms: "Iterable[Mapping]", edges: "Iterable[tuple[int, int, str]]", rooted: "AbstractSet[int]"
) -> dict[int, list[int]]:
    """For each ``grounded_pending`` term, the draft terms whose approval alone makes it
    grounded (REQ-1387 findings: the term whose approval makes it grounded). A pending term no
    single approval grounds is absent."""
    terms = list(terms)
    edges = list(edges)
    conducting = _conducting(terms)
    up = _up(_down(set(conducting), edges))
    approved = {tid for tid, t in conducting.items() if t.get("approved")}
    grounding = term_grounding(terms, edges, rooted)
    pending = {tid for tid, g in grounding.items() if g == GROUNDED_PENDING}
    if not pending:
        return {}
    out: dict[int, list[int]] = {}
    # A draft that could stand between a pending term and data: in service, reaching data, and
    # not itself a data end (a data end never stands between).
    candidates = sorted(
        tid
        for tid in conducting
        if tid not in approved and tid not in rooted and grounding[tid] != NOT_GROUNDED
    )
    for candidate in candidates:
        clean = _clean_ids(conducting, up, rooted, approved | {candidate})
        for tid in sorted(pending & clean):
            out.setdefault(tid, []).append(candidate)
    return out


def _defined(term: "Mapping") -> bool:
    return bool((term.get("definition") or "").strip())


@dataclass(frozen=True)
class AgentView:
    """What an agent sees of the term graph (REQ-1387, STATUS IS HUMAN, GROUNDING IS COMPUTED).

    ``named`` maps each term an agent may see by name to its status (approved, or draft for a
    defined draft term). ``hops`` are the undefined terms an agent traverses with no name or
    definition, each kept distinct. ``edges`` are the edges between seen terms, named or hop,
    each as ``(a, b, rel_type)``, so the shape of the lineage is unchanged.
    """

    named: "dict[int, str]"
    hops: "frozenset[int]"
    edges: "tuple[tuple[int, int, str], ...]"

    def seen(self) -> set[int]:
        return set(self.named) | set(self.hops)


def agent_view(
    terms: "Iterable[Mapping]", edges: "Iterable[tuple[int, int, str]]", rooted: "AbstractSet[int]"
) -> AgentView:
    """The terms an agent may see, and how (REQ-1387): an approved term on a path to data; a
    defined draft term on a path to data that holds at least one approved term, marked draft; an
    undefined term on such a path only as an anonymous hop; nothing else -- nothing on a path with
    no approved term, nothing not grounded, nothing deprecated or retired.

    A path to data runs down over in-service terms, in each edge type's direction, and ends at the
    first term holding a binding: a term holding one is its own path. Every simple path is walked;
    the terms between a start and its binding are Concepts (terms holding no binding), which a
    curator creates by hand, so the walk is over that small part of the graph.
    """
    terms = list(terms)
    edges = list(edges)
    conducting = _conducting(terms)
    down = _down(set(conducting), edges)
    approved = {tid for tid, t in conducting.items() if t.get("approved")}
    on_approved_path: set[int] = set()

    def record(path: list[int]) -> None:
        if any(tid in approved for tid in path):
            on_approved_path.update(path)

    def walk(path: list[int], members: set[int]) -> None:
        for neighbor in sorted(down.get(path[-1], ())):
            if neighbor in members:
                continue
            if neighbor in rooted:
                record([*path, neighbor])
                continue
            path.append(neighbor)
            members.add(neighbor)
            walk(path, members)
            members.discard(neighbor)
            path.pop()

    for start in sorted(conducting):
        if start in rooted:
            record([start])
        else:
            walk([start], {start})
    named = {
        tid: "approved" if tid in approved else "draft"
        for tid in on_approved_path
        if tid in approved or _defined(conducting[tid])
    }
    hops = frozenset(on_approved_path - set(named))
    seen = set(named) | hops
    seen_edges = tuple(
        sorted({(a, b, r) for a, b, r in edges if a in seen and b in seen and a != b})
    )
    return AgentView(named=named, hops=hops, edges=seen_edges)


def agent_paths(view: AgentView, rooted: "AbstractSet[int]", start: int) -> list[list[int]]:
    """``start``'s paths down to data over the terms ``view`` lets an agent see, each ending at the
    first term holding a binding (REQ-1387). A term holding one is its own path."""
    seen = view.seen()
    if start not in seen:
        return []
    if start in rooted:
        return [[start]]
    down = _down(seen, view.edges)
    paths: list[list[int]] = []

    def walk(path: list[int]) -> None:
        for neighbor in sorted(down.get(path[-1], ())):
            if neighbor in path:
                continue
            if neighbor in rooted:
                paths.append([*path, neighbor])
            else:
                walk([*path, neighbor])

    walk([start])
    return paths
