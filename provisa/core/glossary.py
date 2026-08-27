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


# REQ-1592: the ENTERPRISE scope — a term that belongs to the whole org rather than to any domain.
# The same character means the OPPOSITE THING on a role's ``domain_access`` (``provisa/security/
# rights.py``, read by ``env_authority.domains_within``): there it is unlimited AUTHORITY, "this
# role reaches every domain". Here it is unlimited MEMBERSHIP, "this term is in every domain".
# Different table, different direction, and they must never be read across: a term's ``*`` grants
# its curators nothing, and the glossary router is the only place that maps one to the other —
# by REFUSING, so that only ``org_glossary_rw`` may declare or unset it.
ENTERPRISE_DOMAIN = "*"


def readable_term(allowed: "AbstractSet[str] | None", term_domains: "AbstractSet[str]") -> bool:
    """May a caller whose domains are ``allowed`` READ a term scoped to ``term_domains``? (REQ-1592)

    Reading and curating stopped asking the same question once ``*`` existed. An enterprise-wide
    term is the org's shared vocabulary — the phrase every domain uses and none owns — so it is
    visible to every holder of ``glossary_read``, while curating it takes ``org_glossary_rw``.
    Every other term reads by the ordinary ANY rule, which :func:`within_domains` still decides.
    """
    return ENTERPRISE_DOMAIN in term_domains or within_domains(allowed, term_domains)


def live_term_ids(
    terms: "Iterable[Mapping]", edges: "Iterable[tuple[int, int]]", rooted: "AbstractSet[int]"
) -> set[int]:
    """The terms a consuming surface may offer, by the one admission rule (REQ-1387).

    A term is live when it is all three of:

    * **in service** — neither ``retired`` (a curator took it out) nor ``deprecated``
      (the derivation lost its last column but the row was kept as an anchor);
    * **defined** — it carries a definition. A term derived from a column name is a
      token, not a meaning: "customer" off ``cust_id`` tells an agent nothing the schema
      does not already say, so an undefined term is a proposal awaiting a curator, never
      vocabulary to ground a question on;
    * **grounded** — connected, over in-service terms, to a term that holds a physical
      ref. The glossary is an entry point into the data, so every chain must terminate
      at a column; an abstract term wired to nothing physical names no data and cannot
      answer anything.

    Connectivity is structural and ignores definitions: an abstract term reaches data
    through an undefined rooted term just as well as a defined one. Out-of-service terms
    do not conduct — a retired term must not keep its dependents alive.

    ``rooted`` is the set of term ids holding at least one physical ref. Callers supply
    it because the two callers count refs differently: the repository counts every ref,
    the exporter counts only refs whose column actually publishes.
    """
    by_id = {t["id"]: t for t in terms}
    conducting = {
        tid for tid, t in by_id.items() if not t.get("retired") and not t.get("deprecated")
    }
    adjacency: dict[int, set[int]] = {}
    for a, b in edges:
        if a not in conducting or b not in conducting:
            continue
        adjacency.setdefault(a, set()).add(b)
        adjacency.setdefault(b, set()).add(a)
    frontier = [tid for tid in rooted if tid in conducting]
    grounded = set(frontier)
    while frontier:
        for neighbor in adjacency.get(frontier.pop(), ()):
            if neighbor not in grounded:
                grounded.add(neighbor)
                frontier.append(neighbor)
    return {tid for tid in grounded if (by_id[tid].get("definition") or "").strip()}
