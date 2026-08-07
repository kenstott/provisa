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

# Requirements: REQ-1387

from __future__ import annotations

import re

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
# same term, so they drop anywhere in the phrase (unless nothing else remains).
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


def _phrase(physical_name: str) -> str:
    spaced = _CAMEL_2.sub(r"\1 \2", physical_name)
    spaced = _CAMEL_1.sub(r"\1 \2", spaced)
    spaced = _SEPARATORS.sub(" ", spaced).lower()
    spaced = _NON_ALNUM.sub("", spaced)
    tokens = [t for t in spaced.split(" ") if t]
    if not tokens:
        return physical_name.strip().lower() or physical_name
    expanded = [_ABBREVIATIONS.get(t, t) for t in tokens]
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
    """The table's concept phrase: normalized business name with a singular head noun."""
    tokens = _phrase(table_name).split(" ")
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
