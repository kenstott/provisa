# Copyright (c) 2026 Kenneth Stott
# Canary: 2c6e9a17-83f4-4d0b-b5e2-7a91d4c03f68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The alignments an import proposes (REQ-2237), for a steward to approve or reject.

An imported item is compared with the glossary's terms in service by name, as the glossary
compares names (provisa.term_finding.scan.normal: lowercase, leading article and plural gone), and
by the rule that derives a term from a column name (provisa.core.glossary.normalize_term:
abbreviations expanded, a trailing identifier or code dropped). Each item proposes, strongest
first, and at most one alignment with any one term:

1. SAME_AS the term that already carries its IRI (``iri``). Nothing else is proposed for it.
2. SAME_AS the term of its name (``name``), and a property SAME_AS the term its name qualified by
   the class it describes would derive (``qualified name``: ``Person`` ``hasName`` and the term
   ``person name`` the glossary derives from ``persons.name``).
3. SYNONYM_OF the term its name normalizes to (``normalized name``: ``CustomerIdentifier`` and
   ``customer``), and the term any of its alternative labels names (``synonym``).
4. A class, value type or concept: the term whose head it names is a KIND_OF it, and it is a
   KIND_OF the term its own head names (``head noun``: ``enterprise customer`` and ``customer``).
5. A class or concept: a Property term its name qualifies is PART_OF it (``qualifier``:
   ``customer email`` and ``customer``).

The file's own structure between its items is proposed as it states it (``ontology``). Every
proposal waits on a steward: none is approved here, and an item no proposal is approved for comes
in as a Concept that reaches no data until it aligns with a term that does.
"""

# Requirements: REQ-2237

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from provisa.core.glossary import normalize_term
from provisa.ontology_import.parse import CLASS, CONCEPT, PART_OF, PROPERTY, Edge, Item
from provisa.term_finding.scan import normal

SAME_AS = "SAME_AS"
#: The types an alignment takes: SAME_AS, or an edge the glossary holds.
ALIGNMENT_TYPES = (SAME_AS, "SYNONYM_OF", "KIND_OF", "PART_OF", "VALID_VALUE_OF", "RELATED_TO")

IRI = "iri"
NAME = "name"
QUALIFIED_NAME = "qualified name"
NORMALIZED_NAME = "normalized name"
SYNONYM = "synonym"
HEAD_NOUN = "head noun"
QUALIFIER = "qualifier"
ONTOLOGY = "ontology"
#: An alignment a steward added, rather than one proposed.
STEWARD = "steward"

#: Leading words a property's label carries that its value does not (``has name``).
_VERB_WORDS = frozenset({"has", "is"})


@dataclass(frozen=True)
class Term:
    """A glossary term in service: ``role`` is class, property or concept."""

    id: int
    name: str
    role: str
    iri: str | None


@dataclass(frozen=True)
class Proposal:
    """Item ``item`` (an ordinal) aligned with ``term_id`` or with the item ``other_item``:
    ``rel_type`` read from the item when ``item_is_source``, else toward it."""

    item: int
    rel_type: str
    item_is_source: bool
    basis: str
    term_id: int | None = None
    other_item: int | None = None


def _bare(name: str) -> str:
    words = name.split(" ")
    return " ".join(words[1:]) if len(words) > 1 and words[0] in _VERB_WORDS else name


def _index(terms: Sequence[Term]) -> tuple[dict, dict, dict]:
    """Terms by compared name, by each proper tail of their words, by each proper head."""
    by_name: dict[str, list[Term]] = {}
    tails: dict[str, list[Term]] = {}
    heads: dict[str, list[Term]] = {}
    for term in terms:
        by_name.setdefault(normal(term.name), []).append(term)
        words = term.name.split(" ")
        for i in range(1, len(words)):
            tails.setdefault(normal(" ".join(words[i:])), []).append(term)
            heads.setdefault(normal(" ".join(words[:i])), []).append(term)
    return by_name, tails, heads


def propose(items: Sequence[Item], edges: Sequence[Edge], terms: Sequence[Term]) -> list[Proposal]:
    """Every alignment the import proposes, in item order, each item's strongest first."""
    by_iri = {t.iri: t for t in terms if t.iri}
    by_name, tails, heads = _index(terms)
    described: dict[int, list[int]] = {}
    for edge in edges:
        if edge.rel_type == PART_OF and items[edge.source].kind == PROPERTY:
            described.setdefault(edge.source, []).append(edge.target)
    out: list[Proposal] = []
    for ordinal, item in enumerate(items):
        paired: set[int] = set()

        def put(term: Term, rel_type: str, item_is_source: bool, basis: str) -> None:
            if term.id not in paired:
                paired.add(term.id)
                out.append(Proposal(ordinal, rel_type, item_is_source, basis, term_id=term.id))

        held = by_iri.get(item.iri) if item.iri else None
        if held is not None:
            put(held, SAME_AS, True, IRI)
            continue
        own = normal(item.name)
        for term in by_name.get(own, []):
            put(term, SAME_AS, True, NAME)
        for target in described.get(ordinal, []):
            for term in by_name.get(normal(f"{items[target].name} {_bare(item.name)}"), []):
                put(term, SAME_AS, True, QUALIFIED_NAME)
        for term in by_name.get(normal(normalize_term(item.name)), []):
            put(term, "SYNONYM_OF", True, NORMALIZED_NAME)
        for label in item.labels:
            for variant in (label, normalize_term(label)):
                for term in by_name.get(normal(variant), []):
                    put(term, "SYNONYM_OF", True, SYNONYM)
        if item.kind != PROPERTY:
            for term in tails.get(own, []):
                put(term, "KIND_OF", False, HEAD_NOUN)
            words = item.name.split(" ")
            for i in range(1, len(words)):
                for term in by_name.get(normal(" ".join(words[i:])), []):
                    put(term, "KIND_OF", True, HEAD_NOUN)
        if item.kind in (CLASS, CONCEPT):
            for term in heads.get(own, []):
                if term.role == PROPERTY:
                    put(term, PART_OF, False, QUALIFIER)
    out.extend(Proposal(e.source, e.rel_type, True, ONTOLOGY, other_item=e.target) for e in edges)
    return out
