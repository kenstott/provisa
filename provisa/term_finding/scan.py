# Copyright (c) 2026 Kenneth Stott
# Canary: 9b2e47c1-0d6a-4f38-a5e1-c84f2b7d6e03
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A document's passages read into candidate glossary terms (REQ-2179).

Two steps, each a model call:

1. **Mentions.** The passages are read in batches (:func:`batches`); for each the model names the
   concepts its passages define or use (:func:`extraction_prompt`) -- business concepts, roles,
   organization types, policies, processes and regulations, never a named individual -- each with
   the passage it is in, the passage's own words for it (``quote``), whether those words define
   it, the sense the passage uses it in and the variants it is called by. A mention is kept only
   when its category is one of :data:`CATEGORIES` and its quote is the passage's own text
   (:func:`read_mentions`); what is left out is counted, never silently dropped.
2. **Candidates.** The mentions are resolved into concepts in one call (:func:`resolution_prompt`):
   the mentions of one concept grouped, however worded; one word used with two meanings split
   into two groups, each named apart and scoped by its passages; explanations that cannot both be
   true named in pairs; and, for a group with no explicit definition, one synthesized from its
   mentions' quotes, naming the mentions it drew from (:func:`read_groups`).

:func:`candidates` turns the groups into proposals: an explicit definition first (the earliest in
the document), every other explanation kept as a supporting passage; a synthesized definition
marked so, listing every passage it used; conflicting explanations side by side. :func:`match`
then finds the glossary terms and synonyms each candidate duplicates or names again.

Nothing here touches a database or a model: the caller passes the passages and a ``call`` that
answers a prompt (:mod:`.model`).
"""

# Requirements: REQ-2179

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

#: The kinds of concept a scan proposes (REQ-2179). A named individual -- a particular person,
#: company or policy edition -- is none of these.
CATEGORIES = (
    "business concept",
    "role",
    "organization type",
    "policy",
    "process",
    "regulation",
)
#: The kinds of variant a concept is called by. A ``form`` (case, plural, article) or a
#: ``reference`` ("as defined in Section 2") is resolved into the candidate but not offered as a
#: synonym by default; an ``abbreviation`` or an ``alternative name`` is.
VARIANT_KINDS = ("abbreviation", "alternative name", "form", "reference")
SYNONYM_KINDS = frozenset({"abbreviation", "alternative name"})
#: About how much passage text one mention-finding call reads, in characters.
BATCH_CHARS = 6000
#: The most output one mention-finding call and the resolution call may use, in tokens.
EXTRACT_MAX_TOKENS = 4096
RESOLVE_MAX_TOKENS = 8192

EXPLICIT = "explicit"
SYNTHESIZED = "synthesized"
#: A candidate whose group neither holds an explicit definition nor came back with a synthesized
#: one: the steward writes it.
MISSING = "missing"

DUPLICATE = "duplicate"
SYNONYM = "synonym"


class ScanFailed(RuntimeError):
    """The scan cannot go on, said naming why (an answer that is not JSON, a refused budget)."""


@dataclass(frozen=True)
class Passage:
    ordinal: int
    section: str | None
    page: int | None
    text: str


@dataclass(frozen=True)
class Variant:
    text: str
    kind: str


@dataclass(frozen=True)
class Mention:
    id: int
    passage: int
    name: str
    category: str
    sense: str
    quote: str
    explicit: bool
    variants: tuple[Variant, ...]


@dataclass
class LeftOut:
    """What a scan read but does not propose, counted by why."""

    not_a_concept: int = 0
    quote_not_in_passage: int = 0
    unknown_passage: int = 0
    malformed: int = 0
    unresolved: int = 0

    def as_dict(self) -> dict[str, int]:
        return dict(self.__dict__)


@dataclass(frozen=True)
class Group:
    name: str
    sense: str
    mentions: tuple[int, ...]
    variants: tuple[Variant, ...]
    conflicts: tuple[tuple[int, int], ...]
    definition: str | None
    uses: tuple[int, ...]


@dataclass
class Candidate:
    """A proposed term with everything the steward needs to decide on it."""

    name: str
    category: str
    sense: str
    definition: str | None
    definition_kind: str
    #: Each passage the candidate is found in: ``ordinal``, ``section``, ``page``, ``quote``,
    #: ``explicit``, and its ``role`` -- ``definition`` (the explicit definition), ``synthesis``
    #: (a passage the synthesized definition drew on) or ``supporting``.
    passages: list[dict[str, Any]]
    #: Each variant: ``text``, ``kind``, and ``keep`` -- proposed as a synonym.
    variants: list[dict[str, Any]]
    #: Explanations that cannot both be true, each pair side by side.
    conflicts: list[list[dict[str, Any]]]
    #: The other candidates of this scan named by the same word with another meaning.
    homonyms: list[str] = field(default_factory=list)
    #: The glossary term this candidate duplicates or names again (:func:`match`).
    match_term_id: int | None = None
    match_kind: str | None = None
    #: Every glossary term matched, by name: ``term_id``, ``name``, ``via`` (the candidate's name
    #: or variant that matched).
    matches: list[dict[str, Any]] = field(default_factory=list)


# -- reading the model's answers ----------------------------------------------------------------


def _json(raw: str, what: str) -> Any:
    text = raw.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else ""
        text = text.rsplit("```", 1)[0]
    try:
        return json.loads(text)
    except ValueError as exc:
        raise ScanFailed(f"the model's answer to {what} was not JSON: {exc}") from None


_QUOTES = str.maketrans({"‘": "'", "’": "'", "“": '"', "”": '"'})
_SPACE = re.compile(r"\s+")


def _plain(text: str) -> str:
    return _SPACE.sub(" ", text.translate(_QUOTES)).strip().lower()


def _name(raw: Any) -> str | None:
    if not isinstance(raw, str):
        return None
    name = _SPACE.sub(" ", raw).strip().lower()
    return name or None


_ARTICLE = re.compile(r"^(the|a|an)\s+")


def _singular(word: str) -> str:
    if word.endswith("ies") and len(word) > 4:
        return word[:-3] + "y"
    if word.endswith("sses"):
        return word[:-2]
    if word.endswith("s") and not word.endswith(("ss", "us", "is")) and len(word) > 3:
        return word[:-1]
    return word


def normal(name: str) -> str:
    """``name`` as compared with other names: lowercase, its leading article and plural gone."""
    plain = _ARTICLE.sub("", _plain(name))
    words = plain.split(" ")
    return " ".join([*words[:-1], _singular(words[-1])]) if plain else ""


def _variants(raw: Any) -> tuple[list[Variant], int]:
    """The well-formed variants of ``raw`` and how many were not."""
    if raw is None:
        return [], 0
    if not isinstance(raw, list):
        return [], 1
    found: list[Variant] = []
    bad = 0
    for v in raw:
        text = v.get("text") if isinstance(v, dict) else None
        kind = v.get("kind") if isinstance(v, dict) else None
        if not isinstance(text, str) or not text.strip() or kind not in VARIANT_KINDS:
            bad += 1
            continue
        found.append(Variant(_SPACE.sub(" ", text).strip(), kind))
    return found, bad


# -- step 1: mentions ---------------------------------------------------------------------------


def batches(passages: Sequence[Passage], limit: int = BATCH_CHARS) -> list[list[Passage]]:
    """The passages in document order, gathered into batches of about ``limit`` characters."""
    out: list[list[Passage]] = []
    current: list[Passage] = []
    size = 0
    for p in passages:
        if current and size + len(p.text) > limit:
            out.append(current)
            current, size = [], 0
        current.append(p)
        size += len(p.text)
    if current:
        out.append(current)
    return out


def _where(p: Passage) -> str:
    bits = []
    if p.section:
        bits.append(f"section {p.section!r}")
    if p.page is not None:
        bits.append(f"page {p.page}")
    return f" ({', '.join(bits)})" if bits else ""


def extraction_prompt(title: str, batch: Sequence[Passage]) -> str:
    """The prompt naming the concepts ``batch``'s passages define or use."""
    numbered = "\n\n".join(f"[P{p.ordinal}]{_where(p)}\n{p.text}" for p in batch)
    return (
        "You are helping a data steward build a business glossary from a document. Below are "
        f"passages of the document {title!r}, each numbered [P<n>].\n\n"
        "Find the concepts these passages define or use that belong in a business glossary: "
        "business concepts, roles, organization types, policies, processes and regulations. "
        "Never a named individual: not a particular person, company, product, system, place, "
        "or a particular edition of a policy or regulation (not 'Jane Smith', 'Acme Ltd' or "
        "'Privacy Policy v2.1'). The general kind an individual belongs to may be a concept "
        "('employee', 'supplier', 'privacy policy'). Leave out everyday words that are not the "
        "document's vocabulary.\n\n"
        "Report each concept once for every passage it appears in, as an object with:\n"
        '- "passage": the passage number n of [P<n>].\n'
        '- "name": the concept as the passage names it, singular, lowercase.\n'
        f'- "category": one of {", ".join(repr(c) for c in CATEGORIES)}.\n'
        '- "sense": a few words saying which meaning the passage uses the name in, so the '
        "same word used with two meanings can be told apart.\n"
        '- "quote": the words of the passage that define or explain the concept, copied '
        "exactly as they appear, at most two sentences.\n"
        "- \"explicit\": true when the quote is a definition ('X means ...', 'X is defined "
        "as ...', an entry of a definitions section), false when it only uses or explains "
        "the concept.\n"
        '- "variants": the other ways the passage refers to the same concept, each '
        '{"text": ..., "kind": ...} with kind one of '
        f"{', '.join(repr(k) for k in VARIANT_KINDS)}: 'form' for a case, plural or article "
        "difference, 'reference' for a phrase such as 'as defined in Section 2'. An empty "
        "list when there are none.\n\n"
        f"Passages:\n\n{numbered}\n\n"
        "Respond with only a JSON array of these objects, no prose, no code fences. An empty "
        "array when the passages hold no such concept."
    )


def read_mentions(
    raw: str, batch: Sequence[Passage], first_id: int, left_out: LeftOut
) -> list[Mention]:
    """The mentions in the model's answer for ``batch``, numbered from ``first_id``. A mention of
    another category, of a passage not in the batch or whose quote is not the passage's own
    text is counted in ``left_out``."""
    by_ordinal = {p.ordinal: p for p in batch}
    what = f"passages {batch[0].ordinal}-{batch[-1].ordinal}"
    found = _json(raw, what)
    if not isinstance(found, list):
        raise ScanFailed(f"the model's answer to {what} was not a list")
    mentions: list[Mention] = []
    for item in found:
        if not isinstance(item, dict):
            left_out.malformed += 1
            continue
        name = _name(item.get("name"))
        quote = item.get("quote")
        sense = item.get("sense")
        passage = item.get("passage")
        if (
            name is None
            or not isinstance(quote, str)
            or not quote.strip()
            or not isinstance(sense, str)
            or not isinstance(item.get("explicit"), bool)
            or not isinstance(passage, int)
            or isinstance(passage, bool)
        ):
            left_out.malformed += 1
            continue
        if item.get("category") not in CATEGORIES:
            left_out.not_a_concept += 1
            continue
        held = by_ordinal.get(passage)
        if held is None:
            left_out.unknown_passage += 1
            continue
        if _plain(quote) not in _plain(held.text):
            left_out.quote_not_in_passage += 1
            continue
        variants, bad = _variants(item.get("variants"))
        left_out.malformed += bad
        mentions.append(
            Mention(
                id=first_id + len(mentions),
                passage=passage,
                name=name,
                category=item["category"],
                sense=sense.strip(),
                quote=_SPACE.sub(" ", quote).strip(),
                explicit=item["explicit"],
                variants=tuple(variants),
            )
        )
    return mentions


# -- step 2: candidates -------------------------------------------------------------------------


def resolution_prompt(
    title: str, mentions: Sequence[Mention], passages: Mapping[int, Passage]
) -> str:
    """The prompt resolving ``mentions`` into the concepts they are mentions of."""

    def line(m: Mention) -> str:
        variants = ", ".join(f"{v.text} ({v.kind})" for v in m.variants) or "none"
        return (
            f"M{m.id} [{m.category}] {m.name!r} -- sense: {m.sense}; variants: {variants}; "
            f"{'definition' if m.explicit else 'explanation'} in P{m.passage}"
            f'{_where(passages[m.passage])}: "{m.quote}"'
        )

    listed = "\n".join(line(m) for m in mentions)
    return (
        "You are helping a data steward build a business glossary from a document. Below are "
        f"the mentions of concepts found in the document {title!r}, each numbered M<n>, with "
        "the passage P<n> it is in and the passage's own words for it.\n\n"
        "Group the mentions into concepts:\n"
        "- Mentions of one concept form one group however they are worded: a different case, "
        "a plural, an article, an abbreviation, another name, or a reference such as 'as "
        "defined in Section 2'.\n"
        "- One word used with two different meanings in different places forms two groups, "
        "one per meaning. Name such groups apart, the word with its meaning in brackets, "
        "e.g. 'account (customer relationship)' and 'account (ledger)'.\n"
        "- Put every mention in exactly one group.\n\n"
        "For each group give an object with:\n"
        '- "name": the concept\'s preferred name, singular, lowercase.\n'
        '- "sense": a few words saying what the concept is.\n'
        '- "mentions": the numbers n of its mentions M<n>.\n'
        '- "variants": the other names the document uses for it, each {"text": ..., '
        '"kind": ...} with kind one of '
        + ", ".join(repr(k) for k in VARIANT_KINDS)
        + ".\n"
        + '- "conflicts": pairs [a, b] of its mentions whose explanations cannot both be true. '
        "Do not reconcile them. An empty list when there are none.\n"
        '- "definition" and "uses": only when none of its mentions is a definition. '
        "definition is one or two sentences in plain business language drawn only from its "
        + "mentions' quotes, never blending explanations that conflict; uses lists the mention "
        "numbers it drew on. Leave both out when a mention is a definition.\n\n"
        f"Mentions:\n{listed}\n\n"
        "Respond with only a JSON array of these objects, no prose, no code fences."
    )


def _ids(raw: Any) -> list[int] | None:
    if not isinstance(raw, list) or not all(
        isinstance(i, int) and not isinstance(i, bool) for i in raw
    ):
        return None
    return list(raw)


def read_groups(raw: str, mentions: Sequence[Mention], left_out: LeftOut) -> list[Group]:
    """The groups in the model's resolution of ``mentions``. A mention in no group is counted
    in ``left_out`` as unresolved; a mention named by a second group stays in its first."""
    found = _json(raw, "the mentions' resolution")
    if not isinstance(found, list):
        raise ScanFailed("the model's resolution of the mentions was not a list")
    known = {m.id for m in mentions}
    placed: set[int] = set()
    groups: list[Group] = []
    for item in found:
        name = _name(item.get("name")) if isinstance(item, dict) else None
        ids = _ids(item.get("mentions")) if isinstance(item, dict) else None
        if name is None or ids is None:
            left_out.malformed += 1
            continue
        members = tuple(i for i in dict.fromkeys(ids) if i in known and i not in placed)
        if not members:
            continue
        placed.update(members)
        variants, bad = _variants(item.get("variants"))
        left_out.malformed += bad
        conflicts: list[tuple[int, int]] = []
        for pair in item.get("conflicts") or []:
            ids_pair = _ids(pair)
            if ids_pair is None or len(ids_pair) != 2 or ids_pair[0] == ids_pair[1]:
                left_out.malformed += 1
                continue
            if all(i in members for i in ids_pair):
                conflicts.append((ids_pair[0], ids_pair[1]))
        definition = item.get("definition")
        uses = _ids(item.get("uses")) or []
        groups.append(
            Group(
                name=name,
                sense=item["sense"].strip() if isinstance(item.get("sense"), str) else "",
                mentions=members,
                variants=tuple(variants),
                conflicts=tuple(conflicts),
                definition=definition.strip()
                if isinstance(definition, str) and definition.strip()
                else None,
                uses=tuple(i for i in dict.fromkeys(uses) if i in members),
            )
        )
    left_out.unresolved += len(known - placed)
    return groups


def _base(name: str) -> str:
    """A name without its bracketed meaning: 'account (ledger)' -> 'account'."""
    return normal(re.sub(r"\s*\([^)]*\)\s*$", "", name))


def candidates(
    groups: Sequence[Group], mentions: Sequence[Mention], passages: Mapping[int, Passage]
) -> list[Candidate]:
    """The candidate terms of ``groups``, in the order the document first mentions them."""
    by_id = {m.id: m for m in mentions}
    out: list[Candidate] = []
    for g in groups:
        members = sorted((by_id[i] for i in g.mentions), key=lambda m: (m.passage, m.id))
        explicit = next((m for m in members if m.explicit), None)
        if explicit is not None:
            definition, kind, used = explicit.quote, EXPLICIT, {explicit.id}
        elif g.definition is not None and g.uses:
            definition, kind, used = g.definition, SYNTHESIZED, set(g.uses)
        else:
            definition, kind, used = None, MISSING, set()

        def cite(m: Mention) -> dict[str, Any]:
            p = passages[m.passage]
            role = (
                ("definition" if kind == EXPLICIT else "synthesis")
                if m.id in used
                else "supporting"
            )
            return {
                "mention": m.id,
                "ordinal": p.ordinal,
                "section": p.section,
                "page": p.page,
                "quote": m.quote,
                "explicit": m.explicit,
                "sense": m.sense,
                "role": role,
            }

        variants: dict[str, Variant] = {}
        for v in [*g.variants, *(v for m in members for v in m.variants)]:
            key = v.text.lower()
            if key != g.name and key not in variants:
                variants[key] = v
        for m in members:
            if m.name != g.name and m.name not in variants:
                variants[m.name] = Variant(
                    m.name, "form" if normal(m.name) == normal(g.name) else "alternative name"
                )
        out.append(
            Candidate(
                name=g.name,
                category=Counter(m.category for m in members).most_common(1)[0][0],
                sense=g.sense or members[0].sense,
                definition=definition,
                definition_kind=kind,
                passages=[cite(m) for m in members],
                variants=[
                    {"text": v.text, "kind": v.kind, "keep": v.kind in SYNONYM_KINDS}
                    for v in variants.values()
                ],
                conflicts=[[cite(by_id[a]), cite(by_id[b])] for a, b in g.conflicts],
            )
        )
    bases: dict[str, list[str]] = {}
    for c in out:
        bases.setdefault(_base(c.name), []).append(c.name)
    for c in out:
        c.homonyms = [n for n in bases[_base(c.name)] if n != c.name]
    out.sort(key=lambda c: min(p["ordinal"] for p in c.passages))
    return out


def match(
    found: Iterable[Candidate],
    terms: Sequence[Mapping[str, Any]],
    synonym_of: Iterable[tuple[int, int]],
) -> None:
    """Mark each candidate with the glossary terms it duplicates or names again.

    ``terms`` are the glossary's terms (``id``, ``name``); ``synonym_of`` its SYNONYM_OF edges
    (from, to): the from term is a synonym of the to term. A candidate named as a term is a
    ``duplicate`` of it; one whose variant is a term's name, or named as a term that is itself a
    synonym, is a ``synonym`` of it. Either way the candidate is proposed as a merge into the term
    it is another name for -- the preferred term, when the match is a synonym of one.
    """
    by_name: dict[str, Mapping[str, Any]] = {}
    for t in terms:
        by_name.setdefault(normal(t["name"]), t)
    preferred = dict(synonym_of)
    names = {t["id"]: t["name"] for t in terms}
    for c in found:
        seen: dict[int, dict[str, Any]] = {}
        for via in [c.name, *(v["text"] for v in c.variants)]:
            term = by_name.get(normal(via))
            if term is not None and term["id"] not in seen:
                seen[term["id"]] = {"term_id": term["id"], "name": term["name"], "via": via}
        c.matches = list(seen.values())
        if not c.matches:
            continue
        first = c.matches[0]
        target = preferred.get(first["term_id"], first["term_id"])
        if target not in names:
            target = first["term_id"]
        c.match_term_id = target
        c.match_kind = (
            DUPLICATE if first["via"] == c.name and target == first["term_id"] else SYNONYM
        )


# -- the scan -----------------------------------------------------------------------------------


@dataclass
class Result:
    candidates: list[Candidate]
    passages: int
    mentions: int
    left_out: LeftOut
    calls: int


async def run(
    title: str,
    passages: Sequence[Passage],
    call: Callable[[str, int], Awaitable[str]],
) -> Result:
    """Scan ``passages`` with ``call`` (a prompt and an output limit to the model's answer)."""
    if not passages:
        raise ScanFailed(f"the document {title!r} holds no passages to scan")
    left_out = LeftOut()
    mentions: list[Mention] = []
    calls = 0
    for batch in batches(passages):
        raw = await call(extraction_prompt(title, batch), EXTRACT_MAX_TOKENS)
        calls += 1
        mentions.extend(read_mentions(raw, batch, len(mentions), left_out))
    by_ordinal = {p.ordinal: p for p in passages}
    if not mentions:
        return Result([], len(passages), 0, left_out, calls)
    raw = await call(resolution_prompt(title, mentions, by_ordinal), RESOLVE_MAX_TOKENS)
    calls += 1
    groups = read_groups(raw, mentions, left_out)
    return Result(
        candidates(groups, mentions, by_ordinal), len(passages), len(mentions), left_out, calls
    )
