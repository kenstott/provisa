# Copyright (c) 2026 Kenneth Stott
# Canary: 5e1a9c37-82d4-4b6f-a0e3-c47d9b2f8a15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Extract English noun singular/plural pairs from ESDB (SCOWLv2) into provisa/core/data (REQ-194).

Source: ``data/scowl-pre.txt`` of https://github.com/en-wl/wordlist (branch v2), Kevin Atkinson's
English Speller Database; its notice is kept beside the table in ``THIRD_PARTY_NOTICES``.

Usage:

    python scripts/gen_noun_plurals.py <scowl-pre.txt> <Copyright>

after scripts/gen_plural_frequencies.py has written the frequency table it reads.

What is taken (the file's notation is documented in the source's README.md):

- Only entries at ESDB size 80 or below (sizes 70 and below are words "found in most
  dictionaries"; the Copyright's UKACD clause covers generated lists above 80), and only American
  spellings: no ``AU`` region and no ``D``-only spelling (the Copyright's AU clause). A line's
  size is the first number of its SCOWL-INFO, its spelling the VARIANT-INFO token for ``A``,
  ``_`` or every spelling.
- Only common nouns: lemmas tagged ``<n>`` with no POS class (no places, names, trademarks) that
  are one lower-case word.
- A group's first ENTRY on a ``<n>`` line is the plural slot (then the possessives); ``-`` leaves
  it to another line of the group. Each plural alternative of ``(a | V: b)`` keeps its variant
  level; an alternative annotated ``~`` (generally not used), ``!`` (nearly non-existent) or ``@``
  (archaic), or at level ``x`` (invalid), is dropped.
- A noun's plural is chosen across its groups (senses, in file order), each alternative ranked by
  - first, an alternative other than the singular that the database also lists as a common noun
    lemma of its own (the everyday word: ``person``'s plurals are ``persons`` and ``~: people``,
    and ``people`` is itself a size-35 noun, so ``person`` -> ``people``);
  - then its variant level's tie class, then its frequency within a tie (below), then its variant
    level (non-variant, ``.``, ``=``, ``?``, ``v``, ``~``, ``V``, ``-``);
  - then file order.
- Alternatives in the same tie class (``_TIE_CLASS``: every level standard dictionaries accept,
  non-variant down to ``V``, is one class; ``-`` is the next) after the everyday-word rule are
  ranked by their general-English frequency, ``provisa/core/data/plural_frequencies.tsv``
  (scripts/gen_plural_frequencies.py, Google Books Ngram 1-grams): ``schema``'s plurals are
  ``schemata`` and ``V: schemas``, and ``schemas`` is the more frequent, so ``schema`` ->
  ``schemas``. Frequency never applies to a noun with one plural and never lifts a form over
  another class.
- A common noun with no plural in any group (``news``, ``series``) is listed as its own plural:
  the dictionary knows the word and gives it no other form.
- A non-variant plural that the 12dicts source (the count of standard dictionaries listing a word
  as a headword) places at a more common size than its singular is a word of its own: it is listed as
  its own singular as well (``data`` is at 12dicts size 35, ``datum`` at 50, so ``data`` stays
  ``data`` both ways while ``datum`` -> ``data``). Its row carries a third field, ``own``.
- A noun whose spelling is also a function word (preposition, conjunction, pronoun, determiner)
  and whose every plural is the plural of a different noun is that other noun's spelling, not the
  function word's (``by`` read as ``bye``, plural ``byes``): it is listed as its own plural only,
  so it is not inflected.

The table has one row per (singular, plural) pair: the chosen plural first, then every other
accepted alternative, so each plural maps back to its singular. A word of its own (12dicts rule)
follows every pair as ``word<TAB>word<TAB>own``.
"""

from __future__ import annotations

import gzip
import re
import sys
from pathlib import Path

_OUT = Path(__file__).resolve().parents[1] / "provisa" / "core" / "data"
_MAX_SIZE = 80
_LEVELS = ["", ".", "=", "?", "v", "~", "V", "-"]  # x (invalid) and @ (archaic) are dropped
_VARIANT_TOKEN = re.compile(r"^[ABZCD_]?[.=?v~V@\-x]?$")
_WORD = re.compile(r"^[a-z]+$")
_FUNCTION_POS = frozenset({"pp", "c", "pn", "d"})
_HEADWORD_TAG = "[12dicts]"
# Variant levels by tie class (REQ-194, amended 2026-11-18: a tie between dictionary forms is broken
# by word frequency). The requirement's own example, schema: schemata | V: schemas, is a tie, so
# every level the database calls acceptable -- non-variant down to V, the forms standard
# dictionaries list -- is one class; "-" (generally not in standard dictionaries) ranks below it.
# Alternatives of one noun in the same class are tied and the more frequent form wins.
_TIE_CLASS = {"": 0, ".": 0, "=": 0, "?": 0, "v": 0, "~": 0, "V": 0, "-": 1}
_FREQUENCIES = _OUT / "plural_frequencies.tsv"

Alternative = tuple[str, str, int]  # (plural, variant level, file order)


def _tag_size(line: str, tag: str) -> int | None:
    """The size at which a SCOWL-INFO lists ``tag``'s source; None when that source lacks it."""
    size = None
    for token in line.split(": ", 1)[0].split():
        if token.isdigit():
            size = int(token)
        elif token == tag:
            return size
    return None


def _american_level(info: str) -> str | None:
    """The variant level a VARIANT-INFO gives American spelling; None when it gives none."""
    tokens = [t for t in info.split() if not t.startswith("{")]
    if not tokens:
        return ""
    for token in tokens:
        spelling = token[0] if token[:1] in "ABZCD_" and token[:1] else ""
        if spelling in ("", "A", "_"):
            return token[len(spelling) :]
    return None


def _split_line(line: str) -> tuple[int, bool, str, str | None, str, list[str]] | None:
    """(size, au, variant info, its American level, lemma info, entries) of one LINE; None for a
    non-entry line."""
    body = line.split("#", 1)[0].rstrip()
    if not body or not body[0].isdigit():
        return None
    info, _, rest = body.partition(": ")
    size = int(info.split()[0])
    au = "AU" in info.split()
    variant_info, level = "", ""
    head, sep, tail = rest.partition(": ")
    if (
        sep
        and "<" not in head
        and all(_VARIANT_TOKEN.match(t) for t in head.split() if t[0] != "{")
    ):
        variant_info, level = head, _american_level(head)
        rest = tail
    lemma_info, _, entries = rest.partition(": ")
    return size, au, variant_info, level, lemma_info, _entries(entries)


def _entries(text: str) -> list[str]:
    """The ENTRY list, split on top-level ', '."""
    out, depth, cur = [], 0, ""
    for ch in text:
        depth += ch == "("
        depth -= ch == ")"
        if ch == "," and depth == 0:
            out.append(cur.strip())
            cur = ""
        else:
            cur += ch
    if cur.strip():
        out.append(cur.strip())
    return out


def _alternatives(entry: str) -> list[tuple[str, str]]:
    """(word, American variant level) of each acceptable alternative of one ENTRY."""
    if entry.startswith("("):
        parts = entry[1:-1].split(" | ")
    else:
        parts = [entry]
    out = []
    for part in parts:
        info, sep, word = part.rpartition(": ")
        level = _american_level(info) if sep else ""
        if level is None or level in ("x", "@"):
            continue
        if word[-1:] in "~!@":
            continue
        word = word.rstrip("*-†")
        if _WORD.match(word):
            out.append((word, level))
    return out


def _lemma(lemma_info: str) -> tuple[str, str] | None:
    """(word, pos) of a LEMMA-INFO; None without a POS."""
    m = re.match(r"^([-@!]?)(\S+?)[*\-@~!†]? <([^>]+)>", lemma_info)
    if not m:
        return None
    return m.group(2), m.group(3)


def _database_order(singular: str, alts: list[Alternative], lemmas: set[str]) -> list[Alternative]:
    """``alts`` in the database's order, one per word: the everyday-word rule, then the variant
    level, then file order."""
    ranked = sorted(
        alts,
        key=lambda a: (
            not (a[0] != singular and a[0] in lemmas),
            _LEVELS.index(a[1]) if a[1] in _LEVELS else len(_LEVELS),
            a[2],
        ),
    )
    seen: set[str] = set()
    out = []
    for alt in ranked:
        if alt[0] not in seen:
            seen.add(alt[0])
            out.append(alt)
    return out


def _ties(singular: str, ordered: list[Alternative], lemmas: set[str]) -> list[list[int]]:
    """The positions in ``ordered`` of each tie: two or more alternatives under the same everyday-word
    rule and in the same tie class. An alternative spelled as the singular (``head``'s plural
    ``head``) is never in a tie: a word count cannot tell its plural uses from its singular ones, so
    it keeps the database's position."""
    groups: dict[tuple[bool, int], list[int]] = {}
    for i, (word, level, _) in enumerate(ordered):
        if word == singular or level not in _TIE_CLASS:
            continue
        groups.setdefault((word in lemmas, _TIE_CLASS[level]), []).append(i)
    return [positions for positions in groups.values() if len(positions) > 1]


def _ranked(
    singular: str, alts: list[Alternative], lemmas: set[str], frequencies: dict[str, int]
) -> list[str]:
    """``alts``' words, best first: the database's order, with each tie reordered by general-English
    frequency (more frequent first, the database's order between equal counts). Frequency moves a
    form only among the positions its tie holds."""
    ordered = _database_order(singular, alts, lemmas)
    words = [a[0] for a in ordered]
    for positions in _ties(singular, ordered, lemmas):
        tied = sorted((words[i] for i in positions), key=lambda w: -frequencies[w])
        for i, word in zip(positions, tied, strict=True):
            words[i] = word
    return words


def tied_forms(scowl_pre: Path) -> list[str]:
    """Every plural alternative in a tie, whose frequency decides: the words the frequency table
    must cover."""
    nouns, _ = _nouns(scowl_pre)
    lemmas = {singular for singular, _ in nouns}
    forms: set[str] = set()
    for singular, alts in nouns:
        ordered = _database_order(singular, alts, lemmas)
        for positions in _ties(singular, ordered, lemmas):
            forms.update(ordered[i][0] for i in positions)
    return sorted(forms)


def _nouns(scowl_pre: Path) -> tuple[list[tuple[str, list[Alternative]]], list[str]]:
    """(each common noun with its plural alternatives, in file order; the plurals that are words of
    their own). A noun with no alternative is its own plural."""
    plurals: dict[str, list[Alternative]] = {}  # singular -> (plural, level, order)
    nouns: list[str] = []
    function_words: set[str] = set()
    own_words: list[str] = []  # plurals that are words of their own (12dicts rule)
    order = 0
    # The group's lemmas by variant info: a "-" line belongs to the lemma with its variant info
    # (a line without one, to the group's first lemma). None marks a lemma not taken.
    group: dict[str, str | None] = {}
    headword: dict[str, int | None] = {}  # the group's lemma 12dicts sizes, by variant info
    first: str | None = None
    for raw in scowl_pre.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            group, headword, first = {}, {}, None
            continue
        parsed = _split_line(raw)
        if parsed is None:
            continue
        size, au, variant_info, level, lemma_info, entries = parsed
        lemma = _lemma(lemma_info)
        if (
            lemma is not None
            and lemma[0] != "-"
            and lemma[1] in _FUNCTION_POS
            and size <= _MAX_SIZE
        ):
            function_words.add(lemma[0])
        if lemma is None or lemma[1] != "n":
            continue
        word = lemma[0]
        if word != "-":
            taken = not (
                size > _MAX_SIZE
                or au
                or level is None
                or level in ("x", "@")
                or not _WORD.match(word)
            )
            group[variant_info] = word if taken else None
            headword[variant_info] = _tag_size(raw, _HEADWORD_TAG)
            if first is None:
                first = variant_info
            if not taken:
                continue
            if word not in plurals:
                plurals[word] = []
                nouns.append(word)
            target: str | None = word
        else:
            key = variant_info if variant_info in group else (first if not variant_info else None)
            target = group.get(key) if key is not None else None
        if target is None or size > _MAX_SIZE or au or level is None or not entries:
            continue
        lemma_key = variant_info if variant_info in group else first
        singular_dicts = headword.get(lemma_key) if lemma_key is not None else None
        plural_dicts = _tag_size(raw, _HEADWORD_TAG)
        for plural, alt_level in _alternatives(entries[0]):
            order += 1
            plurals[target].append((plural, alt_level, order))
            if (
                plural != target
                and alt_level == ""
                and plural_dicts is not None
                and singular_dicts is not None
                and plural_dicts < singular_dicts
                and plural not in own_words
            ):
                own_words.append(plural)
    owners: dict[str, set[str]] = {}
    for singular in nouns:
        for plural, _, _ in plurals[singular]:
            if plural != singular:
                owners.setdefault(plural, set()).add(singular)
    out: list[tuple[str, list[Alternative]]] = []
    for singular in nouns:
        alts = plurals[singular]
        if (
            alts
            and singular in function_words
            and all(owners.get(a[0], set()) - {singular} for a in alts)
        ):
            alts = []
        out.append((singular, alts))
    return out, own_words


def extract(
    scowl_pre: Path, frequencies: dict[str, int]
) -> tuple[list[tuple[str, str]], list[str]]:
    """(the (singular, plural) rows, the plurals that are words of their own). ``frequencies``
    gives the general-English count of every form :func:`tied_forms` lists."""
    nouns, own_words = _nouns(scowl_pre)
    lemmas = {singular for singular, _ in nouns}
    rows: list[tuple[str, str]] = []
    for singular, alts in nouns:
        if not alts:
            rows.append((singular, singular))
            continue
        rows.extend((singular, plural) for plural in _ranked(singular, alts, lemmas, frequencies))
    return rows, own_words


_NOTICE_HEAD = """Third-party notices
===================

provisa/core/data/noun_plurals.tsv.gz
-------------------------------------
The English noun singular/plural pairs (REQ-194) are extracted from the English Speller Database
(ESDB, previously SCOWLv2) by Kevin Atkinson, file data/scowl-pre.txt of
https://github.com/en-wl/wordlist (branch v2), by scripts/gen_noun_plurals.py: common nouns at
ESDB size 80 or below, American spelling only (no AU region, no D spelling), one plural chosen per
noun as that script describes. Its notice follows, verbatim.

"""


_NGRAM_NOTICE = """
provisa/core/data/plural_frequencies.tsv
----------------------------------------
The counts of the tied noun plurals (REQ-194) are taken from the Google Books Ngram Viewer English
1-grams, version 20200217 (Google Books Ngram Viewer, http://books.google.com/ngrams), by
scripts/gen_plural_frequencies.py: each word's match count over 2000-2019. The source is licensed
under the Creative Commons Attribution 3.0 Unported License
(https://creativecommons.org/licenses/by/3.0/). Only these counts are bundled, not the corpus.
"""


def main(argv: list[str]) -> int:
    scowl_pre, copyright_file = Path(argv[0]), Path(argv[1])
    frequencies = {
        word: int(count)
        for word, count in (
            line.split("\t") for line in _FREQUENCIES.read_text(encoding="utf-8").splitlines()
        )
    }
    rows, own_words = extract(scowl_pre, frequencies)
    _OUT.mkdir(parents=True, exist_ok=True)
    # Own words after every pair, so a noun that is also a lemma (opera -> operas) keeps its plural.
    text = "".join(f"{s}\t{p}\n" for s, p in rows) + "".join(f"{w}\t{w}\town\n" for w in own_words)
    data = text.encode("utf-8")
    # mtime=0: the same source gives the same bytes.
    (_OUT / "noun_plurals.tsv.gz").write_bytes(gzip.compress(data, mtime=0))
    (_OUT / "THIRD_PARTY_NOTICES").write_text(
        _NOTICE_HEAD + copyright_file.read_text(encoding="utf-8") + _NGRAM_NOTICE, encoding="utf-8"
    )
    print(
        f"{len(rows)} pairs, {len({s for s, _ in rows})} nouns, own words: {', '.join(own_words)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
