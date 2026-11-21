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
  - then its sense: the group whose plural the most standard dictionaries list (the smallest
    12dicts size of the group's plural lines; between equal sizes, the group the database lists
    first) is the noun's main sense (``axis`` the line, plural ``axes`` at 12dicts 35, before
    ``axis`` the deer, ``axises`` at 65; ``goose`` the bird, ``geese``, before ``goose`` the poke,
    ``gooses``, both at 35), and the other senses follow in the same order;
  - then its variant level's class (``_TIE_CLASS``: every level standard dictionaries accept,
    non-variant down to ``V``, is one class; ``-`` is the next);
  - then, in the accepted class, a zero plural the database lists first with the regular form
    starred, used depending on usage (``fish <n>: (fish | fishes*)`` -> ``fish``); a zero plural
    in any other pattern does not win (``buck <n>: (bucks | ?: buck)`` -> ``bucks``,
    ``percent <n>: (percent | ?: percents)`` -> ``percents``); a legacy ``~`` alternative (no
    level information) counts for neither rule;
  - then, in the accepted class, the regularly formed plural (the rule-based form
    ``provisa.core.inflection.regular_plural`` gives a word the dictionary does not list) first:
    ``schema`` -> ``schemas``, ``index`` -> ``indexes``, ``maximum`` -> ``maximums``,
    ``focus`` -> ``focuses``;
  - then its frequency within a tie (below), then its variant level (non-variant, ``.``, ``=``,
    ``?``, ``v``, ``~``, ``V``, ``-``);
  - then file order.
- Alternatives under the same everyday-word rule, sense and class with no regular accepted form
  among them are tied and ranked by their general-English frequency,
  ``provisa/core/data/plural_frequencies.tsv`` (scripts/gen_plural_frequencies.py, Google Books
  Ngram 1-grams). Frequency never applies to a noun with one plural, never lifts a form over
  another class or sense, and never over a regular accepted form.
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

from provisa.core.inflection import regular_plural

_OUT = Path(__file__).resolve().parents[1] / "provisa" / "core" / "data"
_MAX_SIZE = 80
_LEVELS = ["", ".", "=", "?", "v", "~", "V", "-"]  # x (invalid) and @ (archaic) are dropped
_VARIANT_TOKEN = re.compile(r"^[ABZCD_]?[.=?v~V@\-x]?$")
_WORD = re.compile(r"^[a-z]+$")
_FUNCTION_POS = frozenset({"pp", "c", "pn", "d"})
_HEADWORD_TAG = "[12dicts]"
# Variant levels by class (REQ-194, amended 2026-11-18). Every level the database calls acceptable
# -- non-variant down to V, the forms standard dictionaries list -- is the accepted class 0; "-"
# (generally not in standard dictionaries) ranks below it. Among a noun's accepted alternatives the
# regularly formed plural wins (schema: schemata | V: schemas -> schemas); alternatives of one class
# with no regular form among them are tied and the more frequent form wins.
_TIE_CLASS = {"": 0, ".": 0, "=": 0, "?": 0, "v": 0, "~": 0, "V": 0, "-": 1}
_ACCEPTED = 0
# ESDB's legacy level: an alternative carried over from SCOWLv1 with no level information.
_LEGACY = "~"
_FREQUENCIES = _OUT / "plural_frequencies.tsv"
# A sense whose plural no 12dicts source lists ranks after every listed one.
_UNLISTED = 1_000

# (plural, variant level, file order, sense rank: 0 for the noun's main sense, True for a zero plural
# listed first on a line whose regular form is starred: ``fish <n>: (fish | fishes*)``)
Alternative = tuple[str, str, int, int, bool]
# (the sense's 12dicts size, the plurals its lines list, its group number)
Sense = tuple[int, frozenset[str], int]


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


def _alternatives(entry: str) -> list[tuple[str, str, bool]]:
    """(word, American variant level, starred) of each acceptable alternative of one ENTRY. ``*``
    marks a form used depending on usage."""
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
        starred = "*" in word
        word = word.rstrip("*-†")
        if _WORD.match(word):
            out.append((word, level, starred))
    return out


def _lemma(lemma_info: str) -> tuple[str, str] | None:
    """(word, pos) of a LEMMA-INFO; None without a POS."""
    m = re.match(r"^([-@!]?)(\S+?)[*\-@~!†]? <([^>]+)>", lemma_info)
    if not m:
        return None
    return m.group(2), m.group(3)


def _counted(alt: Alternative) -> bool:
    """True when ``alt`` counts as accepted for the regular-wins and zero-plural rules: an accepted
    level the database gives it, not the legacy ``~`` (no level information; REQ-194, amended
    2026-11-18: deer, criteria, calves stay)."""
    return alt[1] != _LEGACY and _TIE_CLASS.get(alt[1]) == _ACCEPTED


def _regular(singular: str, alt: Alternative) -> bool:
    """True when ``alt`` is a counted alternative formed regularly from ``singular``."""
    return _counted(alt) and alt[0] == regular_plural(singular)


def _zero(singular: str, alt: Alternative) -> bool:
    """True when ``alt`` is a counted zero plural (spelled as ``singular``) that the database lists
    first with the regular form starred, used depending on usage (``fish <n>: (fish | fishes*)``).
    Only that pattern lets a zero plural win over a regular one (REQ-194, amended 2026-11-19);
    elsewhere the regular form keeps winning (``buck <n>: (bucks | ?: buck)`` -> bucks)."""
    return _counted(alt) and alt[0] == singular and alt[4]


def _database_order(singular: str, alts: list[Alternative], lemmas: set[str]) -> list[Alternative]:
    """``alts`` in the database's order, one per word: the everyday-word rule, then the sense, then
    the variant level's class, then a counted zero plural where its sense also has a counted
    regular one, then the regular counted form, then the variant level, then file order."""
    regular_senses = {a[3] for a in alts if _regular(singular, a)}
    ranked = sorted(
        alts,
        key=lambda a: (
            not (a[0] != singular and a[0] in lemmas),
            a[3],
            _TIE_CLASS.get(a[1], len(_TIE_CLASS)),
            not (a[3] in regular_senses and _zero(singular, a)),
            not _regular(singular, a),
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


def _owners(nouns: list[tuple[str, list[Alternative]]]) -> dict[str, set[str]]:
    """plural -> the nouns that list it as a plural other than themselves."""
    owners: dict[str, set[str]] = {}
    for singular, alts in nouns:
        for plural, *_ in alts:
            if plural != singular:
                owners.setdefault(plural, set()).add(singular)
    return owners


def _ties(
    singular: str, ordered: list[Alternative], lemmas: set[str], owners: dict[str, set[str]]
) -> list[list[int]]:
    """The positions in ``ordered`` of each tie: two or more alternatives under the same everyday-word
    rule, in the same sense and the same class, where that class holds no regular accepted form (a
    regular form wins over every irregular one, so frequency only chooses among irregular forms). An
    alternative a word count cannot measure is never in a tie and keeps the database's position: one
    spelled as the singular (``head``'s plural ``head``: its count holds the singular's uses), or one
    that is also another noun's plural (``axis``'s ``axes``, also ``axe``'s)."""
    groups: dict[tuple[bool, int, int], list[int]] = {}
    regular: set[tuple[bool, int, int]] = set()
    for i, alt in enumerate(ordered):
        word, level, _, sense, _ = alt
        if level not in _TIE_CLASS:
            continue
        key = (word in lemmas, sense, _TIE_CLASS[level])
        if _regular(singular, alt):
            regular.add(key)
        if word == singular or owners.get(word, set()) - {singular}:
            continue
        groups.setdefault(key, []).append(i)
    return [
        positions for key, positions in groups.items() if len(positions) > 1 and key not in regular
    ]


def _ranked(
    singular: str,
    alts: list[Alternative],
    lemmas: set[str],
    owners: dict[str, set[str]],
    frequencies: dict[str, int],
) -> list[str]:
    """``alts``' words, best first: the database's order, with each tie reordered by general-English
    frequency (more frequent first, the database's order between equal counts). Frequency moves a
    form only among the positions its tie holds."""
    ordered = _database_order(singular, alts, lemmas)
    words = [a[0] for a in ordered]
    for positions in _ties(singular, ordered, lemmas, owners):
        tied = sorted((words[i] for i in positions), key=lambda w: -frequencies[w])
        for i, word in zip(positions, tied, strict=True):
            words[i] = word
    return words


def tied_forms(scowl_pre: Path) -> list[str]:
    """Every plural alternative in a tie, whose frequency decides: the words the frequency table
    must cover."""
    nouns, _, _ = _nouns(scowl_pre)
    lemmas = {singular for singular, _ in nouns}
    owners = _owners(nouns)
    forms: set[str] = set()
    for singular, alts in nouns:
        ordered = _database_order(singular, alts, lemmas)
        for positions in _ties(singular, ordered, lemmas, owners):
            forms.update(ordered[i][0] for i in positions)
    return sorted(forms)


def _nouns(
    scowl_pre: Path,
) -> tuple[list[tuple[str, list[Alternative]]], list[str], dict[str, list[Sense]]]:
    """(each common noun with its plural alternatives, in file order; the plurals that are words of
    their own; each noun's senses). A noun with no alternative is its own plural."""
    # singular -> (plural, level, order, group number, the plural line's 12dicts size, star zero)
    found: dict[str, list[tuple[str, str, int, int, int, bool]]] = {}
    nouns: list[str] = []
    function_words: set[str] = set()
    own_words: list[str] = []  # plurals that are words of their own (12dicts rule)
    order = 0
    group_no = 0
    # The group's lemmas by variant info: a "-" line belongs to the lemma with its variant info
    # (a line without one, to the group's first lemma). None marks a lemma not taken.
    group: dict[str, str | None] = {}
    headword: dict[str, int | None] = {}  # the group's lemma 12dicts sizes, by variant info
    first: str | None = None
    for raw in scowl_pre.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            group, headword, first = {}, {}, None
            group_no += 1
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
            if word not in found:
                found[word] = []
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
        line_dicts = _UNLISTED if plural_dicts is None else plural_dicts
        alternatives = _alternatives(entries[0])
        star_zero = (
            bool(alternatives)
            and alternatives[0][0] == target
            and any(w == regular_plural(target) and starred for w, _, starred in alternatives)
        )
        for i, (plural, alt_level, _) in enumerate(alternatives):
            order += 1
            found[target].append(
                (plural, alt_level, order, group_no, line_dicts, star_zero and i == 0)
            )
            if (
                plural != target
                and alt_level == ""
                and plural_dicts is not None
                and singular_dicts is not None
                and plural_dicts < singular_dicts
                and plural not in own_words
            ):
                own_words.append(plural)
    plurals: dict[str, list[Alternative]] = {}
    senses: dict[str, list[Sense]] = {}
    for singular in nouns:
        # A sense's size is the most common 12dicts size of its plural lines; senses rank by size,
        # then by the database's group order.
        sense_size: dict[int, int] = {}
        for _, _, _, number, dicts, _ in found[singular]:
            sense_size[number] = min(dicts, sense_size.get(number, _UNLISTED))
        rank = {
            number: i
            for i, number in enumerate(sorted(sense_size, key=lambda n: (sense_size[n], n)))
        }
        plurals[singular] = [(p, lv, o, rank[n], z) for p, lv, o, n, _, z in found[singular]]
        senses[singular] = [
            (
                sense_size[n],
                frozenset(p for p, _, _, number, _, _ in found[singular] if number == n),
                n,
            )
            for n in sorted(sense_size)
        ]
    owners = _owners([(singular, plurals[singular]) for singular in nouns])
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
    return out, own_words, senses


def _reverse(rows: list[tuple[str, str]], senses: dict[str, list[Sense]]) -> list[tuple[str, str]]:
    """(singular, plural) for each plural that more than one noun lists: the noun singularizing it
    gives (REQ-194, amended 2026-11-18, the main-sense rule in reverse). Each candidate stands for
    the sense that lists the plural, and the candidates rank by
    - that sense's 12dicts size (most dictionaries first);
    - then a sense whose only plural is this form;
    - then a sense that gives the plural to this spelling alone before one whose spelling variants
      share it (``axes``: ``axis`` the line before ``ax``/``axe``, one sense spelled two ways);
    - then file order."""
    candidates: dict[str, list[str]] = {}
    for singular, plural in rows:
        if plural != singular and singular not in candidates.setdefault(plural, []):
            candidates[plural].append(singular)

    def sense(plural: str, singular: str) -> tuple[int, bool, int]:
        return min(
            (size, forms != {plural}, group)
            for size, forms, group in senses[singular]
            if plural in forms
        )

    out = []
    for plural, nouns in candidates.items():
        if len(nouns) == 1:
            continue
        chosen = {noun: sense(plural, noun) for noun in nouns}
        groups = [group for _, _, group in chosen.values()]
        out.append(
            (
                min(
                    nouns,
                    key=lambda n: (chosen[n][:2], groups.count(chosen[n][2]) > 1),
                ),
                plural,
            )
        )
    return out


def extract(
    scowl_pre: Path, frequencies: dict[str, int]
) -> tuple[list[tuple[str, str]], list[str], list[tuple[str, str]]]:
    """(the (singular, plural) rows, the plurals that are words of their own, the singular each
    plural more than one noun lists takes). ``frequencies`` gives the general-English count of every
    form :func:`tied_forms` lists."""
    nouns, own_words, senses = _nouns(scowl_pre)
    lemmas = {singular for singular, _ in nouns}
    owners = _owners(nouns)
    rows: list[tuple[str, str]] = []
    for singular, alts in nouns:
        if not alts:
            rows.append((singular, singular))
            continue
        rows.extend(
            (singular, plural) for plural in _ranked(singular, alts, lemmas, owners, frequencies)
        )
    return rows, own_words, _reverse(rows, senses)


_NOTICE_HEAD = """Third-party notices
===================

provisa/core/data/noun_plurals.tsv.gz
-------------------------------------
The English noun singular/plural pairs (REQ-194) are extracted from the English Speller Database
(ESDB, previously SCOWLv2) by Kevin Atkinson, file data/scowl-pre.txt of
https://github.com/en-wl/wordlist (branch v2, commit 1e5b7d3a72f47a71da5d28686c1dd4b397178485), by
scripts/gen_noun_plurals.py: common nouns at ESDB size 80 or below, American spelling only (no AU
region, no D spelling), one plural chosen per noun as that script describes. Its notice follows, verbatim.

"""


_NGRAM_NOTICE = """
provisa/core/data/plural_frequencies.tsv
----------------------------------------
The counts of the tied noun plurals (REQ-194) are derived from the Google Books Ngram Viewer
English 1-grams, version 20200217 (Google Books Ngram Viewer, http://books.google.com/ngrams), used
under the Creative Commons Attribution 3.0 Unported License
(https://creativecommons.org/licenses/by/3.0/). Changes: scripts/gen_plural_frequencies.py keeps
only the noun-tagged 1-grams (word_NOUN) of the competing plural forms and sums each one's match
count over 2000-2019; only those counts are bundled, not the corpus.
"""


def main(argv: list[str]) -> int:
    scowl_pre, copyright_file = Path(argv[0]), Path(argv[1])
    frequencies = {
        word: int(count)
        for word, count in (
            line.split("\t") for line in _FREQUENCIES.read_text(encoding="utf-8").splitlines()
        )
    }
    rows, own_words, reverse = extract(scowl_pre, frequencies)
    _OUT.mkdir(parents=True, exist_ok=True)
    # The singular of a plural several nouns list first, so it is the one singularizing finds; own
    # words after every pair, so a noun that is also a lemma (opera -> operas) keeps its plural.
    text = (
        "".join(f"{s}\t{p}\tmain\n" for s, p in reverse)
        + "".join(f"{s}\t{p}\n" for s, p in rows)
        + "".join(f"{w}\t{w}\town\n" for w in own_words)
    )
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
