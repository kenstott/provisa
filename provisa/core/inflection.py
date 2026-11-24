# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3f6b2e-41a7-4c9e-b5d0-7e2c9a1f6034
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Singular and plural noun forms, general-English dictionary first (REQ-194).

The dictionary is ``data/noun_plurals.tsv.gz``: English noun singular/plural pairs extracted from
the English Speller Database (scripts/gen_noun_plurals.py; notice in ``data/THIRD_PARTY_NOTICES``).
A noun in it takes only its dictionary forms; a form the table marks as a word of its own
(``data``) is its own singular and plural, though it is another noun's plural too; a word it does
not list is looked up in lemminflect's dictionary; a word in neither the supplement, the table nor
lemminflect (an abbreviation, a coined table name) stays as written in both directions. This is the one inflection helper every naming path uses (relationship fields, aliases,
glossary terms).
"""

# Requirements: REQ-194

import functools
import gzip
from collections.abc import Iterator
from pathlib import Path

from lemminflect import config as _lemminflect_config
from lemminflect import getAllInflectionsOOV

_TABLE = Path(__file__).parent / "data" / "noun_plurals.tsv.gz"

# Recorded supplement (REQ-194): words the dictionary and lemminflect inflect wrongly or lack
# (coined table-name nouns, abbreviations). Consulted before the dictionary.
_SUPPLEMENT: tuple[tuple[str, str], ...] = (
    ("person", "people"),
    ("doc", "docs"),
    ("dataset", "datasets"),
    ("timestamp", "timestamps"),
    ("webhook", "webhooks"),
    ("endpoint", "endpoints"),
    ("username", "usernames"),
    ("signup", "signups"),
    ("lineitem", "lineitems"),
    ("littermate", "littermates"),
    ("sku", "skus"),
    ("kpi", "kpis"),
)


@functools.cache
def _dictionary() -> tuple[dict[str, str], dict[str, str], frozenset[str]]:
    """(singular -> its plural, plural -> its singular, words of their own). A row marked ``main``
    (before every pair) names the singular of a plural more than one noun lists; then the table lists
    each noun's chosen plural first, then its other plurals; a noun listed with itself as plural has
    no other form; a row marked ``own`` (after every pair) is a word that is its own singular,
    whatever else it is the plural of."""
    plural_of: dict[str, str] = {}
    singular_of: dict[str, str] = {}
    own: set[str] = set()
    for singular_form, plural_form in _SUPPLEMENT:
        plural_of[singular_form] = plural_form
        singular_of[plural_form] = singular_form
        own.discard(singular_form)
    with gzip.open(_TABLE, "rt", encoding="utf-8") as f:
        for line in f:
            singular_form, plural_form, *mark = line.rstrip("\n").split("\t")
            if mark == ["main"]:
                singular_of.setdefault(plural_form, singular_form)
                continue
            plural_of.setdefault(singular_form, plural_form)
            if mark == ["own"]:
                own.add(singular_form)
            elif plural_form != singular_form:
                singular_of.setdefault(plural_form, singular_form)
    return plural_of, singular_of, frozenset(own)


_NOUN_TAGS = ("NN", "NNS", "NNP", "NNPS")  # lemminflect's tags of upos NOUN (uposToTags)


# The words InflectionLUCodec.updateForAuxMod replaces wholesale with verb-only forms once the
# inflection table is read, so none of them keeps a noun form from the table.
_AUX_MODAL_REPLACED = frozenset({"can", "may", "will", "shall", "must", "ought", "dare", "be"})


def _overrides(fn: str) -> Iterator[tuple[str, str, str]]:
    """(word, pos, form) per entry of a lemminflect overrides file, as OverridesCodec reads it."""
    with open(fn, encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line[0] == "#":
                continue
            word, pos, form = line.split(",")
            yield word, pos, form


def _lemma_map() -> dict[str, str]:
    """Lower-case word -> the first NOUN lemma ``getAllLemmas(word, upos="NOUN")`` gives.

    Streams lemminflect's lemma table row by row as LemmaLUCodec parses it (word, category,
    spellings; the category upper-cased is the upos), a later row of a word and upos replacing an
    earlier one, then applies its overrides, keeping only NOUN rows of lower-case words."""
    lemma_of: dict[str, str] = {}
    with gzip.open(_lemminflect_config.lemma_lu_fn, "rt", encoding="utf-8") as f:
        for raw in f:
            word, category, forms = raw.strip().split(",")
            if category.upper() == "NOUN" and word == word.lower():
                lemma_of[word] = forms.split("/", 1)[0].lower()
    for word, pos, form in _overrides(_lemminflect_config.lemma_overrides_fn):
        if pos == "NOUN" and word == word.lower():
            lemma_of[word] = form.lower()
    return lemma_of


def _plural_map() -> dict[str, str | None]:
    """Lower-case lemma -> the first NNS form ``getAllInflections(lemma, upos="NOUN")`` gives, or
    None for a lemma lemminflect lists with a noun tag but no NNS form.

    Streams lemminflect's inflection table row by row as InflectionLUCodec parses it: a ``noun``
    row gives its word the NN tag and, when its plural field is not empty, NNS spellings that
    replace an earlier row's; the aux and modal words are then replaced by verb-only forms; then
    its overrides apply. Only lower-case lemmas with a noun tag are kept."""
    plural_form: dict[str, str | None] = {}
    with gzip.open(_lemminflect_config.inflection_lu_fn, "rt", encoding="utf-8") as f:
        for raw in f:
            word, category, *forms = raw.strip().split(",")
            if category != "noun" or word != word.lower():
                continue
            if forms and forms[0]:
                plural_form[word] = forms[0].split("/", 1)[0].lower()
            else:
                plural_form.setdefault(word, None)
    for word in _AUX_MODAL_REPLACED:
        plural_form.pop(word, None)
    for word, pos, form in _overrides(_lemminflect_config.infl_overrides_fn):
        if pos not in _NOUN_TAGS or word != word.lower():
            continue
        if pos == "NNS":
            plural_form[word] = form.lower()
        else:
            plural_form.setdefault(word, None)
    return plural_form


@functools.cache
def _lemminflect_nouns() -> tuple[dict[str, str], dict[str, str | None]]:
    """lemminflect's dictionary answers for nouns, as two compact maps: (lower-case word -> the
    first NOUN lemma ``getAllLemmas(word, upos="NOUN")`` gives, lower-case lemma -> the first NNS
    form ``getAllInflections(lemma, upos="NOUN")`` gives, or None for a lemma it lists with no NNS
    form). A word absent from a map is one lemminflect answers ``{}`` for.

    Streamed row by row from lemminflect's own lookup files, keeping only the noun answers for
    lower-case words, the only ones the functions below ask. lemminflect's functions and codecs
    both build every part of speech of every word in memory (a 42 MB transient at boot whose
    pages the allocator keeps); these maps hold a few MB.
    tests/unit/test_inflection_lemminflect_maps.py checks every answer against lemminflect's."""
    return _lemma_map(), _plural_map()


def _match_case(form: str, written: str) -> str:
    """``form`` (lower case) in the case ``written`` was given in."""
    if written.isupper() and len(written) > 1:
        return form.upper()
    if written[:1].isupper():
        return form[:1].upper() + form[1:]
    return form


def is_plural(word: str) -> bool:
    """True when ``word`` is the plural of a different noun."""
    lower = word.lower()
    plural_of, singular_of, own = _dictionary()
    if lower in own:
        return False
    if lower in singular_of:
        return True
    if lower in plural_of:
        return False
    lemma = _lemminflect_nouns()[0].get(lower)
    return lemma is not None and lemma != lower


def singular(word: str) -> str:
    """``word``'s singular: the dictionary's (then lemminflect's) for a word listed there, else as written."""
    lower = word.lower()
    plural_of, singular_of, own = _dictionary()
    if lower in own:
        return word
    if lower in singular_of:
        return _match_case(singular_of[lower], word)
    if lower in plural_of:
        return word
    lemma = _lemminflect_nouns()[0].get(lower)
    if lemma is None:
        return word
    return _match_case(lemma, word)


def regular_plural(word: str) -> str:
    """The rule-based plural (lower case) lemminflect forms for any word, listed or not. For
    scripts/gen_noun_plurals.py only: it names the regularly formed plural among a listed noun's
    accepted forms. No naming path calls it."""
    return getAllInflectionsOOV(word.lower(), upos="NOUN")["NNS"][0]


def rule_plural(word: str) -> str:
    """``word``'s plural per lemminflect's dictionary (lower case); as written when lemminflect does
    not list it."""
    lower = word.lower()
    plural_forms = _lemminflect_nouns()[1]
    if lower not in plural_forms:
        return lower
    form = plural_forms[lower]
    if form is None:
        # lemminflect lists the noun with no plural form: getAllInflections(...)["NNS"] raised.
        raise KeyError("NNS")
    return form


def plural(word: str) -> str:
    """``word``'s plural: as written when it is already one, the dictionary's for a noun it
    lists, else lemminflect's, else as written."""
    if is_plural(word):
        return word
    lower = word.lower()
    plural_of, _, _ = _dictionary()
    if lower in plural_of:
        return _match_case(plural_of[lower], word)
    return _match_case(rule_plural(lower), word)
