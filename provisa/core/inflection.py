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
not list (an abbreviation, a coined table name) takes lemminflect's rule-based out-of-vocabulary
form. This is the one inflection helper every naming path uses (relationship fields, aliases,
glossary terms).
"""

# Requirements: REQ-194

import functools
import gzip
from pathlib import Path

from lemminflect import getAllInflectionsOOV, getAllLemmasOOV

_TABLE = Path(__file__).parent / "data" / "noun_plurals.tsv.gz"


@functools.cache
def _dictionary() -> tuple[dict[str, str], dict[str, str], frozenset[str]]:
    """(singular -> its plural, plural -> its singular, words of their own). The table lists each
    noun's chosen plural first, then its other plurals; a noun listed with itself as plural has no
    other form; a row marked ``own`` (after every pair) is a word that is its own singular, whatever
    else it is the plural of."""
    plural_of: dict[str, str] = {}
    singular_of: dict[str, str] = {}
    own: set[str] = set()
    with gzip.open(_TABLE, "rt", encoding="utf-8") as f:
        for line in f:
            singular_form, plural_form, *mark = line.rstrip("\n").split("\t")
            plural_of.setdefault(singular_form, plural_form)
            if mark == ["own"]:
                own.add(singular_form)
            elif plural_form != singular_form:
                singular_of.setdefault(plural_form, singular_form)
    return plural_of, singular_of, frozenset(own)


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
    return getAllLemmasOOV(lower, upos="NOUN")["NOUN"][0] != lower


def singular(word: str) -> str:
    """``word``'s singular: the dictionary's for a word it lists, else the rule-based form."""
    lower = word.lower()
    plural_of, singular_of, own = _dictionary()
    if lower in own:
        return word
    if lower in singular_of:
        return _match_case(singular_of[lower], word)
    if lower in plural_of:
        return word
    return _match_case(getAllLemmasOOV(lower, upos="NOUN")["NOUN"][0], word)


def rule_plural(word: str) -> str:
    """``word``'s rule-based plural (lower case): the form a word the dictionary does not list takes,
    and the regularly formed plural among a listed noun's accepted forms (scripts/gen_noun_plurals.py)."""
    return getAllInflectionsOOV(word.lower(), upos="NOUN")["NNS"][0]


def plural(word: str) -> str:
    """``word``'s plural: as written when it is already one, the dictionary's for a noun it
    lists, else the rule-based form."""
    if is_plural(word):
        return word
    lower = word.lower()
    plural_of, _, _ = _dictionary()
    if lower in plural_of:
        return _match_case(plural_of[lower], word)
    return _match_case(rule_plural(lower), word)
