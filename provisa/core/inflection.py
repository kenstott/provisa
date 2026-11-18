# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3f6b2e-41a7-4c9e-b5d0-7e2c9a1f6034
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Singular and plural noun forms, dictionary first (REQ-194).

A word the English dictionary (lemminflect's lexicon) knows takes only its dictionary forms: a
noun its dictionary singular or plural, a word known only as another part of speech (``by``,
``placed``) is left as written. A word the dictionary does not know (an abbreviation, a coined
table name) takes lemminflect's rule-based out-of-vocabulary form. This is the one inflection
helper every naming path uses (relationship fields, aliases, glossary terms).
"""

# Requirements: REQ-194

from lemminflect import getAllInflections, getAllInflectionsOOV, getAllLemmas, getAllLemmasOOV


def _match_case(form: str, written: str) -> str:
    """``form`` (lower case, from the lexicon) in the case ``written`` was given in."""
    if written.isupper() and len(written) > 1:
        return form.upper()
    if written[:1].isupper():
        return form[:1].upper() + form[1:]
    return form


def _noun_lemmas(lower: str) -> tuple[str, ...] | None:
    """``lower``'s noun lemmas, dictionary-first; the rule-based lemma when the dictionary does
    not know the word; None when the dictionary knows it only as another part of speech."""
    known = getAllLemmas(lower)
    if "NOUN" in known:
        return known["NOUN"]
    if known:
        return None
    return getAllLemmasOOV(lower, upos="NOUN")["NOUN"]


def is_plural(word: str) -> bool:
    """True when ``word`` is the plural of a different noun."""
    lemmas = _noun_lemmas(word.lower())
    return lemmas is not None and lemmas[0] != word.lower()


def singular(word: str) -> str:
    """``word``'s singular; ``word`` as written when the dictionary knows it as no noun."""
    lemmas = _noun_lemmas(word.lower())
    if lemmas is None:
        return word
    return _match_case(lemmas[0], word)


def plural(word: str) -> str:
    """``word``'s plural; ``word`` as written when it is already a plural or the dictionary
    knows it as no noun.

    The lexicon lists a noun used both as a mass noun and as a count noun with its mass form
    first (``mapping``: mapping, mappings); the first listed form that differs from ``word`` is
    its count plural. A noun whose only listed plural is itself (``data``, ``sheep``) keeps it."""
    lower = word.lower()
    lemmas = _noun_lemmas(lower)
    if lemmas is None or lemmas[0] != lower:
        return word
    if getAllLemmas(lower):
        # A known noun the dictionary lists no plural for (``behalf``, ``dearth``) has none: it
        # stays as written, never a rule form (REQ-194).
        forms = getAllInflections(lower, upos="NOUN").get("NNS", (lower,))
    else:
        forms = getAllInflectionsOOV(lower, upos="NOUN")["NNS"]
    counted = [f for f in forms if f != lower]
    return _match_case(counted[0] if counted else forms[0], word)
