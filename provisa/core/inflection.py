# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3f6b2e-41a7-4c9e-b5d0-7e2c9a1f6034
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Singular and plural noun forms by English dictionary lookup (REQ-194).

Every form comes from lemminflect's lexicon: a word with a noun entry takes its dictionary
singular or plural; a word with none (an abbreviation, a coined table name) is returned as
written. No suffix rule guesses a form -- the lexicon-only calls (``getAllLemmas`` and
``getAllInflections``) are used, never the out-of-vocabulary rule models. This is the one
inflection helper every naming path uses (relationship fields, aliases, glossary terms).
"""

# Requirements: REQ-194

from lemminflect import getAllInflections, getAllLemmas


def _match_case(form: str, written: str) -> str:
    """``form`` (lower case, from the lexicon) in the case ``written`` was given in."""
    if written.isupper() and len(written) > 1:
        return form.upper()
    if written[:1].isupper():
        return form[:1].upper() + form[1:]
    return form


def _lemmas(word: str) -> tuple[str, ...]:
    """The lexicon's noun lemmas of ``word``, dictionary-first; empty when it has no entry."""
    return getAllLemmas(word.lower(), upos="NOUN").get("NOUN", ())


def is_plural(word: str) -> bool:
    """True when the dictionary lists ``word`` as the plural of a different noun."""
    lemmas = _lemmas(word)
    return bool(lemmas) and lemmas[0] != word.lower()


def singular(word: str) -> str:
    """``word``'s dictionary singular; ``word`` as written when the lexicon has no noun entry."""
    lemmas = _lemmas(word)
    if not lemmas:
        return word
    return _match_case(lemmas[0], word)


def plural(word: str) -> str:
    """``word``'s dictionary plural; ``word`` as written when it is already a plural or the
    lexicon has no noun entry for it.

    The lexicon lists a noun used both as a mass noun and as a count noun with its mass form
    first (``mapping``: mapping, mappings); the first listed form that differs from ``word`` is
    its count plural. A noun whose only listed plural is itself (``data``, ``sheep``) keeps it."""
    if is_plural(word):
        return word
    lower = word.lower()
    forms = getAllInflections(lower, upos="NOUN").get("NNS", ())
    if not forms:
        return word
    counted = [f for f in forms if f != lower]
    return _match_case(counted[0] if counted else forms[0], word)
