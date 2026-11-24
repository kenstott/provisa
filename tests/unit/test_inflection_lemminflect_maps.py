# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1e8a90-3b7d-4f26-9e14-a2d6f0b3c871
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-194: inflection's compact noun maps give exactly lemminflect's dictionary answers.

provisa.core.inflection reads lemminflect's lookup files once and keeps only the noun answers
(a few MB) instead of calling lemminflect's functions, which hold every part of speech of every
word for the life of the process (about 40 MB of the boot peak). Every lower-case word lemminflect
lists is asked of both, and every answer must be the same.
"""

# Requirements: REQ-194

import pytest
from lemminflect import getAllInflections, getAllLemmas
from lemminflect.core.Inflections import Inflections
from lemminflect.core.Lemmatizer import Lemmatizer

from provisa.core import inflection


def _lower_keys(*tables: dict) -> list[str]:
    return sorted({w for t in tables for w in t if w == w.lower()})


def test_every_lemma_lemminflect_lists_is_the_map_s():
    lemmatizer = Lemmatizer()
    words = _lower_keys(lemmatizer._getLemmaDict(), lemmatizer._getOverridesDict())
    lemma_of, _ = inflection._lemminflect_nouns()
    assert len(words) > 30000
    for word in words:
        expected = getAllLemmas(word, upos="NOUN")
        assert lemma_of.get(word) == (expected["NOUN"][0] if expected else None), word
    assert set(lemma_of) <= set(words)


def test_every_plural_lemminflect_lists_is_the_map_s():
    inflections = Inflections()
    lemmas = _lower_keys(inflections._getInflDict(), inflections._getOverridesDict())
    _, plural_form = inflection._lemminflect_nouns()
    assert len(lemmas) > 30000
    for lemma in lemmas:
        expected = getAllInflections(lemma, upos="NOUN")
        if not expected:
            assert lemma not in plural_form, lemma
        else:
            assert plural_form[lemma] == (expected["NNS"][0] if "NNS" in expected else None), lemma
    assert set(plural_form) <= set(lemmas)


@pytest.mark.parametrize(
    ("word", "is_plural", "singular", "plural"),
    [
        # Not in the supplement or the dictionary table: lemminflect's answers.
        ("aeroplanes", True, "aeroplane", "aeroplanes"),
        ("aeroplane", False, "aeroplane", "aeroplanes"),
        # In no dictionary at all: as written.
        ("qzxv", False, "qzxv", "qzxv"),
    ],
)
def test_words_past_the_table_take_lemminflect_s_answer(word, is_plural, singular, plural):
    expected_lemmas = getAllLemmas(word, upos="NOUN")
    assert inflection.is_plural(word) is is_plural
    assert inflection.singular(word) == singular
    assert inflection.plural(word) == plural
    if expected_lemmas:
        assert inflection._lemminflect_nouns()[0][word] == expected_lemmas["NOUN"][0]
