# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1d8e72-3b4f-4a90-9d6e-2f7a8b0c4e15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-194 (amended 2026-11-20): the recorded supplement word list is pinned, both directions."""

import pytest

from provisa.core import inflection

# Every pair the requirement names or the supplement records.
SUPPLEMENT_PAIRS = [
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
]

# Pairs the requirement text lists by name.
REQUIRED_PAIRS = [("person", "people"), ("doc", "docs"), ("dataset", "datasets")]


def test_supplement_list_is_exactly_pinned():
    assert list(inflection._SUPPLEMENT) == SUPPLEMENT_PAIRS


@pytest.mark.parametrize("pair", REQUIRED_PAIRS)
def test_required_pairs_present(pair):
    assert pair in inflection._SUPPLEMENT


@pytest.mark.parametrize(("one", "many"), SUPPLEMENT_PAIRS)
def test_plural_of_singular(one, many):
    assert inflection.plural(one) == many


@pytest.mark.parametrize(("one", "many"), SUPPLEMENT_PAIRS)
def test_singular_of_plural(one, many):
    assert inflection.singular(many) == one


@pytest.mark.parametrize("many", [m for _, m in SUPPLEMENT_PAIRS])
def test_plural_of_plural_unchanged(many):
    assert inflection.plural(many) == many


@pytest.mark.parametrize("one", [o for o, _ in SUPPLEMENT_PAIRS])
def test_singular_of_singular_unchanged(one):
    assert inflection.singular(one) == one


@pytest.mark.parametrize(("one", "many"), SUPPLEMENT_PAIRS)
def test_is_plural_flags(one, many):
    assert inflection.is_plural(many) is True
    assert inflection.is_plural(one) is False


@pytest.mark.parametrize(("one", "many"), SUPPLEMENT_PAIRS)
def test_case_is_kept(one, many):
    assert inflection.plural(one.capitalize()) == many.capitalize()
    assert inflection.singular(many.upper()) == one.upper()


@pytest.mark.parametrize("word", ["zorbex", "Zorbex", "ZORBEX", "qwrtz"])
def test_unknown_word_plural_as_written(word):
    assert inflection.plural(word) == word


@pytest.mark.parametrize("word", ["zorbexes", "Zorbexes", "qwrtzs"])
def test_unknown_word_singular_as_written(word):
    assert inflection.singular(word) == word


def test_unknown_word_is_not_plural():
    assert not inflection.is_plural("zorbexes")


def test_dictionary_word_case_preserved():
    assert inflection.plural("Cat") == "Cats"
    assert inflection.singular("CATS") == "CAT"
