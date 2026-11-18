# Copyright (c) 2026 Kenneth Stott
# Canary: 2b9e7c41-6f3a-4d58-a0c2-9e1d5b8f7a63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-194 (amended 2026-11-18): singular and plural come from a general-English dictionary first.

Every naming path -- relationship fields, OpenAPI aliases, Cypher types, glossary concepts --
takes the dictionary form of a noun the dictionary lists, never a rule form; only a word it does
not list takes the rule-based form.
"""

import pytest

from provisa.compiler import naming
from provisa.core import glossary, relationship_names

# (singular, plural) pairs the dictionary (provisa/core/data/noun_plurals.tsv.gz) lists.
PAIRS = [
    ("person", "people"),
    ("doc", "docs"),
    ("dataset", "datasets"),
    ("loss", "losses"),
    ("address", "addresses"),
    ("access", "accesses"),
    ("class", "classes"),
    ("analysis", "analyses"),
    ("glass", "glasses"),
    ("inquiry", "inquiries"),
    ("order", "orders"),
    ("company", "companies"),
    ("mapping", "mappings"),
    ("embedding", "embeddings"),
]


@pytest.mark.parametrize(("one", "many"), PAIRS)
def test_one_to_many_field_takes_dictionary_plural(one, many):
    assert naming._rel_words(one, "one-to-many") == many
    assert naming._rel_words(many, "one-to-many") == many


@pytest.mark.parametrize(("one", "many"), PAIRS)
def test_many_to_one_field_takes_dictionary_singular(one, many):
    assert naming._rel_words(many, "many-to-one") == one
    assert naming._rel_words(one, "many-to-one") == one


@pytest.mark.parametrize(("one", "many"), PAIRS)
def test_head_noun_of_compound_inflected(one, many):
    assert naming._rel_words(f"customer_{one}", "one-to-many") == f"customer_{many}"
    assert naming._rel_words(f"customer_{many}", "many-to-one") == f"customer_{one}"


@pytest.mark.parametrize(("one", "many"), PAIRS)
def test_cypher_type_takes_dictionary_singular(one, many):
    assert relationship_names.derive_cypher_alias(f"{many}_id", "many-to-one") == (
        f"BELONGS_TO_{one.upper()}"
    )


@pytest.mark.parametrize(("one", "many"), PAIRS)
def test_glossary_concept_takes_dictionary_singular(one, many):
    assert glossary.table_concept(many) == one
    assert glossary.table_concept(one) == one


def test_plural_only_dictionary_word_unchanged_in_plural():
    assert naming._rel_words("data", "one-to-many") == "data"
    assert naming._rel_words("customer_data", "one-to-many") == "customer_data"


@pytest.mark.parametrize("word", ["series", "news", "sheep"])
def test_dictionary_noun_without_other_plural_unchanged(word):
    # Listed with no plural but itself: never the rule form ("serieses").
    assert naming._rel_words(word, "one-to-many") == word
    assert naming._rel_words(word, "many-to-one") == word


@pytest.mark.parametrize(("one", "many"), [("fct_ordr", "fct_ordrs"), ("lineitm", "lineitms")])
def test_word_outside_dictionary_takes_rule_form(one, many):
    assert naming._rel_words(one, "one-to-many") == many
    assert naming._rel_words(many, "one-to-many") == many
    assert naming._rel_words(many, "many-to-one") == one
    assert naming._rel_words(one, "many-to-one") == one


def test_out_of_dictionary_cypher_and_glossary_take_rule_singular():
    assert relationship_names.derive_cypher_alias("ordrs_id", "many-to-one") == "BELONGS_TO_ORDR"
    assert glossary.table_concept("fct_ordrs") == "fct ordr"


def test_helper_preserves_case():
    from provisa.core import inflection

    assert inflection.plural("Address") == "Addresses"
    assert inflection.singular("LOSSES") == "LOSS"
    assert inflection.plural("Ordr") == "Ordrs"
