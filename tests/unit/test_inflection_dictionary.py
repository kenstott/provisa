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


# REQ-194 (amended 2026-11-18): a plural that more standard dictionaries list as a headword than
# its singular is an everyday word of its own -- its own singular and plural.
def test_everyday_plural_is_its_own_singular():
    assert naming._rel_words("data", "many-to-one") == "data"
    assert naming._rel_words("customer_data", "many-to-one") == "customer_data"
    assert glossary.table_concept("data") == "data"
    assert relationship_names.derive_cypher_alias("data_id", "many-to-one") == "BELONGS_TO_DATA"


def test_singular_of_everyday_plural_still_takes_it():
    assert naming._rel_words("datum", "one-to-many") == "data"


def test_everyday_plural_that_is_also_a_noun_keeps_its_own_plural():
    assert naming._rel_words("opera", "many-to-one") == "opera"
    assert naming._rel_words("opera", "one-to-many") == "operas"
    assert naming._rel_words("operas", "many-to-one") == "opera"


# REQ-194 (amended 2026-11-18): a word the dictionary knows only in an unrelated sense of the same
# spelling (by, read as the noun bye) is not inflected.
@pytest.mark.parametrize("rel_type", ["one-to-many", "many-to-one"])
def test_word_known_only_in_unrelated_sense_not_inflected(rel_type):
    assert naming._rel_words("by", rel_type) == "by"
    assert naming._rel_words("treated_by", rel_type) == "treated_by"


def test_unrelated_sense_leaves_the_other_noun_inflected():
    assert naming._rel_words("bye", "one-to-many") == "byes"
    assert naming._rel_words("byes", "many-to-one") == "bye"


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


# REQ-194 (amended 2026-11-18): where the dictionary lists more than one form with no ranking
# between them, the form more frequent in general English wins (schema: schemata, schemas).
def test_tie_between_dictionary_plurals_broken_by_frequency():
    assert naming._rel_words("schema", "one-to-many") == "schemas"
    assert naming._rel_words("schemas", "many-to-one") == "schema"
    assert naming._rel_words("schemata", "many-to-one") == "schema"
    assert naming._rel_words("customer_schema", "one-to-many") == "customer_schemas"


def test_frequency_keeps_a_single_dictionary_form():
    # One dictionary plural: frequency never applies (person keeps people, datum keeps data).
    assert naming._rel_words("person", "one-to-many") == "people"
    assert naming._rel_words("datum", "one-to-many") == "data"
