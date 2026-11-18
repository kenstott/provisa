# Copyright (c) 2026 Kenneth Stott
# Canary: 2b9e7c41-6f3a-4d58-a0c2-9e1d5b8f7a63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-194 (amended 2026-11-18): singular and plural come from a dictionary lookup, never rules.

Every naming path -- relationship fields, OpenAPI aliases, Cypher types, glossary concepts --
takes the dictionary form; a word the dictionary lacks is left as written.
"""

import pytest

from provisa.compiler import naming
from provisa.core import glossary, relationship_names

# (singular, plural) pairs the dictionary lists.
PAIRS = [
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


def test_uncountable_dictionary_word_unchanged():
    assert naming._rel_words("data", "one-to-many") == "data"
    assert naming._rel_words("data", "many-to-one") == "data"
    assert glossary.table_concept("data") == "data"


@pytest.mark.parametrize("word", ["fct_ordr", "ordr", "lineitm", "by"])
def test_word_outside_dictionary_left_as_written(word):
    assert naming._rel_words(word, "one-to-many") == word
    assert naming._rel_words(word, "many-to-one") == word


def test_out_of_dictionary_cypher_and_glossary_unchanged():
    assert relationship_names.derive_cypher_alias("ordr_id", "many-to-one") == "BELONGS_TO_ORDR"
    assert glossary.table_concept("fct_ordr") == "fct ordr"


def test_helper_preserves_case():
    from provisa.core import inflection

    assert inflection.plural("Address") == "Addresses"
    assert inflection.singular("LOSSES") == "LOSS"
    assert inflection.singular("Fct_Ordr") == "Fct_Ordr"
