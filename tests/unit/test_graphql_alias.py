# Copyright (c) 2026 Kenneth Stott
# Canary: c99060fb-e956-4c8e-b1ea-8322ca3ddc5b
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for derive_graphql_alias, the one derivation of a relationship's GraphQL field (REQ-351)."""

import pytest

from provisa.core.relationship_names import derive_graphql_alias


@pytest.mark.parametrize(
    "target,cardinality,expected",
    [
        # one-to-many: pluralise
        ("review", "one-to-many", "reviews"),
        ("order", "one-to-many", "orders"),
        ("line_item", "one-to-many", "lineItems"),
        ("category", "one-to-many", "categories"),  # y → ies
        ("address", "one-to-many", "addresses"),  # s → es
        ("box", "one-to-many", "boxes"),  # x → es
        ("match", "one-to-many", "matches"),  # ch → es
        # many-to-one: singular
        ("customer", "many-to-one", "customer"),
        ("order", "many-to-one", "order"),
        (
            "ProductCategory",
            "many-to-one",
            "productCategory",
        ),  # PascalCase normalized to snake then compound-noun rule
        ("product_category", "many-to-one", "productCategory"),
        # empty target
        ("", "one-to-many", None),
    ],
)
def test_derive_graphql_alias(target, cardinality, expected):
    assert derive_graphql_alias(target, cardinality) == expected
