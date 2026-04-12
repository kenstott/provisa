# Copyright (c) 2026 Kenneth Stott
# Canary: c99060fb-e956-4c8e-b1ea-8322ca3ddc5b
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for _derive_graphql_alias in admin schema."""

import pytest

from provisa.api.admin.schema import _derive_graphql_alias


@pytest.mark.parametrize("target,cardinality,alias,expected", [
    # one-to-many: pluralise
    ("review", "one-to-many", None, "reviews"),
    ("order", "one-to-many", None, "orders"),
    ("line_item", "one-to-many", None, "lineItems"),
    ("category", "one-to-many", None, "categories"),   # y → ies
    ("address", "one-to-many", None, "addresses"),      # s → es
    ("box", "one-to-many", None, "boxes"),              # x → es
    ("match", "one-to-many", None, "matches"),          # ch → es
    # many-to-one: singular
    ("customer", "many-to-one", None, "customer"),
    ("order", "many-to-one", None, "order"),
    ("ProductCategory", "many-to-one", None, "productcategory"),  # no split on PascalCase without _
    ("product_category", "many-to-one", None, "productCategory"),
    # empty target
    ("", "one-to-many", None, None),
])
def test_derive_graphql_alias(target, cardinality, alias, expected):
    assert _derive_graphql_alias(target, cardinality, alias) == expected
