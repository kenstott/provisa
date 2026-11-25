# Copyright (c) 2026 Kenneth Stott
# Canary: 9f012000-fd79-4ba6-b8ab-626cb9167b9c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1874 (amended 2026-11-24): Stripe and Shopify are brands, not source types.

The names belong to the brands: Stripe is carried by the OpenAPI source and Shopify by the remote
GraphQL source (REQ-1923). No SourceType member is named for either.
"""

import pytest

from provisa.core.models import SourceType
from provisa.graphql_remote.brands import BRANDS as GRAPHQL_BRANDS
from provisa.openapi.brands import BRANDS as OPENAPI_BRANDS


@pytest.mark.parametrize("name", ["stripe", "shopify"])
def test_brand_name_is_not_a_source_type(name: str) -> None:
    assert name not in SourceType.__members__
    assert name not in {t.value for t in SourceType}


def test_stripe_is_a_brand_of_the_openapi_source() -> None:
    assert "stripe" in OPENAPI_BRANDS
    assert "stripe" not in GRAPHQL_BRANDS
    assert SourceType.openapi.value == "openapi"


def test_shopify_is_a_brand_of_the_remote_graphql_source() -> None:
    assert "shopify" in GRAPHQL_BRANDS
    assert "shopify" not in OPENAPI_BRANDS
    assert SourceType.graphql_remote.value == "graphql_remote"
