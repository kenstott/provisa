# Copyright (c) 2026 Kenneth Stott
# Canary: 9d41c6e2-3a57-4f08-b6c9-0e2a7d5b8f31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An NL-generated JSON:API row request always asks for the page total (REQ-1197, REQ-257).

JSON:API counts a list's total only when the request carries ``page[total]=true``. A request the
NL layer writes on a visitor's behalf always carries it, so the answer says how many rows match.
Aggregate and group-by requests return no page of resource rows and carry no page parameters.
"""

# Requirements: REQ-1197, REQ-257, REQ-1359

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.nl import executor
from provisa.nl.runner import AggregationPlan, _generate_jsonapi_query

_META = SimpleNamespace(domain_id="pet_store", table_name="pets")


def test_a_table_request_asks_for_the_page_total():
    node = SimpleNamespace(type_name="DimPet", domain_id="pet_store", table_name="dim_pet")
    query, error = _generate_jsonapi_query(None, {"DimPet"}, {"DimPet": node})
    assert error is None
    assert query == "/data/jsonapi/pet_store/dim_pet?page[size]=20&page[total]=true"


def test_a_plan_that_is_a_plain_row_fetch_asks_for_the_page_total():
    plan = AggregationPlan(_META, [], False, [], [])
    query, error = _generate_jsonapi_query(plan, set(), {})
    assert error is None
    assert query == "/data/jsonapi/pet_store/pets?page[size]=20&page[total]=true"


@pytest.mark.parametrize(
    "plan",
    [
        AggregationPlan(_META, [], True, ["count"], []),
        AggregationPlan(_META, ["species"], False, ["count"], []),
    ],
    ids=["aggregate", "group-by"],
)
def test_aggregate_and_group_by_requests_carry_no_page_parameters(plan):
    query, error = _generate_jsonapi_query(plan, set(), {})
    assert error is None
    assert query is not None and "page[" not in query


@pytest.mark.anyio
async def test_the_executor_runs_a_request_that_asks_for_the_total():
    with patch.object(executor, "_execute_domain_table", new=AsyncMock(return_value={})) as run:
        await executor._execute_jsonapi(
            "/data/jsonapi/pet_store/dim_pet?page[size]=20&page[total]=true", "admin", object()
        )
    assert run.await_args is not None
    assert run.await_args.args[:2] == ("pet_store", "dim_pet")
