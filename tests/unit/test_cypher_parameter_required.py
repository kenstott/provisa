# Copyright (c) 2026 Kenneth Stott
# Canary: 27d72876-48c8-41ef-9a56-f41afb8afe95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1986: a Cypher read over HTTP that leaves a table's required parameter without a value is
refused as the caller's to put right (400), naming the table and the parameter -- not a 500."""

# Requirements: REQ-1986, REQ-345

from __future__ import annotations

import json
from unittest.mock import MagicMock

from tests.unit.test_cache_before_route import pipe  # noqa: F401  (the shared pipeline fixture)
from tests.unit.test_cypher_cache_before_route import graph  # noqa: F401


async def test_a_read_missing_a_required_parameter_is_a_400_naming_it(graph, monkeypatch):  # noqa: F811
    from provisa.api.rest.cypher_router import CypherRequest, cypher_query
    from provisa.compiler.nf_extractor import apply_dropped_tables
    from provisa.core.parameters import missing_reason
    from provisa.pgwire import _pipeline

    async def _govern(sql, role_id, **kw):
        # The pipeline's own refusal of the read (a parameterized table, no value given).
        apply_dropped_tables(
            "SELECT 1 FROM assignments_by_breed",
            {"assignments_by_breed": missing_reason(["breed_name"])},
        )

    monkeypatch.setattr(_pipeline, "_govern_and_route_compiled", _govern)
    request = MagicMock()
    request.state.role = "analyst"
    request.headers = {}
    body = CypherRequest(query="MATCH (o:Orders) RETURN o.id AS id", params={})
    response = await cypher_query(body, request, query_id=None, x_provisa_stats=None)

    assert response.status_code == 400, response.body
    error = json.loads(bytes(response.body))["error"]
    assert "assignments_by_breed" in error and "breed_name" in error
