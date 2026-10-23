# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An API endpoint's side of the source contract (REQ-1756): a REST, GraphQL API, gRPC API,
SPARQL or Neo4j endpoint. v1: it declares no capability, so every read of it lands (connector
contract's ``Lands``); its call is the one remote call every path makes of an endpoint
(``caller.call_api``), given the endpoint and its source as the read's registration."""

# Requirements: REQ-1756, REQ-863
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from provisa.federation.connector_contract import Lands, Plan, Read


@dataclass(frozen=True)
class EndpointCall:
    """What an endpoint's call is made with besides its arguments."""

    endpoint: Any  # ApiEndpoint
    source: Any  # ApiSource: its address, credential and headers


class ApiEndpointContract(Lands):
    async def call(self, state: Any, plan: Plan) -> Any:
        """The endpoint's answer to the plan's arguments (``caller.ApiAnswer``)."""
        from provisa.api_source.caller import call_api

        registration: EndpointCall = plan.read.registration
        source = registration.source
        return await call_api(
            registration.endpoint,
            plan.arguments,
            base_url=source.base_url,
            auth=source.auth,
            source_headers=source.headers,
        )


async def call_endpoint(endpoint: Any, source: Any, params: dict) -> Any:
    """``endpoint``'s answer to ``params``, through its source type's contract."""
    from provisa.federation.connector_contract import contract_for

    contract = contract_for(source.type.value)
    assert contract is not None  # every one of these types has its contract (_CONTRACTS)
    read = Read(
        table={"source_id": endpoint.source_id, "table_name": endpoint.table_name},
        name=endpoint.table_name,
        statement="",
        source_id=endpoint.source_id,
        arguments=params,
        registration=EndpointCall(endpoint, source),
    )
    return await contract.call(None, await contract.plan(None, read))
