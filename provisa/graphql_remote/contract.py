# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A remote GraphQL source's side of the source contract (REQ-1756). v1: it declares no
capability, so every read of it lands (connector contract's ``Lands``); its call is the remote
field's query (``executor.execute_remote``), the read's registration naming the field, its
selection and its limits, and the plan's arguments its variables."""

# Requirements: REQ-1756, REQ-863
from __future__ import annotations

from typing import Any

from provisa.federation.connector_contract import Lands, Plan


class GraphqlRemoteContract(Lands):
    async def call(self, state: Any, plan: Plan) -> Any:
        """The remote field's answer (``executor.RemoteAnswer``). The registration is
        ``execute_remote``'s request: every argument of it but its variables."""
        from provisa.graphql_remote import executor

        return await executor.execute_remote(
            **plan.read.registration, variables=plan.arguments or None
        )


async def call_field(source_id: str, table_name: str, request: dict, variables: dict | None) -> Any:
    """The remote field's answer to ``variables``, through the contract: ``request`` is
    ``execute_remote``'s every argument but its variables."""
    from provisa.federation.connector_contract import Read, contract_for

    contract = contract_for("graphql_remote")
    assert contract is not None  # every one of these types has its contract (_CONTRACTS)
    read = Read(
        table={"source_id": source_id, "table_name": table_name},
        name=table_name,
        statement="",
        source_id=source_id,
        arguments=variables,
        registration=request,
    )
    return await contract.call(None, await contract.plan(None, read))
