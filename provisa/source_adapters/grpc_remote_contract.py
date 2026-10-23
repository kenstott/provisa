# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A remote gRPC source's side of the source contract (REQ-1756). v1: it declares no
capability, so every read of it lands (connector contract's ``Lands``); its call is the query
method (``grpc_remote_adapter.fetch``), the read's registration naming the method, and the
plan's arguments its request message's fields."""

# Requirements: REQ-1756, REQ-863
from __future__ import annotations

from typing import Any

from provisa.federation.connector_contract import Lands, Plan


class GrpcRemoteContract(Lands):
    async def call(self, state: Any, plan: Plan) -> Any:
        """The method's rows. The registration is ``grpc_remote_adapter.fetch``'s request:
        every argument of it but ``args``."""
        from provisa.source_adapters import grpc_remote_adapter

        return await grpc_remote_adapter.fetch(**plan.read.registration, args=plan.arguments)


async def call_method(source_id: str, table_name: str, request: dict, args: dict) -> Any:
    """The query method's rows for ``args``, through the contract: ``request`` is
    ``grpc_remote_adapter.fetch``'s every argument but ``args``."""
    from provisa.federation.connector_contract import Read, contract_for

    contract = contract_for("grpc_remote")
    assert contract is not None  # every one of these types has its contract (_CONTRACTS)
    read = Read(
        table={"source_id": source_id, "table_name": table_name},
        name=table_name,
        statement="",
        source_id=source_id,
        arguments=args,
        registration={"source_id": source_id, **request},
    )
    return await contract.call(None, await contract.plan(None, read))
