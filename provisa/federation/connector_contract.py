# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The source contract every connector Provisa calls itself provides, other than a SQL database's
(REQ-1756, REQ-863): its capabilities, the plan of a read and the call.

A plan is made before any call: the arguments the call is given and whether the rows it returns
are the read's complete dataset -- every condition, the ordering, the limit and every column
handled, with no row rule or mask of the role's on it. A complete dataset is the answer; any other
read's rows are landed and the engine runs the statement over them, the arguments a pre-filter
only. Routing's trusted_to_answer gate reads the plan and nothing else of the connector.

Provisa's own connectors implement the contract in their own code (Qdrant, the API caller's
sources, SPARQL). A code source's function cannot, so its proxy implements it on its behalf
(codesource.proxy). A SQL database is read through its driver, judged by the dialect capability
list, and a source the engine reads through its own connector is the engine's: neither has one.

v1: Qdrant reports a complete dataset for what it answers exactly (a similarity top-k, a
retrieve by id, a scroll); every other connector declares no capability and lands. The target
shape of a capability, for later: the comparators it evaluates exactly, per data type; the
ordering, by one key or several, in which directions; and the limit, as offset and length."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Read:
    """One read of a table by a statement: the governed statement, PostgreSQL text."""

    table: dict
    name: str  # the name the statement reads it by
    statement: str
    source_id: str = ""
    arguments: dict | None = None  # the arguments the statement names for the read itself


@dataclass(frozen=True)
class Plan:
    """What a connector's call is given for a read, and whether its rows are the read's answer."""

    read: Read
    arguments: dict = field(default_factory=dict)
    complete_dataset: bool = False
    incomplete_because: str | None = None  # named in the route's reason


class SourceContract(ABC):
    """A connector's side of the source contract."""

    @abstractmethod
    async def plan(self, state: Any, read: Read, ctx: Any = None) -> Plan:
        """The read's plan, before any call."""

    @abstractmethod
    async def call(self, state: Any, plan: Plan) -> Any:
        """The rows the plan's call returns."""


class Lands(SourceContract):
    """A connector that declares no capability (v1): every read is incomplete and lands, its call
    made by the landing path that has always made it (the API caller, the remote fetches)."""

    def __init__(self, kind: str) -> None:
        self.kind = kind

    async def plan(self, state: Any, read: Read, ctx: Any = None) -> Plan:
        return Plan(
            read=read,
            arguments=dict(read.arguments or {}),
            incomplete_because=f"a {self.kind} source declares no capability",
        )

    async def call(self, state: Any, plan: Plan) -> Any:
        raise RuntimeError(
            f"{plan.read.name}: a {self.kind} read is never its complete dataset; its rows are "
            "landed by the landing path"
        )


_CONTRACTS: dict[str, SourceContract] = {}


def contract_for(source_type: str) -> SourceContract | None:
    """``source_type``'s contract, or None for a SQL database or a source the engine reads."""
    if not _CONTRACTS:
        from provisa.codesource.proxy import CodeProxy
        from provisa.qdrant.contract import QdrantContract

        _CONTRACTS.update(
            {
                "node": CodeProxy(),
                "python": CodeProxy(),
                "qdrant": QdrantContract(),
                "openapi": Lands("REST"),
                "graphql_api": Lands("GraphQL remote"),
                "grpc_api": Lands("gRPC"),
                "grpc_remote": Lands("gRPC remote"),
                "sparql": Lands("SPARQL"),
            }
        )
    return _CONTRACTS.get(source_type)
