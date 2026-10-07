# Copyright (c) 2026 Kenneth Stott
# Canary: f0c16aa9-3937-4bd6-9bf1-a9b79300dc3c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shared types for the Cypher→SQL translator: the graph-variable kind enum and
translator exceptions. Leaf module imported by translator and its mixins to
break the mixin↔translator import cycle."""

from __future__ import annotations

from enum import Enum


class GraphVarKind(str, Enum):
    NODE = "NODE"
    EDGE = "EDGE"
    PATH = "PATH"
    PASSTHROUGH = "PASSTHROUGH"  # pre-built JSON from rel/node union subquery


class CypherTranslateError(Exception):
    pass


class UnregisteredRelationshipType(CypherTranslateError):
    """A Cypher statement names a relationship type the model does not register (REQ-603): a
    traversal exists only along a registered relationship, so the statement is refused -- on
    every surface that carries Cypher, since every one of them translates it here."""

    def __init__(self, types: list[str]) -> None:
        self.types = types
        super().__init__(f"Unregistered relationship type(s): {', '.join(types)}")


class CypherCrossSourceError(CypherTranslateError):
    """Raised when a Cypher query spans multiple incompatible data sources."""

    pass
