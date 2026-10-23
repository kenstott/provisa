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
    MAP = "MAP"  # a map property: a declared object column, returned as a map (REQ-1991)
    # REQ-2135: 1 when a path bounded by the depth limit was cut there; the transport takes the
    # column off the rows and reports it beside them.
    CUT = "CUT"
    # REQ-2135: an RDF term, one JSON value (provisa.gql.terms); a subject's IRI is built from it
    # outside SQL (provisa.gql.iri).
    TERM = "TERM"


# The column a statement carries when one of its paths is bounded by the depth limit.
CUT_COLUMN = "__cut"


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


#: The column a domain union's arm carries a node returned bare in: one JSON value, its id, its
#: label and its own type's properties (each as text, as a property column is).
NODE_COLUMN = "__node"
