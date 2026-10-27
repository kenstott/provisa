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

import difflib
from enum import Enum
from typing import Any


class GraphVarKind(str, Enum):
    NODE = "NODE"
    EDGE = "EDGE"
    PATH = "PATH"
    PASSTHROUGH = "PASSTHROUGH"  # pre-built JSON from rel/node union subquery
    MAP = "MAP"  # a map property: a declared object column, returned as a map (REQ-1991)
    # REQ-345: a map literal -- bare, in a list, collected, or a COLLECT { } subquery's rows. Built
    # with each value JSON-encoded (so a map of several types is one engine type); the answer
    # decodes them.
    JSON_MAP = "JSON_MAP"
    # A list literal of values of several types ([1, 'a', true]): each value JSON-encoded, so the
    # list is one engine type; the answer decodes them.
    JSON_LIST = "JSON_LIST"
    DURATION = "DURATION"  # REQ-2173: a duration's [months, days, microseconds]
    ZONED = "ZONED"  # REQ-2173: a date time's [microseconds since the epoch, the zone it names]
    POINT = "POINT"  # a point's [srid, x, y(, z)] (spatial.py)
    # REQ-2135: 1 when a path bounded by the depth limit was cut there; the transport takes the
    # column off the rows and reports it beside them.
    CUT = "CUT"
    # REQ-2135: an RDF term, one JSON value (provisa.gql.terms); a subject's IRI is built from it
    # outside SQL (provisa.gql.iri).
    TERM = "TERM"


# The column a statement carries when one of its paths is bounded by the depth limit.
CUT_COLUMN = "__cut"

# REQ-2168: the table function a composed ``CALL ns.name(args) YIELD …`` is translated to, its
# arguments the Cypher name, the yields and the call's argument values (each as JSON). The Cypher
# surface resolves the name through the role's Cypher command bindings and puts the command's rows
# in its place before the statement is governed.
CALL_MARKER = "__provisa_command_call"


class CypherTranslateError(Exception):
    pass


class CypherMistake(CypherTranslateError):
    """REQ-778: a mistake in the user's statement, refused at translation in the user's own terms
    -- the clause or expression, what is wrong, and the fix -- before any engine runs it. ``code``
    and ``params`` let a client word it in the reader's language (serverErrors catalog)."""

    def __init__(self, code: str, message: str, **params: Any) -> None:
        self.code = code
        self.params = params
        super().__init__(message)


class UnregisteredRelationshipType(CypherMistake):
    """A Cypher statement names a relationship type the model does not register (REQ-603): a
    traversal exists only along a registered relationship, so the statement is refused -- on
    every surface that carries Cypher, since every one of them translates it here. Naming a type
    that does not exist is the user's mistake (REQ-778): the message offers the registered types
    nearest the first one named, as the nearest, not as every one there is."""

    def __init__(self, types: list[str], registered: set[str]) -> None:
        self.types = types
        named = ", ".join(f":{t}" for t in types)
        if not registered:
            super().__init__(
                "cypher.no_relationship_types",
                f"There is no relationship type {named}: this role can traverse no relationship.",
                types=named,
            )
            return
        near = ", ".join(
            f":{t}"
            for t in difflib.get_close_matches(types[0], sorted(registered), n=3, cutoff=0.0)
        )
        super().__init__(
            "cypher.unknown_relationship_type",
            f"There is no relationship type {named}. Nearest registered types: {near}.",
            types=named,
            suggestions=near,
        )


class CypherCrossSourceError(CypherTranslateError):
    """Raised when a Cypher query spans multiple incompatible data sources."""

    pass


#: The column a domain union's arm carries a node returned bare in: one JSON value, its id, its
#: label and its own type's properties (each as text, as a property column is).
NODE_COLUMN = "__node"
