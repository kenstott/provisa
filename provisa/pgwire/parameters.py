# Copyright (c) 2026 Kenneth Stott
# Canary: 77a7af5f-9554-4975-8360-5b5ba611bc4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The types of a statement's ``$N`` placeholders, as pgwire's ParameterDescription answers them
when the client declared none: by the statement's own cast of the placeholder (``$1::int``),
else a bigint. Flight SQL's prepared statements answer the same (REQ-2175)."""

# Requirements: REQ-2175

from __future__ import annotations

import re
from collections.abc import Callable, Container

#: A cast's type name as the PostgreSQL type oid a placeholder takes from it; any other name is
#: text.
_CAST_OID = {
    "text": 25,
    "varchar": 25,
    "int": 23,
    "int4": 23,
    "int8": 20,
    "bigint": 20,
    "bool": 16,
    "float8": 701,
}
#: A placeholder the statement casts nowhere: a bigint.
_UNCAST_OID = 20
_TEXT_OID = 25
_VARCHAR_OID = 1043
#: A string literal: a ``$1`` in one ('$10'::money) is its text, no placeholder.
_LITERAL = re.compile(r"'(?:[^']|'')*'")


def placeholder_oids(sql: str) -> list[int]:
    """The type oid of each placeholder ``$1`` .. ``$n`` of ``sql`` (none when it has none)."""
    sql = _LITERAL.sub("''", sql)
    indices = {int(m) for m in re.findall(r"\$(\d+)", sql)}
    if not indices:
        return []
    cast = {int(m): _CAST_OID.get(t.lower(), 25) for m, t in re.findall(r"\$(\d+)::(\w+)", sql)}
    return [cast.get(i, _UNCAST_OID) for i in range(1, max(indices) + 1)]


def resolved_oids(
    oids: list[int], types: dict[int, str], oid_of: Callable[[str], int], bindable: Container[int]
) -> list[int]:
    """``oids`` with each placeholder the compiler typed (``types``, $N to its type) as that type,
    as PostgreSQL resolves it: a varchar has no operators of its own, so what meets one is text.
    A type Bind has no decoder of is text: its value is sent in its text form."""
    resolved = list(oids)
    for index, spelled in types.items():
        oid = oid_of(spelled)
        known = oid if oid in bindable and oid != _VARCHAR_OID else _TEXT_OID
        if index <= len(resolved):
            resolved[index - 1] = known
    return resolved
