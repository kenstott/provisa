# Copyright (c) 2026 Kenneth Stott
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


def placeholder_oids(sql: str) -> list[int]:
    """The type oid of each placeholder ``$1`` .. ``$n`` of ``sql`` (none when it has none)."""
    indices = {int(m) for m in re.findall(r"\$(\d+)", sql)}
    if not indices:
        return []
    cast = {int(m): _CAST_OID.get(t.lower(), 25) for m, t in re.findall(r"\$(\d+)::(\w+)", sql)}
    return [cast.get(i, _UNCAST_OID) for i in range(1, max(indices) + 1)]
