# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1929: a JSON:API root resource's id, and the single-resource lookup it allows."""

# Requirements: REQ-1929

from __future__ import annotations

from typing import Any


def resource_id_columns(ctx: Any, table_id: int, physical_to_gql: dict[str, str]) -> list[str]:  # noqa: ANN401
    """A root resource's id columns, physical, in key order: its declared primary key;
    otherwise the column an approved relationship points at; otherwise a column named id;
    otherwise none, and its id is its position in the response. No read is refused for want of
    a key. Only a type with id columns offers a single-resource lookup."""
    keys = ctx.pk_columns.get(table_id) or []
    if keys and all(k in physical_to_gql for k in keys):
        return list(keys)
    for join in ctx.joins.values():
        if (
            join.target.table_id == table_id
            and join.target_expr is None
            and join.target_column in physical_to_gql
        ):
            return [join.target_column]
    if "id" in physical_to_gql:
        return ["id"]
    return []


def split_resource_id(text: str, count: int) -> list[str] | None:
    """The key values a resource id names, the inverse of the encoder's ``resource_id``: one
    key's text as itself; several joined by ':' with each ':' and '\\' escaped by '\\'. None
    when the text does not name ``count`` values."""
    if count == 1:
        return [text]
    parts: list[str] = []
    cur: list[str] = []
    chars = iter(text)
    for ch in chars:
        if ch == "\\":
            nxt = next(chars, None)
            if nxt not in (":", "\\"):
                return None
            cur.append(nxt)
        elif ch == ":":
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    parts.append("".join(cur))
    return parts if len(parts) == count else None
