# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Lower a gRPC table request directly to the query IR (a semantic SELECT).

Shared by the native gRPC servicer and the HTTP gRPC proxy so both follow the one pipeline every
transport uses — query language → IR → governed IR → plan → physical — and gRPC never round-trips
through GraphQL.
"""

from __future__ import annotations

from typing import Any

from provisa.compiler.sql_gen import _q
from provisa.compiler.sql_rewrite import _semantic_table_ref


def grpc_table_to_semantic_sql(ctx: Any, type_name: str, limit: int) -> str | None:
    """Semantic SELECT over the table matching ``type_name``, or None if none matches. proto collapses
    the domain separator (``PS__Inquiries`` → ``PsInquiries``), so match case/separator-insensitively."""

    def _n(s: str) -> str:
        return s.replace("_", "").lower()

    meta = next((m for m in ctx.tables.values() if _n(m.type_name) == _n(type_name)), None)
    if meta is None:
        return None
    cols = ", ".join(_q(c) for c, _t in ctx.aggregate_columns.get(meta.table_id, [])) or "*"
    sql = f"SELECT {cols} FROM {_semantic_table_ref(meta)}"
    return f"{sql} LIMIT {int(limit)}" if limit and limit > 0 else sql
