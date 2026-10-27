# Copyright (c) 2026 Kenneth Stott
# Canary: 0b670fb8-680b-4fe7-b1ff-96b3e934b363
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The shape of a materialized view's store table (REQ-970): its key and its typed output
columns, read the same way by the event loop's reconcile and by a refresh's first build, so the two
never disagree about the table -- a disagreement drops the built rows."""

# Requirements: REQ-970

from __future__ import annotations

from typing import Any


def view_key(mv: Any) -> list[str]:
    """The view's store-table key: the operator-declared ``primary_key``, else inferred from an
    unambiguous GROUP BY; ``[]`` when neither."""
    from provisa.events.lineage import infer_pk

    declared = list(getattr(mv, "primary_key", []) or [])
    if declared:
        return declared
    return infer_pk(mv.sql) if getattr(mv, "sql", None) else []


async def output_columns(
    engine: Any, select_sql: str, *, authorization: Any
) -> list[tuple[str, str]] | None:
    """The typed output columns of ``select_sql`` (name, IR type), read by a zero-row probe through
    the engine; None when the engine returns no column types."""
    from provisa.core.ir_types import to_ir

    probe = await engine.execute_engine(
        # Its columns alone: never its rows, a column transform's among them.
        f"SELECT * FROM ({select_sql}) AS _mv_probe LIMIT 0",
        authorization=authorization,
    )
    if probe.column_types is None:
        return None
    dialect = engine.engine.dialect
    return [(n, to_ir(t, dialect)) for n, t in zip(probe.column_names, probe.column_types)]
