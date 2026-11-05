# Copyright (c) 2026 Kenneth Stott
# Canary: 33c482d7-558d-4109-916e-a501810cbbc2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A GraphQL field's answer to rows a test writes, as the product gives it: the rows become Arrow
at a row engine's fetch boundary (``ir_arrow.row_batch``), the Rust encoder writes the answer
(provisa.executor.json_encoders), and the test reads the JSON back (``json.loads``: an integer
keeps all its digits). The shapes are the ones the
deleted ``serialize_rows`` / ``serialize_aggregate`` returned, so a test of what they built is a
test of what the encoder writes."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import json

from provisa.compiler.sql_types import ColumnRef
from provisa.core.ir_arrow import row_batch
from provisa.executor import json_encoders as je
from provisa.executor.serialize import to_one_truncated


def batches(rows: Sequence[Sequence[Any]], columns: list[ColumnRef]) -> list[Any]:  # noqa: ANN401
    """The rows as one record batch (none for no row), a column per selected column."""
    if not rows:
        return []
    names = [f"c{i}" for i in range(len(columns))]
    return [row_batch([tuple(r) for r in rows], names)]


def encoded(
    rows: Sequence[Sequence[Any]], columns: list[ColumnRef], result_limit: int | None = None
) -> bytes:  # noqa: ANN401
    """The field's rows as the encoder writes them: its JSON bytes."""
    return je.graphql_field(batches(rows, columns), columns, result_limit).body


def serialize_rows(
    rows: Sequence[Sequence[Any]],
    columns: list[ColumnRef],
    root_field: str,
    result_limit: int | None = None,
) -> dict:
    """``{"data": {root_field: rows}}``, with each truncated to-one path in
    ``extensions.warnings`` (``{message, path}``)."""
    answer = je.graphql_field(batches(rows, columns), columns, result_limit)
    out: dict[str, Any] = {"data": {root_field: json.loads(answer.body)}}
    if answer.truncated:
        out["extensions"] = {
            "warnings": [
                {"message": to_one_truncated(path).message, "path": path}
                for path in answer.truncated
            ]
        }
    return out


def serialize_aggregate(
    rows: Sequence[Sequence[Any]],
    columns: list[ColumnRef],
    root_field: str,
    agg_alias: str = "aggregate",
) -> dict:
    """``{"data": {root_field: {agg_alias: {...}, "nodes": [...]}}}``."""
    body = je.graphql_aggregate(batches(rows, columns), columns, agg_alias)
    return {"data": {root_field: json.loads(body)}}


def field_answer(value: Any) -> je.FieldAnswer:  # noqa: ANN401 — a field's value
    """A root field's answer holding ``value`` as its JSON, as a stub of ``_execute_one_field``
    returns it (the encoder's FieldAnswer)."""
    rows = len(value) if isinstance(value, list) else None
    return je.FieldAnswer(json.dumps(value, separators=(",", ":")).encode(), rows, {}, [])
