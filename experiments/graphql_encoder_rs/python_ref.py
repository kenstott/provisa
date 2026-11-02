# Copyright (c) 2026 Kenneth Stott
# Canary: f1d7c5cc-c978-4ea6-9884-fd783e8af23e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The product's GraphQL JSON path, run unmodified, from Arrow batches to response bytes.

An ENGINE-routed root field is read as ``_execute_plan``'s buffered drain reads it: the engine's
Arrow stream through ``stream_rows_from_arrow`` (``column_values`` per column, rows zipped), every
row taken with ``iter_rows``. ``_execute_one_field`` shapes the rows with ``serialize_rows`` (through
``_format_response``) and keeps the root field's rows; ``_handle_query`` answers
``{"data": {root_field: rows}}`` as a ``TimedJSONResponse``, an ``OrjsonResponse`` whose ``render``
is ``json_response.dumps``. The stage timer TimedJSONResponse adds is left out.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from provisa.api.json_response import OrjsonResponse
from provisa.compiler.sql_types import ColumnRef
from provisa.executor.serialize import serialize_rows
from provisa.federation.runtime_support import stream_rows_from_arrow


def arrow_rows(schema: Any, batches: Sequence[Any]) -> list[tuple]:  # noqa: ANN401 — pyarrow
    """The rows the buffered drain collects (``_execute_plan``'s ``_drain``)."""
    return list(stream_rows_from_arrow(schema, iter(batches)).iter_rows())


def shape(rows: list[tuple], columns: list[ColumnRef], root_field: str, result_limit: int | None):
    """``_execute_one_field``'s field rows for a JSON answer."""
    response = serialize_rows(rows, columns, root_field, result_limit=result_limit)
    return response.get("data", {}).get(root_field, [])


def render(root_field: str, field_rows: Any) -> bytes:  # noqa: ANN401 — the shaped rows
    """``_handle_query``'s single-field answer, as OrjsonResponse renders it."""
    return OrjsonResponse(content={"data": {root_field: field_rows}}).body


def encode(
    schema: Any,  # noqa: ANN401 — pyarrow.Schema
    batches: Sequence[Any],
    columns: list[ColumnRef],
    root_field: str,
    result_limit: int | None = None,
) -> bytes:
    return render(root_field, shape(arrow_rows(schema, batches), columns, root_field, result_limit))


def plan_tuples(columns: list[ColumnRef]) -> list[tuple[str, str | None, str | None, bool]]:
    """The selection plan the Rust encoder takes: what serialize_rows reads of each ColumnRef."""
    return [(c.field_name, c.nested_in, c.cardinality, c.is_agg) for c in columns]
