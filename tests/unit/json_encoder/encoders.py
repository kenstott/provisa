# Copyright (c) 2026 Kenneth Stott
# Canary: b223da12-76f4-4cd8-b512-3d12d5ec86eb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The product's encoder path for each JSON surface, as the routers call it
(provisa.executor.json_encoders, provisa._encoders.json), each returning the response body: the
Rust side of the differential. Each also encodes the same result as a row result (REQ-2195:
``RowBatches``, the cells column_values gives) and holds it to the Arrow form's answer."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

import orjson
import pandas as pd
import pyarrow as pa

from provisa._encoders import json as encoders
from provisa.compiler.sql_types import ColumnRef
from provisa.core.ir_arrow import column_values
from provisa.executor import json_encoders as je
from provisa.executor.serialize import to_one_truncated
from provisa.observability.request_facts import DocumentResponse


_T = TypeVar("_T")


def _row_form(batches: list[Any]) -> Any:  # noqa: ANN401 — RowBatches, or None
    """``batches`` as a row result's batches, None where a row source holds no such values: a map
    (column_values gives a list of (key, value) tuples; a row source's map is a dict), pandas'
    NaT, or a cell column_values refuses."""
    if any("map<" in str(f.type) for b in batches for f in b.schema):
        return None
    try:
        rows = [list(zip(*(column_values(c) for c in b.columns), strict=True)) for b in batches]
    except (ValueError, OverflowError, TypeError, KeyError, pa.ArrowException):
        return None
    if any(cell is pd.NaT for batch in rows for row in batch for cell in row):
        return None  # pandas' NaT, which only Arrow's int64 minimum is
    return encoders.RowBatches([r for r in rows if r])


def _rows_too(encode: Callable[[Any], _T], batches: list[Any]) -> _T:  # noqa: ANN401
    """``encode(batches)``, its answer held to the same result's as a row result (REQ-2195)."""
    rows = _row_form(batches)
    try:
        sent = encode(batches)
    except Exception as exc:
        if rows is not None:
            try:
                encode(rows)
            except Exception as from_rows:  # noqa: BLE001 — refused as the Arrow form is
                assert type(from_rows) is type(exc), f"arrow {exc!r}; rows {from_rows!r}"
            else:
                raise AssertionError(
                    f"the Arrow form raised {exc!r}; the rows form answered"
                ) from exc
        raise
    if rows is not None:
        from_rows = encode(rows)
        if isinstance(sent, je.FieldAnswer):
            assert isinstance(from_rows, je.FieldAnswer)
            got = (from_rows.body, from_rows.rows, from_rows.joined, from_rows.truncated)
            assert got == (sent.body, sent.rows, sent.joined, sent.truncated), (
                "the rows form answers otherwise"
            )
        else:
            assert from_rows == sent, "the rows form answers otherwise"
    return sent


def encode_response(
    batches: list[Any], columns: list[ColumnRef], root: str, limit: int | None = None
) -> bytes:  # noqa: ANN401
    """GraphQL, as _handle_query answers one root field: the rows embedded as their bytes, and
    each truncated to-one path's warning in extensions.warnings (REQ-1350)."""
    answer = _rows_too(lambda b: je.graphql_field(b, columns, limit), batches)
    doc: dict[str, Any] = {"data": {root: orjson.Fragment(answer.body)}}
    if answer.truncated:
        doc["extensions"] = {"warnings": [to_one_truncated(p).as_dict() for p in answer.truncated]}
    return bytes(DocumentResponse(doc).body)


def encode_aggregate(batches: list[Any], columns: list[ColumnRef], root: str, alias: str) -> bytes:  # noqa: ANN401
    doc = {
        "data": {
            root: orjson.Fragment(
                _rows_too(lambda b: je.graphql_aggregate(b, columns, alias), batches)
            )
        }
    }
    return bytes(DocumentResponse(doc).body)


def encode_rest(
    batches: list[Any], columns: list[ColumnRef], agg_alias: str | None = None
) -> bytes:  # noqa: ANN401
    return _rows_too(lambda b: je.rest(b, columns, agg_alias), batches)[0]


def encode_rest_csv(batches: list[Any], columns: list[ColumnRef]) -> bytes:  # noqa: ANN401
    return _rows_too(lambda b: je.rest_csv(b, columns), batches)[0]


def encode_jsonapi(batches: list[Any], columns: list[ColumnRef], **document: Any) -> bytes:  # noqa: ANN401
    return _rows_too(lambda b: je.jsonapi(b, columns, **document), batches)[0]


def encode_jsonapi_group_by(batches: list[Any], columns: list[ColumnRef], **names: Any) -> bytes:  # noqa: ANN401
    return _rows_too(lambda b: je.jsonapi_group_by(b, columns, **names), batches)


def encode_jsonapi_aggregate(batches: list[Any], columns: list[ColumnRef], alias: str) -> bytes:  # noqa: ANN401
    return _rows_too(lambda b: je.jsonapi_aggregate(b, columns, alias), batches)


def encode_odata(batches: list[Any], names: list[str], entity: tuple, **document: Any) -> bytes:  # noqa: ANN401
    return _rows_too(lambda b: je.odata(b, names, entity, **document), batches)
