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
Rust side of the differential."""

from __future__ import annotations

from typing import Any

import orjson

from provisa.compiler.sql_types import ColumnRef
from provisa.executor import json_encoders as je
from provisa.executor.serialize import to_one_truncated
from provisa.observability.request_facts import DocumentResponse


def encode_response(
    batches: list[Any], columns: list[ColumnRef], root: str, limit: int | None = None
) -> bytes:  # noqa: ANN401
    """GraphQL, as _handle_query answers one root field: the rows embedded as their bytes, and
    each truncated to-one path's warning in extensions.warnings (REQ-1350)."""
    answer = je.graphql_field(batches, columns, limit)
    doc: dict[str, Any] = {"data": {root: orjson.Fragment(answer.body)}}
    if answer.truncated:
        doc["extensions"] = {"warnings": [to_one_truncated(p).as_dict() for p in answer.truncated]}
    return bytes(DocumentResponse(doc).body)


def encode_aggregate(batches: list[Any], columns: list[ColumnRef], root: str, alias: str) -> bytes:  # noqa: ANN401
    doc = {"data": {root: orjson.Fragment(je.graphql_aggregate(batches, columns, alias))}}
    return bytes(DocumentResponse(doc).body)


def encode_rest(
    batches: list[Any], columns: list[ColumnRef], agg_alias: str | None = None
) -> bytes:  # noqa: ANN401
    return je.rest(batches, columns, agg_alias)[0]


def encode_rest_csv(batches: list[Any], columns: list[ColumnRef]) -> bytes:  # noqa: ANN401
    return je.rest_csv(batches, columns)[0]


def encode_jsonapi(batches: list[Any], columns: list[ColumnRef], **document: Any) -> bytes:  # noqa: ANN401
    return je.jsonapi(batches, columns, **document)[0]


def encode_jsonapi_group_by(batches: list[Any], columns: list[ColumnRef], **names: Any) -> bytes:  # noqa: ANN401
    return je.jsonapi_group_by(batches, columns, **names)


def encode_jsonapi_aggregate(batches: list[Any], columns: list[ColumnRef], alias: str) -> bytes:  # noqa: ANN401
    return je.jsonapi_aggregate(batches, columns, alias)


def encode_odata(batches: list[Any], names: list[str], entity: tuple, **document: Any) -> bytes:  # noqa: ANN401
    return je.odata(batches, names, entity, **document)
