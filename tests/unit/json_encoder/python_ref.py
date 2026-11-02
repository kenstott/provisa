# Copyright (c) 2026 Kenneth Stott
# Canary: 2e1ddc86-e05d-4ade-a2bf-cb4bf1d30dd5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The reference side of the differential: the JSON surfaces' Python path (oracle.py, the old path
frozen and corrected where a defect was fixed), from Arrow batches to the response body, as each
router called it; each body is what the product's OrjsonResponse renders (json_response.dumps)."""

from __future__ import annotations

import csv
import io
from collections.abc import Sequence
from typing import Any

from provisa.api.json_response import dumps
from provisa.compiler.sql_types import ColumnRef
from tests.unit.json_encoder import oracle


def encode(
    schema: Any,
    batches: Sequence[Any],
    columns: list[ColumnRef],
    root_field: str,
    result_limit: int | None = None,
) -> bytes:  # noqa: ANN401
    rows, truncated = oracle.serialize_rows(oracle.arrow_rows(list(batches)), columns, result_limit)
    doc: dict[str, Any] = {"data": {root_field: rows}}
    if truncated:
        doc["extensions"] = {"warnings": oracle.truncation_warnings(truncated)}
    return dumps(doc)


def aggregate(
    schema: Any, batches: Sequence[Any], columns: list[ColumnRef], root_field: str, alias: str
) -> bytes:  # noqa: ANN401
    payload = oracle.serialize_aggregate(oracle.arrow_rows(list(batches)), columns, alias)
    return dumps({"data": {root_field: payload}})


def rest(
    schema: Any,
    batches: Sequence[Any],
    columns: list[ColumnRef],
    root_field: str,
    agg_alias: str | None = None,
) -> bytes:  # noqa: ANN401
    rows = oracle.arrow_rows(list(batches))
    if agg_alias is not None:
        return dumps({"data": oracle.serialize_aggregate(rows, columns, agg_alias)[agg_alias]})
    return dumps({"data": oracle.serialize_rows(rows, columns)[0]})


def rest_csv(
    schema: Any, batches: Sequence[Any], columns: list[ColumnRef], root_field: str
) -> bytes:  # noqa: ANN401
    rows, _ = oracle.serialize_rows(oracle.arrow_rows(list(batches)), columns)
    buf = io.StringIO()
    writer = csv.writer(buf)
    if rows:
        writer.writerow(rows[0].keys())
        for row in rows:
            writer.writerow(row.values())
    return buf.getvalue().encode("utf-8")


def jsonapi(
    schema: Any,  # noqa: ANN401
    batches: Sequence[Any],
    columns: list[ColumnRef],
    *,
    gql_table: str,
    table_name: str,
    domain_id: str,
    page_number: int,
    page_size: int,
    probe_rows: int,
    total_count: int | None,
    base_keys: list[str],
    gql_to_physical: dict[str, str],
    rel_gql_to_physical: dict[str, str],
    rel_scalar_gql_to_physical: dict[str, dict[str, str]],
    physical_include_names: list[str],
    include_keys: dict[str, list[str]],
    extra: dict[str, str],
    warnings: list[dict] | None,
) -> bytes:
    rows, truncated = oracle.serialize_rows(oracle.arrow_rows(list(batches)), columns)
    return dumps(
        oracle.jsonapi_document(
            rows,
            truncated=truncated,
            table_name=table_name,
            page_number=page_number,
            page_size=page_size,
            probe_rows=probe_rows,
            total_count=total_count,
            base_keys=base_keys,
            gql_to_physical=gql_to_physical,
            rel_gql_to_physical=rel_gql_to_physical,
            rel_scalar_gql_to_physical=rel_scalar_gql_to_physical,
            physical_include_names=physical_include_names,
            include_keys=include_keys,
            base_path=f"/data/jsonapi/{domain_id}/{table_name}",
            extra=extra,
            warnings=warnings,
        )
    )


def jsonapi_group_by(
    schema: Any,  # noqa: ANN401
    batches: Sequence[Any],
    columns: list[ColumnRef],
    *,
    root_field: str,
    table_name: str,
    gql_to_physical: dict[str, str],
    rel_gql_to_physical: dict[str, str],
    rel_scalar_gql_to_physical: dict[str, dict[str, str]],
) -> bytes:
    group_rows, _ = oracle.serialize_rows(oracle.arrow_rows(list(batches)), columns)
    return dumps(
        oracle.jsonapi_group_by(
            group_rows,
            columns,
            table_name=table_name,
            gql_to_physical=gql_to_physical,
            rel_gql_to_physical=rel_gql_to_physical,
            rel_scalar_gql_to_physical=rel_scalar_gql_to_physical,
        )
    )


def jsonapi_aggregate(
    schema: Any,
    batches: Sequence[Any],
    columns: list[ColumnRef],
    *,
    root_field: str,
    agg_alias: str,
) -> bytes:  # noqa: ANN401
    payload = oracle.serialize_aggregate(oracle.arrow_rows(list(batches)), columns, agg_alias)
    return dumps({"data": None, "meta": {"aggregate": payload[agg_alias]}})


def odata(
    schema: Any,  # noqa: ANN401
    batches: Sequence[Any],
    names: list[str],
    *,
    entity: Any,  # noqa: ANN401
    fields: tuple[str, ...],
    plans: tuple[Any, ...],  # noqa: ANN401
    context: str,
    count: int | None,
    page: int,
    root: str,
    items: tuple[tuple[str, str], ...],
    options: Any,  # noqa: ANN401
    warnings: list[dict] | None,
) -> bytes:
    from provisa.api.odata.expand import page_link

    document: dict[str, Any] = {"@odata.context": context}
    if count is not None:
        document["@odata.count"] = count
    if page == 0:
        document["value"] = []
        return dumps(document)
    rows = oracle.arrow_rows(list(batches))
    if warnings:
        document["@Provisa.warnings"] = warnings
    document["value"] = [
        oracle.write(dict(zip(names, row)), entity, fields, plans, root) for row in rows[:page]
    ]
    if len(rows) > page:
        document["@odata.nextLink"] = page_link(f"{root}/{entity.name}", items, options, page)
    return dumps(document)
