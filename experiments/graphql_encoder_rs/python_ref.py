# Copyright (c) 2026 Kenneth Stott
# Canary: f1d7c5cc-c978-4ea6-9884-fd783e8af23e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The product's JSON paths, from Arrow batches to response bytes, run on the exact-values
contract (J497/J497b, b2b50fcb7: put its provisa/ first on the path, build.sh extracts it).

Every surface reads an ENGINE-routed result as ``_execute_plan``'s buffered drain reads it: the
engine's Arrow stream through ``stream_rows_from_arrow`` (``column_values`` per column, rows
zipped). What each router then does with the rows is called here as the router calls it, the
product's own functions unmodified; the few lines of router glue between them are transcribed
(each names its source lines) because the routers are closures over the app's state:

- GraphQL: ``serialize_rows`` (``_format_response``), ``{"data": {root: rows}}`` (``_handle_query``).
- REST (``rest/generator.py`` rest_table_endpoint, 754-847): ``serialize_rows`` or
  ``serialize_aggregate``, ``{"data": ...}``.
- JSON:API (``jsonapi/generator.py``, 936-988 and 1158-1233): ``serialize_rows``,
  ``rename_row_keys``, ``_extract_included``, ``rows_to_jsonapi``, meta, ``build_pagination_links``.
- OData (``odata/router.py`` ``_read``, 266-291): ``expand.write`` per row (``_entities``),
  ``@odata.context``, ``@odata.count``, ``@Provisa.warnings``, ``value``, ``page_link``.

Each body is what ``OrjsonResponse`` renders (``json_response.dumps``).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from provisa.api.json_response import OrjsonResponse
from provisa.compiler.sql_types import ColumnRef
from provisa.executor.serialize import serialize_aggregate, serialize_rows
from provisa.federation.runtime_support import stream_rows_from_arrow


def arrow_rows(schema: Any, batches: Sequence[Any]) -> list[tuple]:  # noqa: ANN401 — pyarrow
    """The rows the buffered drain collects (``_execute_plan``'s ``_drain``)."""
    return list(stream_rows_from_arrow(schema, iter(batches)).iter_rows())


def body(content: Any) -> bytes:  # noqa: ANN401 — a route's content
    return bytes(OrjsonResponse(content=content).body)


# ------------------------------------------------------------------ GraphQL


def encode(
    schema: Any,  # noqa: ANN401 — pyarrow.Schema
    batches: Sequence[Any],
    columns: list[ColumnRef],
    root_field: str,
    result_limit: int | None = None,
) -> bytes:
    rows = arrow_rows(schema, batches)
    response = serialize_rows(rows, columns, root_field, result_limit=result_limit)
    return body({"data": {root_field: response.get("data", {}).get(root_field, [])}})


def plan_tuples(columns: list[ColumnRef]) -> list[tuple]:
    """The selection plan the Rust encoders take: what the serializer reads of each ColumnRef."""
    return [
        (
            c.field_name,
            c.nested_in,
            c.cardinality,
            c.is_agg,
            c.json,
            sorted(c.json_text),
            c.integer,
        )
        for c in columns
    ]


# ------------------------------------------------------------------ REST


def rest(
    schema: Any,
    batches: Sequence[Any],
    columns: list[ColumnRef],
    root_field: str,
    agg_alias: str | None = None,
) -> bytes:  # noqa: ANN401
    """rest_table_endpoint 754-771, 847: a group-by's and a read's rows, or the aggregate."""
    rows = arrow_rows(schema, batches)
    if agg_alias is not None:
        agg_response = serialize_aggregate(rows, columns, root_field, agg_alias=agg_alias)
        return body({"data": agg_response["data"][root_field][agg_alias]})
    response_data = serialize_rows(rows, columns, root_field)
    return body({"data": response_data.get("data", {}).get(root_field, [])})


# ------------------------------------------------------------------ JSON:API


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
    """_jsonapi_table_endpoint 1158-1233, from the rows on."""
    from provisa.api.jsonapi.generator import _extract_included
    from provisa.api.jsonapi.naming import rename_row_keys
    from provisa.api.jsonapi.pagination import build_pagination_links
    from provisa.api.jsonapi.serializer import rows_to_jsonapi

    result_rows = arrow_rows(schema, batches)
    response_data = serialize_rows(result_rows, columns, gql_table)
    rows = response_data.get("data", {}).get(gql_table, [])
    has_next = len(rows) > page_size if probe_rows else len(rows) == page_size
    rows = rows[:page_size]
    rows = [
        rename_row_keys(row, gql_to_physical, rel_gql_to_physical, rel_scalar_gql_to_physical)
        for row in rows
    ]
    rel_physical_to_gql = {p: g for g, p in rel_gql_to_physical.items()}
    physical_rel_fields = {f"{p}_id": p for p in rel_physical_to_gql}
    included_rows, row_links = _extract_included(rows, physical_include_names, include_keys)
    doc = rows_to_jsonapi(
        rows,
        table_name,
        id_field="id",
        relationship_fields=physical_rel_fields,
        included_rows=included_rows or None,
        id_columns=base_keys,
        included_id_columns=include_keys,
        row_links=row_links if physical_include_names else None,
    )
    if total_count is not None:
        doc["meta"]["total"] = total_count
    else:
        del doc["meta"]["total"]
    if warnings:
        doc["meta"]["warnings"] = warnings
    if not doc["meta"]:
        del doc["meta"]
    doc["links"] = build_pagination_links(
        base_url=f"/data/jsonapi/{domain_id}/{table_name}",
        page_number=page_number,
        page_size=page_size,
        total=total_count,
        query_params=extra or None,
        has_next=has_next,
    )
    return body(doc)


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
    """_jsonapi_table_endpoint 936-971."""
    from provisa.api.jsonapi.naming import rename_row_keys

    shaped = serialize_rows(arrow_rows(schema, batches), columns, root_field)
    group_rows = shaped.get("data", {}).get(root_field, [])
    for row in group_rows:
        if "groupKey" in row:
            row["groupKey"] = {gql_to_physical.get(k, k): v for k, v in row["groupKey"].items()}
        if "nodes" in row:
            row["nodes"] = [
                rename_row_keys(
                    node, gql_to_physical, rel_gql_to_physical, rel_scalar_gql_to_physical
                )
                for node in row["nodes"]
            ]
    data = [
        {"type": f"{table_name}GroupBy", "id": str(idx), "attributes": row}
        for idx, row in enumerate(group_rows)
    ]
    return body({"data": data})


def jsonapi_aggregate(
    schema: Any,
    batches: Sequence[Any],
    columns: list[ColumnRef],
    *,
    root_field: str,
    agg_alias: str,
) -> bytes:  # noqa: ANN401
    """_jsonapi_table_endpoint 973-988."""
    shaped = serialize_aggregate(
        arrow_rows(schema, batches), columns, root_field, agg_alias=agg_alias
    )
    agg_payload = shaped.get("data", {}).get(root_field, {}).get(agg_alias, {})
    return body({"data": None, "meta": {"aggregate": agg_payload}})


# ------------------------------------------------------------------ OData


def odata(
    schema: Any,  # noqa: ANN401
    batches: Sequence[Any],
    names: list[str],
    *,
    entity: Any,  # noqa: ANN401 — EntitySet
    fields: tuple[str, ...],
    plans: tuple[Any, ...],  # noqa: ANN401 — expand.Plan
    context: str,
    count: int | None,
    page: int,
    root: str,
    items: tuple[tuple[str, str], ...],
    options: Any,  # noqa: ANN401 — QueryOptions
    warnings: list[dict] | None,
) -> bytes:
    """router._read 249-291, from the statement's rows on (``_entities``: ``write`` per row)."""
    from provisa.api.odata.expand import page_link, write

    document: dict[str, Any] = {"@odata.context": context}
    if count is not None:
        document["@odata.count"] = count
    if page == 0:
        document["value"] = []
        return body(document)
    rows = arrow_rows(schema, batches)
    if warnings:
        document["@Provisa.warnings"] = warnings
    document["value"] = [
        write(dict(zip(names, row)), entity, fields, plans, root) for row in rows[:page]
    ]
    if len(rows) > page:
        document["@odata.nextLink"] = page_link(f"{root}/{entity.name}", items, options, page)
    return body(document)
