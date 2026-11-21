# Copyright (c) 2026 Kenneth Stott
# Canary: 8e4d1b7a-2c95-4f3e-b061-7a9c3e5d2f18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin routes behind a Wikidata source's table form (REQ-1923).

Endpoints:
  GET  /admin/sources/sparql/wikidata/starters      — the starter reference tables
  GET  /admin/sources/sparql/wikidata/search?q=&kind= — items by name: a class (only those with
                                                      instances), or a filter value
  GET  /admin/sources/sparql/wikidata/browse        — the most-used top-level classes, counted
  GET  /admin/sources/sparql/wikidata/classes/{id}  — a class: its instance count, broader and
                                                      narrower classes, suggested columns
  POST /admin/sources/sparql/wikidata/class-table   — the SPARQL table of a class's instances

  POST /admin/sources/sparql/columns                — any SPARQL table's columns, read from the
                                                      SELECT variables of its query

Nothing is saved here and nothing waits on a query of the table itself: the form registers the
table it is given as any SPARQL table, its columns read from its query.
"""

# Requirements: REQ-1923

from __future__ import annotations

from typing import Literal

import httpx
from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.admin.capabilities import require_capability_request
from provisa.api.errors import ApiError
from provisa.api_source.models import ApiColumnType
from provisa.core.http_failure import describe_http_failure
from provisa.api_source.persist import data_type_for_api_column
from provisa.sparql import wikidata
from provisa.sparql.source import extract_variables

router = APIRouter(prefix="/admin/sources/sparql/wikidata", tags=["admin", "sparql"])
sparql_router = APIRouter(prefix="/admin/sources/sparql", tags=["admin", "sparql"])


def _refused(exc: wikidata.InvalidWikidataSource) -> ApiError:
    return ApiError(422, exc.code, str(exc), **exc.params)


def _unreachable(exc: httpx.HTTPError) -> ApiError:
    reason = describe_http_failure(exc)
    return ApiError(502, "wikidata.unreachable", reason, reason=reason)


@router.get("/starters")
async def starters(request: Request) -> list[dict]:
    require_capability_request(request, "table_registration")
    return [wikidata.describe_starter(s) for s in wikidata.STARTER_TABLES]


@router.get("/search")
async def search(
    request: Request, q: str, language: str, kind: Literal["class", "item"]
) -> list[dict]:
    require_capability_request(request, "table_registration")
    try:
        return await wikidata.search_entities(q, language=language, classes=kind == "class")
    except wikidata.InvalidWikidataSource as exc:
        raise _refused(exc) from exc
    except httpx.HTTPError as exc:
        raise _unreachable(exc) from exc


@router.get("/browse")
async def browse(request: Request, language: str) -> list[dict]:
    require_capability_request(request, "table_registration")
    try:
        return await wikidata.browse_classes(language=language)
    except wikidata.InvalidWikidataSource as exc:
        raise _refused(exc) from exc
    except httpx.HTTPError as exc:
        raise _unreachable(exc) from exc


@router.get("/classes/{class_id}")
async def describe_class(request: Request, class_id: str, language: str) -> dict:
    require_capability_request(request, "table_registration")
    try:
        info = await wikidata.describe_class(class_id, language=language)
    except wikidata.InvalidWikidataSource as exc:
        raise _refused(exc) from exc
    except httpx.HTTPError as exc:
        raise _unreachable(exc) from exc
    return wikidata.describe_class_info(info)


class ClassPropertyBody(BaseModel):
    id: str
    label: str | None
    data_type: str
    item: bool


class RowFilterBody(BaseModel):
    property: str
    value: str


class ClassTableBody(BaseModel):
    class_id: str
    properties: list[ClassPropertyBody]
    filter: RowFilterBody | None
    language: str


# The column types a property's values land as (wikidata._PROPERTY_TYPES), by data_type name.
_DATA_TYPES = {
    "text": ApiColumnType.string,
    "double": ApiColumnType.number,
    "timestamp": ApiColumnType.timestamp,
}


@router.post("/class-table")
async def class_table(request: Request, body: ClassTableBody) -> dict:
    require_capability_request(request, "table_registration")
    unknown = [p.data_type for p in body.properties if p.data_type not in _DATA_TYPES]
    if unknown:
        raise ApiError(
            422,
            "wikidata.bad_column_type",
            f"Column types not offered for a Wikidata property: {unknown}",
            types=unknown,
        )
    properties = [
        wikidata.ClassProperty(
            id=p.id, label=p.label, type=_DATA_TYPES[p.data_type], item=p.item, uses=0
        )
        for p in body.properties
    ]
    row_filter = (
        wikidata.RowFilter(property=body.filter.property, value=body.filter.value)
        if body.filter is not None
        else None
    )
    try:
        table = await wikidata.class_table(
            body.class_id, properties, row_filter=row_filter, language=body.language
        )
    except wikidata.InvalidWikidataSource as exc:
        raise _refused(exc) from exc
    except httpx.HTTPError as exc:
        raise _unreachable(exc) from exc
    return wikidata.describe_class_table(table)


class SparqlQueryBody(BaseModel):
    query: str


@sparql_router.post("/columns")
async def select_columns(request: Request, body: SparqlQueryBody) -> dict:
    """REQ-1923 (amended 2026-11-20): a SPARQL table's columns, one per SELECT variable of its
    query, every binding text; read from the query alone, so nothing waits on the endpoint. A
    query whose columns cannot be read from it (``SELECT *``, or not a SELECT) is refused."""
    require_capability_request(request, "table_registration")
    names = extract_variables(body.query)
    if not names:
        raise ApiError(
            422,
            "sparql.no_select_variables",
            "The query names no SELECT variables (SELECT * or not a SELECT query): its columns "
            "cannot be read from it",
        )
    text = data_type_for_api_column(ApiColumnType.string)
    return {"columns": [{"name": n, "data_type": text} for n in names]}
