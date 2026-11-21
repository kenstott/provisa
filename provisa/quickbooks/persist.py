# Copyright (c) 2026 Kenneth Stott
# Canary: a0e93d57-6c21-4f8b-b7d4-5f1c28e06b39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A registered QuickBooks table's API source and endpoint rows (REQ-2197): what the API caller
reads it through, written when the table is registered, as a SPARQL table's are.

- ``api_sources``: the company's address in the Accounting API, and the source's credential (the
  app and the approval entered on its form), encrypted at rest;
- ``api_endpoints``: the entity's query, its columns typed from the entity catalog, and its
  paging: pages of the table's page size (at most QuickBooks' 1,000), up to its ``max_pages``.
"""

# Requirements: REQ-2197
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from provisa.api_source.models import ApiColumn, ApiEndpoint, ApiSource, ApiSourceType
from provisa.core.paging import PaginationConfig, PaginationType
from provisa.quickbooks.entities import columns_of, entity
from provisa.quickbooks.query import MAX_PAGE, entity_query
from provisa.quickbooks.rows import NORMALIZER
from provisa.quickbooks.settings import QuickBooksSettings

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: The path a query is sent to under the company's address.
QUERY_PATH = "/query"
#: How many pages one read takes unless the table says otherwise: 100,000 rows at full pages.
DEFAULT_MAX_PAGES = 100


class QuickBooksTableRefused(ValueError):
    """A table a QuickBooks source cannot serve: a stable code, its params, English text."""

    def __init__(self, code: str, message: str, **params: str) -> None:
        super().__init__(message)
        self.code = code
        self.params = params


def table_paging(pagination: PaginationConfig | None) -> PaginationConfig:
    """The paging a QuickBooks table is read with: the table's own page size and page count
    where it sets them, QuickBooks' largest page and :data:`DEFAULT_MAX_PAGES` otherwise."""
    page_size = MAX_PAGE
    max_pages = DEFAULT_MAX_PAGES
    if pagination is not None and pagination.is_endpoint:
        if "page_size" in pagination.model_fields_set:
            page_size = pagination.page_size
        if "max_pages" in pagination.model_fields_set:
            max_pages = pagination.max_pages
    if page_size > MAX_PAGE:
        raise QuickBooksTableRefused(
            "quickbooks.page_too_large",
            f"A QuickBooks page holds at most {MAX_PAGE} rows; the table asks for {page_size}",
            page_size=str(page_size),
        )
    return PaginationConfig(
        type=PaginationType.offset,
        page_param="STARTPOSITION",
        page_size_param="MAXRESULTS",
        page_size=page_size,
        max_pages=max_pages,
    )


def build_endpoint(
    source_id: str,
    table_name: str,
    column_names: list[str],
    *,
    ttl: int,
    pagination: PaginationConfig | None = None,
) -> ApiEndpoint:
    """The endpoint table ``table_name`` (an entity, as QuickBooks or its table spells it) is
    read through, with ``column_names`` of its columns (all of them when empty)."""
    try:
        read = entity(table_name)
    except KeyError as missing:
        raise QuickBooksTableRefused(
            "quickbooks.unknown_entity", str(missing.args[0]), table=table_name
        ) from None
    catalog = {c.name: c for c in columns_of(read)}
    unknown = [n for n in column_names if n not in catalog]
    if unknown:
        raise QuickBooksTableRefused(
            "quickbooks.unknown_column",
            f"QuickBooks {read.name} has no column {', '.join(unknown)}",
            table=table_name,
            columns=", ".join(unknown),
        )
    chosen = column_names or list(catalog)
    return ApiEndpoint(
        source_id=source_id,
        path=QUERY_PATH,
        method="GET",
        table_name=table_name,
        columns=[ApiColumn(name=n, type=catalog[n].type) for n in chosen],
        ttl=ttl,
        pagination=table_paging(pagination),
        query_template=entity_query(read),
        response_normalizer=NORMALIZER,
    )


def build_api_source(settings: QuickBooksSettings) -> ApiSource:
    return ApiSource(
        id=settings.source_id,
        type=ApiSourceType.quickbooks,
        base_url=settings.base_url,
        auth=settings.auth(),
    )


async def persist_quickbooks_table(
    conn: "Connection",
    settings: QuickBooksSettings,
    model: Any,
) -> tuple[ApiSource, ApiEndpoint]:
    """Write the rows registered table ``model`` is read through; return them, for the caller to
    mirror into live state."""
    from provisa.api_source.openapi_endpoint import store_openapi_auth
    from provisa.api_source.persist import persist_api_endpoint, persist_api_source

    endpoint = build_endpoint(
        settings.source_id,
        model.table_name,
        [c.name for c in model.columns],
        ttl=model.cache_ttl or 300,
        pagination=getattr(model, "pagination", None),
    )
    api_source = build_api_source(settings)
    await persist_api_source(conn, api_source)
    await store_openapi_auth(conn, api_source.id, settings.auth().model_dump())
    await persist_api_endpoint(conn, endpoint)
    return api_source, endpoint
