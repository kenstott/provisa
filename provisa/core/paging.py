# Copyright (c) 2026 Kenneth Stott
# Canary: e4485e7b-7b53-42ec-bc3b-7da04bbdc12d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How a registered remote table is read page by page: the one representation (REQ-318).

A table's ``pagination`` is authored once, on the table (config, or the admin Paging section),
and every reader takes it from there; the ``api_endpoints`` row a REST table is served from
carries a copy written from it.

- A paged REST endpoint declares its ``type`` and the parameters it pages by, its page size and
  ``max_pages``, the most pages one read takes.
- A Relay connection table (remote GraphQL) pages by the arguments its schema declares, which
  are not set here; it may only set ``max_rows``, the most rows one read takes. The operator's
  ``graphql_remote.max_rows`` is the default and the ceiling: a table may lower it, never raise
  it.
"""

# Requirements: REQ-318, REQ-1350

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator


class PaginationType(str, Enum):  # REQ-318
    link_header = "link_header"
    cursor = "cursor"
    offset = "offset"
    page_number = "page_number"


#: What only a paged REST endpoint declares.
_ENDPOINT_FIELDS = frozenset(
    {"cursor_field", "cursor_param", "page_param", "page_size_param", "page_size", "max_pages"}
)


class PaginationConfig(BaseModel):  # REQ-318
    type: PaginationType | None = None
    cursor_field: str | None = None
    cursor_param: str | None = None
    page_param: str | None = None
    page_size_param: str | None = None
    page_size: int = Field(default=100, ge=1)
    max_pages: int = Field(default=10, ge=1)
    max_rows: int | None = Field(default=None, ge=1)
    # REQ-316: the property of a REST answer its rows sit under (a page wrapper's ``data``,
    # ``values``, ``results``); None when the answer is the rows. It may be declared with no
    # paging type: an answer wrapped and not paged.
    rows_field: str | None = None

    @model_validator(mode="after")
    def _one_kind(self) -> PaginationConfig:
        if self.type is None and self.model_fields_set & _ENDPOINT_FIELDS:
            raise ValueError(
                "pagination: page parameters, page_size and max_pages describe a paged endpoint "
                "and need its type; a connection table sets max_rows only"
            )
        if self.max_rows is not None and (self.type is not None or self.rows_field is not None):
            raise ValueError(
                "pagination: max_rows bounds a connection table; a REST endpoint is bounded by "
                "max_pages and names where its rows are with rows_field"
            )
        return self

    @property
    def is_endpoint(self) -> bool:
        """Whether this describes a REST endpoint -- how it pages, or where its rows are --
        (else a connection table's bound)."""
        return self.type is not None or self.rows_field is not None


class PagingRefused(ValueError):
    """A table's paging that its table cannot take: a stable code, its params, English text."""

    def __init__(self, code: str, params: dict[str, Any], message: str) -> None:
        self.code = code
        self.params = params
        super().__init__(message)


#: What a table's paging describes, by what reads the table.
ENDPOINT = "endpoint"  # a REST endpoint (api_endpoints)
CONNECTION = "connection"  # a Relay connection of a remote GraphQL source


def paging_kind(source_type: str, *, connection: bool | None = None) -> str | None:
    """What reads a table of a ``source_type`` source page by page: :data:`ENDPOINT` for an
    OpenAPI source, :data:`CONNECTION` for a remote GraphQL source's connection table, None for
    any other. ``connection``: whether the GraphQL table is a connection, where that is known
    (its registered spec); a GraphQL table known not to be one pages by nothing."""
    if source_type == "openapi":
        return ENDPOINT
    if source_type == "graphql_remote" and connection is not False:
        return CONNECTION
    return None


def check_paging(
    pagination: PaginationConfig, *, table: str, kind: str | None, ceiling_rows: int
) -> None:
    """Raise :class:`PagingRefused` unless ``table`` can take ``pagination``.

    ``kind`` is what reads the table (:data:`ENDPOINT`, :data:`CONNECTION`), None for a table
    nothing pages. ``ceiling_rows`` is ``graphql_remote.max_rows``: a connection table may lower
    it and never raise it."""
    if kind is None:
        raise PagingRefused(
            "schema.paging_not_paged",
            {"table": table},
            f"table {table!r} is not read page by page, so it takes no pagination",
        )
    if kind == ENDPOINT and not pagination.is_endpoint:
        raise PagingRefused(
            "schema.paging_endpoint_needs_type",
            {"table": table},
            f"table {table!r} is a REST endpoint: its pagination names the paging type, or where "
            "its rows are",
        )
    if kind == CONNECTION and pagination.is_endpoint:
        raise PagingRefused(
            "schema.paging_connection_rows_only",
            {"table": table},
            f"table {table!r} is a connection table: it pages by its schema's arguments and "
            "sets max_rows only",
        )
    if pagination.max_rows is not None and pagination.max_rows > ceiling_rows:
        raise PagingRefused(
            "schema.paging_above_ceiling",
            {"table": table, "max_rows": pagination.max_rows, "ceiling": ceiling_rows},
            f"table {table!r}: max_rows={pagination.max_rows} is above graphql_remote.max_rows="
            f"{ceiling_rows}; a table may lower the operator's bound, never raise it",
        )


def paging_row(pagination: PaginationConfig | None) -> dict[str, Any] | None:
    """The stored form: only what was declared, so a connection table's bound does not come
    back carrying an endpoint's defaults (which only a typed paging may declare)."""
    return None if pagination is None else pagination.model_dump(mode="json", exclude_unset=True)


def stored_paging(row: dict[str, Any] | None) -> PaginationConfig | None:
    """A table's paging read back from its stored form (:func:`paging_row`)."""
    return None if row is None else PaginationConfig.model_validate(row)


def connection_max_rows(pagination: PaginationConfig | None, ceiling_rows: int) -> int:
    """The most rows one read of a connection table takes: the table's own bound where it set
    one (never above the ceiling, refused at save and at load), else ``graphql_remote.max_rows``."""
    if pagination is None or pagination.max_rows is None:
        return ceiling_rows
    return pagination.max_rows
