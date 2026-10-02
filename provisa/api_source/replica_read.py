# Copyright (c) 2026 Kenneth Stott
# Canary: 91c175bd-b80e-47d3-93dc-d1a98017db1f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The whole collection of an API table, read for a replica build (REQ-1915).

A build copies the table in bounded batches, so the collection is never held whole:

- an endpoint that pages is read a page at a time (``caller.iter_api_pages``);
- an endpoint that answers with one document has that document written to a spool file as it
  arrives and its rows parsed from the file one at a time (``federation.replica_spool``).
  Three answer shapes are read that way: plain JSON (a list of items at the endpoint's root
  path, or one object), the Neo4j transaction API's rows, and SPARQL bindings.

An answer that can only be understood whole (a gRPC call, a response normalizer with no
row-at-a-time reader, a root path through an array index) is not read here: the caller reads
it as a single document in memory, which the build reports as not optimal.
"""

# Requirements: REQ-1915

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import aclosing
from typing import IO, Any

import httpx

from provisa.api_source.caller import (
    _MAX_RETRIES,
    _RETRY_BACKOFF_BASE,
    ApiCallError,
    ApiNotFoundError,
    Paging,
    PreparedCall,
    iter_api_pages,
    prepare_call,
)
from provisa.api_source.flattener import flatten_item, flatten_response
from provisa.api_source.models import ApiEndpoint, PaginationType
from provisa.federation.replica_errors import BuildFailure

#: Seconds each connect, read or write of the spooled call may take.
_TIMEOUT = 30.0
_NOT_FOUND = 404
_TOO_MANY = 429
_SERVER_ERROR = 500

#: Paging whose answer does not say whether there is more: a page shorter than the page size
#: is the last.
_SIZED_PAGING = frozenset({PaginationType.offset, PaginationType.page_number})


class PageLimitReached(BuildFailure, ApiCallError):
    """A build read as many pages as the endpoint's paging allows and the endpoint had more.

    A request that reaches the cap gets what was read. A replica is served as the whole table,
    so a build never swaps in a table cut at the cap, and never reads past a cap the operator
    declared: it fails, naming the cap."""

    code = "replication.page_limit_reached"

    def __init__(self, table: str, max_pages: int, rows: int) -> None:
        self.params = {"table": table, "max_pages": max_pages, "rows": rows}
        super().__init__(
            f"the replica of {table} was not built: its endpoint's paging stops at "
            f"max_pages={max_pages}, and the endpoint had more after page {max_pages} "
            f"({rows:,} rows read). A replica is the whole table, so it is not cut at the cap. "
            "Raise the endpoint's max_pages or its page size."
        )


#: The rows of one spooled answer, parsed from its file.
RowReader = Callable[[IO[bytes], ApiEndpoint], Iterator[dict]]


def _plain_rows(body: IO[bytes], endpoint: ApiEndpoint) -> Iterator[dict]:
    """Rows of a plain JSON answer: the items of the list at the endpoint's root path, one at
    a time; or the one object there (a single row, or a map of name to count)."""
    from provisa.federation.replica_spool import json_items, json_starts

    at = endpoint.response_root or ""
    first = json_starts(body, at)
    if first == "start_array":
        for item in json_items(body, f"{at}.item" if at else "item"):
            if isinstance(item, dict):
                yield flatten_item(item, endpoint.columns)
    elif first == "start_map":
        for whole in json_items(body, at):
            yield from flatten_response(whole, None, endpoint.columns)
    elif first is None:
        raise KeyError(f"Cannot navigate path {at!r}: not found in the response")
    else:
        raise ValueError(f"Expected dict or list at root path {at!r}, got {first}")


def _neo4j_rows(body: IO[bytes], endpoint: ApiEndpoint) -> Iterator[dict]:
    """Rows of a Neo4j transaction API answer (``normalizers.neo4j_tabular``): its ``errors``
    first (a bad statement answers 200 with errors, which is a failed read), then each row of
    the statement's one result under that result's column names."""
    from provisa.federation.replica_spool import json_items

    errors = list(json_items(body, "errors.item"))
    if errors:
        raise ValueError(f"neo4j query failed: {errors}")
    results = list(json_items(body, "results.item.columns"))
    if len(results) > 1:
        raise ValueError(
            f"neo4j answered {len(results)} results for the one statement of "
            f"{endpoint.table_name!r}"
        )
    for names in results:
        for values in json_items(body, "results.item.data.item.row"):
            yield flatten_item(dict(zip(names, values)), endpoint.columns)


def _sparql_rows(body: IO[bytes], endpoint: ApiEndpoint) -> Iterator[dict]:
    """Rows of a SPARQL 1.1 SELECT answer (``normalizers.sparql_bindings``): one per binding,
    each variable's term reduced to its value."""
    from provisa.federation.replica_spool import json_items

    for binding in json_items(body, "results.bindings.item"):
        if not isinstance(binding, dict):
            continue
        item = {
            name: term.get("value") if isinstance(term, dict) else term
            for name, term in binding.items()
        }
        yield flatten_item(item, endpoint.columns)


_ROW_READERS: dict[str | None, RowReader] = {
    None: _plain_rows,
    "neo4j_tabular": _neo4j_rows,
    "sparql_bindings": _sparql_rows,
}


def _row_reader(endpoint: ApiEndpoint) -> RowReader | None:
    """How ``endpoint``'s one-document answer is read a row at a time, or None when it has to
    be understood whole."""
    reader = _ROW_READERS.get(endpoint.response_normalizer or None)
    if reader is _plain_rows and any(
        part.isdigit() for part in (endpoint.response_root or "").split(".")
    ):
        return None  # a root path through an array index names one element, not a stream
    return reader


def _spooled_rows(
    endpoint: ApiEndpoint, call: Callable[[], PreparedCall], rows: RowReader, spooled: Any
) -> Iterator[dict]:
    """Send the call, spool its answer, yield its rows. A 429 or 5xx answer is sent again, as
    ``caller._request_with_retry`` does; nothing has been yielded when that happens, since the
    status is known before the first byte is spooled."""
    with httpx.Client(timeout=_TIMEOUT) as client:
        for attempt in range(_MAX_RETRIES):
            sent = call()  # prepared per attempt: an expiring token is fetched again
            try:
                with spooled(
                    lambda sent=sent: client.stream(
                        sent.method,
                        sent.url,
                        params=sent.params,
                        headers=sent.headers,
                        json=sent.json_body,
                        data=sent.form_body,
                    )
                ) as body:
                    yield from rows(body, endpoint)
                    return
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status == _NOT_FOUND:
                    raise ApiNotFoundError(f"404 Not Found: {sent.url}") from exc
                if status != _TOO_MANY and status < _SERVER_ERROR:
                    raise
                if attempt == _MAX_RETRIES - 1:
                    raise ApiCallError(
                        f"API call failed after {_MAX_RETRIES} retries: {status} {sent.url}"
                    ) from exc
                time.sleep(_RETRY_BACKOFF_BASE * (2**attempt))


def replica_source(
    endpoint: ApiEndpoint, api_source: Any, columns: list[tuple[str, str]], *, table: str
) -> Any | None:
    """``endpoint``'s whole collection (its default parameters) as a replica build's source,
    or None when its answer can only be read whole. ``table`` names it in a spool refusal."""
    from provisa.federation.replica_source import CursorSource
    from provisa.federation.replica_spool import SpooledDocumentSource

    if endpoint.method == "RPC":
        return None
    params = dict(endpoint.default_params)
    base_url, auth = api_source.base_url, api_source.auth

    pagination = endpoint.pagination
    if pagination is not None:
        sized = pagination.type in _SIZED_PAGING

        async def row_batches(_batch_rows: int) -> AsyncIterator[list[dict]]:
            paging = Paging()
            pages = total = last = 0
            async with aclosing(
                iter_api_pages(endpoint, params, base_url=base_url, auth=auth, paging=paging)
            ) as answer:
                async for page in answer:
                    rows = flatten_response(
                        page,
                        endpoint.response_root,
                        endpoint.columns,
                        endpoint.response_normalizer,
                    )
                    pages, last, total = pages + 1, len(rows), total + len(rows)
                    if rows:
                        yield rows
                    if sized and last < pagination.page_size:
                        break  # a short page is the last one
            more = last >= pagination.page_size if sized else bool(paging.more)
            if pages >= pagination.max_pages and more:
                raise PageLimitReached(table, pagination.max_pages, total)

        return CursorSource(row_batches, columns)

    rows = _row_reader(endpoint)
    if rows is None:
        return None
    return SpooledDocumentSource(
        lambda spooled: _spooled_rows(
            endpoint, lambda: prepare_call(endpoint, params, base_url, auth), rows, spooled
        ),
        columns,
        table=table,
    )
