# Copyright (c) 2026 Kenneth Stott
# Canary: 3d6f8b12-a47e-4c09-b5d3-61e9f0c2a874
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A QuickBooks table's pages, read through the company's query endpoint (REQ-2197).

``GET <base>/query?query=<query> STARTPOSITION <n> MAXRESULTS <m>``, a page of the endpoint's
page size at a time, from row 1, until a page comes back short, the statement's pushed LIMIT is
read, or the endpoint's ``max_pages`` is reached (then the answer is cut, as any API answer's
is). The query is the statement's narrowed one where it gave conditions
(``quickbooks.query``), else the table's own.

An access token QuickBooks refuses (401) is dropped and the page asked for once more with a new
one; a second refusal fails the read. Any other refusal fails it with QuickBooks' own reason.
"""

# Requirements: REQ-2197
from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import httpx

from provisa.api_source.caller import ApiCallError, _request_with_retry
from provisa.api_source.models import ApiEndpoint
from provisa.core.secrets import resolve_secrets
from provisa.quickbooks.query import MAX_PAGE, MAX_ROWS_ARG, QUERY_ARG, page_query
from provisa.quickbooks.rows import records

#: The Accounting API minor version every query is answered in, so an answer has the fields
#: the entity catalog names whatever the company's default is.
MINOR_VERSION = "75"
_DEFAULT_PAGE = 100


class QuickBooksRefused(ApiCallError):
    """QuickBooks refused a query: its own reason, never the credential."""


@dataclass
class Read:
    """What a read knows after each page: whether the table had more when it stopped."""

    more: bool | None = None


def _fault(resp: httpx.Response) -> str:
    """QuickBooks' reason for refusing a query: its Fault's errors, else the status alone."""
    try:
        body = resp.json()
    except ValueError:
        return f"HTTP {resp.status_code}"
    fault = body.get("Fault") if isinstance(body, dict) else None
    if not isinstance(fault, dict):
        return f"HTTP {resp.status_code}"
    said = [
        ": ".join(str(e[k]) for k in ("Message", "Detail") if e.get(k))
        for e in fault.get("Error") or []
        if isinstance(e, dict)
    ]
    return f"HTTP {resp.status_code}: {'; '.join(s for s in said if s) or fault.get('type')}"


def page_size(endpoint: ApiEndpoint) -> int:
    paging = endpoint.pagination
    return min(paging.page_size if paging is not None else _DEFAULT_PAGE, MAX_PAGE)


def max_pages(endpoint: ApiEndpoint) -> int | None:
    return endpoint.pagination.max_pages if endpoint.pagination is not None else None


async def pages(
    endpoint: ApiEndpoint,
    params: dict,
    base_url: str,
    token: Callable[[], Awaitable[str]],
    forget: Callable[[], None],
    *,
    timeout: float,
    read: Read | None = None,
) -> AsyncGenerator[dict, None]:
    """The answered pages of ``endpoint``'s query, one at a time. ``token`` answers the access
    token to send; ``forget`` drops it when QuickBooks refuses it."""
    if not endpoint.query_template:
        raise ApiCallError(f"{endpoint.table_name}: a QuickBooks table has no query")
    query = str(params.get(QUERY_ARG) or endpoint.query_template)
    bound = params.get(MAX_ROWS_ARG)
    size = page_size(endpoint)
    most = max_pages(endpoint)
    url = resolve_secrets(base_url).rstrip("/") + "/" + endpoint.path.lstrip("/")
    taken = 0
    start = 1
    pages_read = 0
    async with httpx.AsyncClient() as client:
        while True:
            want = size if bound is None else min(size, int(bound) - taken)
            if want <= 0:
                if read is not None:
                    read.more = False
                return
            page = await _page(client, url, page_query(query, start, want), token, forget, timeout)
            pages_read += 1
            got = len(records(page))
            taken += got
            short = got < want
            if read is not None:
                read.more = not short and (bound is None or taken < int(bound))
            yield page
            if short or (most is not None and pages_read >= most):
                return
            start += got


async def _page(
    client: httpx.AsyncClient,
    url: str,
    query: str,
    token: Callable[[], Awaitable[str]],
    forget: Callable[[], None],
    timeout: float,
) -> dict:
    params = {"query": query, "minorversion": MINOR_VERSION}
    for attempt in range(2):
        headers = {"Authorization": f"Bearer {await token()}", "Accept": "application/json"}
        try:
            resp = await _request_with_retry(client, "GET", url, params, headers, timeout=timeout)
        except httpx.HTTPStatusError as refused:
            if refused.response.status_code == 401 and attempt == 0:
                forget()
                continue
            raise QuickBooksRefused(
                f"QuickBooks refused {query!r}: {_fault(refused.response)}"
            ) from None
        return await asyncio.get_running_loop().run_in_executor(None, resp.json)
    raise QuickBooksRefused(f"QuickBooks refused {query!r}: HTTP 401")  # pragma: no cover


def source_token(
    endpoint: ApiEndpoint, base_url: str, auth: Any
) -> tuple[Callable[[], Awaitable[str]], Callable[[], None]]:
    """How a read of ``endpoint`` gets its access token, and drops it: through the source's
    approval kept in the organisation's state store (``quickbooks.token``)."""
    from provisa.quickbooks import token as qb_token

    if auth is None:
        raise ApiCallError(f"{endpoint.source_id}: a QuickBooks source has no credential")
    realm = resolve_secrets(base_url).rstrip("/").rsplit("/", 1)[-1]

    async def get() -> str:
        return await qb_token.access_token(endpoint.source_id, realm, auth)

    def drop() -> None:
        qb_token.forget(endpoint.source_id, realm)

    return get, drop
