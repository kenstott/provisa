# Copyright (c) 2026 Kenneth Stott
# Canary: f912261d-dbda-4025-83cd-f63d504ab859
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Execute queries against a remote GraphQL endpoint (REQ-309)."""

from __future__ import annotations

from provisa.security import sandbox_egress  # REQ-2006
import asyncio
import json
import logging
import re
import time
from collections.abc import AsyncGenerator, Iterator
from contextlib import aclosing
from dataclasses import dataclass
from typing import Any

import httpx

from provisa.core.statement_warnings import ServerWarning, warn
from provisa.federation.replica_errors import BuildFailure
from provisa.graphql_remote.introspect import _build_headers

# Requirements: REQ-307, REQ-309, REQ-310, REQ-313

log = logging.getLogger(__name__)


def _safe_json(resp: httpx.Response) -> dict:
    """Parse JSON response, fixing lone \\u escapes that aren't valid JSON."""
    try:
        return resp.json()
    except json.JSONDecodeError:
        # Some servers emit backslash-u not followed by 4 hex digits (e.g. Windows paths).
        fixed = re.sub(r"(?<!\\)\\u(?![0-9a-fA-F]{4})", r"\\\\u", resp.text)
        return json.loads(fixed)


_OBJECT_FIELD_RE = re.compile(r"Field '([^']+)' of type '[^']+' must have a selection of subfields")

# Matches a column selection that aliases a single-subfield object projection down to a scalar,
# e.g. ``employee_id: employee { id }`` — the alias is the sql column name, the braces hold exactly
# one subfield. Rows come back keyed by the alias but holding a nested {subfield: value} dict; this
# flattens it to the scalar the column is declared as, per the alias's own selection contract.
_SCALAR_PROJECTION_RE = re.compile(r"^(\w+):\s*\w+\s*\{\s*(\w+)\s*\}$")


def _object_fields_from_errors(errors: list) -> set[str]:
    """Extract field names that require subfield selection from GQL error list."""
    found: set[str] = set()
    for err in errors:
        m = _OBJECT_FIELD_RE.search(str(err.get("message", "")))
        if m:
            found.add(m.group(1))
    return found


def _flatten_scalar_projections(rows: list[dict], columns: list[str]) -> None:
    """Unwrap ``alias: field { subfield }`` projections in place from {subfield: v} to v."""
    projections = [m.groups() for c in columns if (m := _SCALAR_PROJECTION_RE.match(c.strip()))]
    if not projections:
        return
    for row in rows:
        if not isinstance(row, dict):
            continue
        for alias, subfield in projections:
            v = row.get(alias)
            if isinstance(v, dict):
                row[alias] = v.get(subfield)


def column_selection(column: dict) -> str:
    """What a table column selects on the remote. A nested object carries its own
    ``gql_selection``. Otherwise the store lands under the sql name and the remote keys the field
    by its GraphQL name, both from the naming authority: where they differ the field is aliased
    ``<sql_name>: <gqlField>`` so the response comes back keyed as the store expects."""
    from provisa.compiler.naming import apply_gql_name, apply_sql_name

    if column.get("gql_selection"):
        return column["gql_selection"]
    sql_name = apply_sql_name(column["name"])
    gql_field = apply_gql_name(column["name"])
    return gql_field if sql_name == gql_field else f"{sql_name}: {gql_field}"


def _field_query(
    field_name: str,
    columns: list[str],
    required_args: list[dict],
    extra_args: list[str],
) -> str:
    """The query for a table read off a root field directly. ``required_args`` are passed as
    variables; ``extra_args`` are literal ``name: value`` pairs."""
    col_selection = "\n".join(columns) if columns else "__typename"
    arg_pass = ", ".join([f"{a['name']}: ${a['name']}" for a in required_args] + extra_args)
    root = f"{field_name}({arg_pass})" if arg_pass else field_name
    var_decls = ", ".join(f"${a['name']}: {a['gql_type']}" for a in required_args)
    head = f"query({var_decls})" if var_decls else "query"
    return f"{head} {{ {root} {{ {col_selection} }} }}"


def table_query(table: dict, columns: list[str], page_size: int = 1) -> str:
    """The query a table's read sends, with every required argument supplied -- the same text
    :func:`execute_remote` builds, for a caller that needs the query without running it."""
    required = table.get("required_args") or []
    if table.get("rows_path"):
        return _connection_query(
            table["field_name"], columns, table["rows_path"], required, page_size
        )
    return _field_query(table["field_name"], columns, required, [])


_CURSOR_VAR = "pageCursor"


def _connection_query(
    field_name: str,
    columns: list[str],
    rows_path: list[str],
    required_args: list[dict],
    page_size: int,
) -> str:
    """The query for one page of a connection table (REQ-309).

    ``rows_path`` ends in the connection's row list -- ``nodes``, or ``edges``/``node`` -- and any
    element before that is the connection field on the object the root field returns. The page
    arguments go on the connection field itself, which is the root field when nothing precedes
    the row list.
    """
    by_edges = rows_path[-2:] == ["edges", "node"]
    holders = rows_path[: -2 if by_edges else -1]
    col_selection = "\n".join(columns) if columns else "__typename"
    rows = f"edges {{ node {{ {col_selection} }} }}" if by_edges else f"nodes {{ {col_selection} }}"
    body = f"{rows} pageInfo {{ hasNextPage endCursor }}"
    page_args = f"first: {page_size}, after: ${_CURSOR_VAR}"
    root_args = [f"{a['name']}: ${a['name']}" for a in required_args]
    for holder in reversed(holders):
        body = f"{holder}({page_args}) {{ {body} }}"
        page_args = ""
    arg_pass = ", ".join(root_args + ([page_args] if page_args else []))
    var_decls = ", ".join(
        [f"${a['name']}: {a['gql_type']}" for a in required_args] + [f"${_CURSOR_VAR}: String"]
    )
    root = f"{field_name}({arg_pass})" if arg_pass else field_name
    return f"query({var_decls}) {{ {root} {{ {body} }} }}"


def error_code(error: dict) -> str | None:
    """The code a GraphQL error carries: its ``type`` (GitHub), or ``extensions.code``, where the
    GraphQL spec places it (Shopify)."""
    code = error.get("type")
    if code is None:
        code = (error.get("extensions") or {}).get("code")
    return code if isinstance(code, str) else None


@dataclass(frozen=True)
class ErrorPolicy:
    """What a source declares about the errors its remote returns (REQ-1923). A source that
    declares nothing has every error fail the read."""

    # Returned beside the data for one field of one row the remote could not give this
    # credential: the field is null in that row and the rest of the read stands.
    row_field: frozenset[str] = frozenset()
    # Returned when a page asks for more than the remote will compute in one query: the page is
    # asked for again at half the size.
    overload: frozenset[str] = frozenset()
    # The same, for a remote that reports it with an untyped error: how that error's message
    # starts.
    overload_messages: tuple[str, ...] = ()
    # Returned when the remote's rate limit has too little left for the query: the query is sent
    # again once the limit has restored what the query costs, as the answer's
    # ``extensions.cost`` states it (:func:`_throttle_wait`).
    throttled: frozenset[str] = frozenset()

    def overloaded_by(self, errors: list[dict]) -> list[str]:
        """What among ``errors`` says the page asked for too much; empty when nothing does."""
        said = {c for e in errors if (c := error_code(e)) in self.overload}
        said |= {
            prefix
            for e in errors
            for prefix in self.overload_messages
            if error_code(e) is None and str(e.get("message", "")).startswith(prefix)
        }
        return sorted(said)

    def throttled_by(self, errors: list[dict]) -> bool:
        """Whether ``errors`` say the remote's rate limit has too little left for the query."""
        return any(error_code(e) in self.throttled for e in errors)


# A remote that limits request rate answers 403 or 429 and says in Retry-After how many
# seconds to wait (GitHub's secondary rate limit), or answers with an error its source declares
# as throttled and states in ``extensions.cost`` what the query costs and how fast the limit
# restores (Shopify). The request is sent again after that wait, this many times at most, and
# only when the wait asked for is no longer than this.
_RETRY_AFTER_STATUSES = (403, 429)
_RETRY_AFTER_ATTEMPTS = 3
_RETRY_AFTER_MAX_SECONDS = 120


class Throttled(ValueError):
    """The remote's rate limit would not run the query in the waits a read allows. ``reason``
    is the remote's own account: its errors and what its ``extensions.cost`` says."""

    def __init__(self, url: str, reason: str):
        super().__init__(f"{url}: the remote's rate limit did not let the query run: {reason}")
        self.reason = reason


def _throttle_wait(body: dict) -> float | None:
    """Seconds until the rate limit stated in a GraphQL answer's ``extensions.cost`` has restored
    what the query costs: ``(requestedQueryCost - currentlyAvailable) / restoreRate``. None when
    the answer states no such limit."""
    cost = (body.get("extensions") or {}).get("cost") or {}
    status = cost.get("throttleStatus") or {}
    requested = cost.get("requestedQueryCost")
    available = status.get("currentlyAvailable")
    rate = status.get("restoreRate")
    if not (
        isinstance(requested, (int, float))
        and isinstance(available, (int, float))
        and isinstance(rate, (int, float))
        and rate > 0
    ):
        return None
    return max(0.0, (requested - available) / rate)


def _wait_asked(resp: httpx.Response, policy: ErrorPolicy) -> tuple[float | None, str]:
    """How long the remote asks to wait before the query is sent again, and what it said; None
    when it asks for no wait it states."""
    wait = resp.headers.get("retry-after", "")
    if resp.status_code in _RETRY_AFTER_STATUSES and wait.isdigit():
        return float(wait), f"HTTP {resp.status_code} Retry-After: {wait}"
    if not policy.throttled or "json" not in resp.headers.get("content-type", ""):
        return None, ""
    body = _safe_json(resp)
    if not isinstance(body, dict) or not policy.throttled_by(body.get("errors") or []):
        return None, ""
    said = f"{body.get('errors')} cost={(body.get('extensions') or {}).get('cost')}"
    computed = _throttle_wait(body)
    if computed is None:
        raise Throttled(str(resp.request.url), f"{said}; it states no wait")
    return computed, said


async def _post(
    client: httpx.AsyncClient,
    url: str,
    payload: dict,
    headers: dict,
    policy: ErrorPolicy | None = None,
) -> httpx.Response:
    """POST the query, waiting out a rate limit the remote puts a time on (``policy`` declares
    which of its errors mean that). A Retry-After refusal asking for a longer wait is returned
    for the caller to raise; a throttled answer that asks for too long a wait, or is still
    throttled after the last attempt, raises :class:`Throttled` naming what the remote said."""
    policy = policy or NO_POLICY
    for attempt in range(_RETRY_AFTER_ATTEMPTS + 1):
        resp = await client.post(url, json=payload, headers=headers)
        wait, said = _wait_asked(resp, policy)
        if wait is None:
            return resp
        if wait > _RETRY_AFTER_MAX_SECONDS or attempt == _RETRY_AFTER_ATTEMPTS:
            if resp.status_code in _RETRY_AFTER_STATUSES:
                return resp
            raise Throttled(url, said)
        log.warning("graphql_remote %s: rate limited (%s); retrying in %.1fs", url, said, wait)
        await asyncio.sleep(wait)
    raise AssertionError("unreachable")  # the last attempt returns or raises


NO_POLICY = ErrorPolicy()


def _accept_row_field_errors(
    errors: list[dict], row_depth: int, tolerated: frozenset[str], table: str
) -> None:
    """Raise unless every error is one a source declares as answering for a single field of a
    single row (REQ-1923).

    A GraphQL response can carry data and errors together: a field that could not be resolved
    comes back null, and its error names the field by ``path``. A source declares the error
    types that mean exactly that for it -- GitHub answers FORBIDDEN for a field this credential
    may not see on this particular row. Such a field is null in the row, which is what the
    remote returned, and the error is logged. An error of any other type, or one whose path is
    the table itself rather than a field inside a row, fails the read.
    """
    for error in errors:
        path = error.get("path") or []
        if error_code(error) not in tolerated or len(path) <= row_depth:
            raise ValueError(f"Remote GraphQL errors: {errors}")
    log.warning(
        "graphql_remote %s: %d field(s) returned null by the remote: %s",
        table,
        len(errors),
        sorted(
            {f"{error_code(e)} {'.'.join(str(p) for p in e['path'][row_depth:])}" for e in errors}
        ),
    )


# The remote's gateway gave up on a page (GitHub answers 502/504 when a query runs past its
# time limit). The same page is asked for again at half the size.
_PAGE_TOO_HEAVY = (502, 504)


@dataclass(frozen=True)
class _ConnectionRead:
    """What stays the same across the pages of one connection read."""

    url: str
    headers: dict
    field_name: str
    columns: list[str]
    rows_path: list[str]
    variables: dict
    required_args: list[dict]  # those the caller supplied a value for
    policy: ErrorPolicy

    @property
    def by_edges(self) -> bool:
        return self.rows_path[-2:] == ["edges", "node"]

    @property
    def holders(self) -> list[str]:
        """The connection field(s) between the root field and the row list."""
        return self.rows_path[: -2 if self.by_edges else -1]

    @property
    def table(self) -> str:
        return ".".join([self.field_name, *self.holders])

    @property
    def row_depth(self) -> int:
        """Path elements that address a row: the root field, the connection, its row list, the
        index (and ``node`` under an edge). A longer error path names a field inside a row."""
        return 1 + len(self.rows_path) + 1


async def _connection_page(
    client: httpx.AsyncClient, read: _ConnectionRead, cursor: str | None, page_size: int
) -> tuple[dict | None, int]:
    """One page of a connection: the connection object (None when the parent is missing) and
    the page size it was served at. A page the remote will not compute at the size asked --
    its gateway gave up, or it answered with one of the source's overload errors -- is asked
    for again at half the size, down to one row."""
    while True:
        query = _connection_query(
            read.field_name, read.columns, read.rows_path, read.required_args, page_size
        )
        payload = {"query": query, "variables": {**read.variables, _CURSOR_VAR: cursor}}
        resp = await _post(client, read.url, payload, read.headers, read.policy)
        data: dict = {}
        answered: object = resp.status_code
        too_heavy = resp.status_code in _PAGE_TOO_HEAVY
        if not too_heavy:
            resp.raise_for_status()
            data = _safe_json(resp)
            answered = read.policy.overloaded_by(data.get("errors") or [])
            too_heavy = bool(answered)
        if too_heavy and page_size > 1:
            page_size = max(1, page_size // 2)
            log.warning(
                "graphql_remote %s: remote answered %s; retrying the page at %d rows",
                read.table,
                answered,
                page_size,
            )
            continue
        if too_heavy:
            raise ValueError(
                f"graphql_remote {read.table}: the remote will not compute even one row of "
                f"this table with the columns selected (it answered {answered}); register the "
                "table with fewer columns"
            )
        if data.get("errors"):
            _accept_row_field_errors(
                data["errors"], read.row_depth, read.policy.row_field, read.table
            )
        connection = (data.get("data") or {}).get(read.field_name)
        for holder in read.holders:
            if connection is None:
                break
            connection = connection.get(holder)
        return connection, page_size


def _page_rows(connection: dict, by_edges: bool) -> list[dict]:
    """The rows of one page. A null node is not a row."""
    if by_edges:
        page = [e.get("node") for e in connection.get("edges") or [] if e]
    else:
        page = connection.get("nodes") or []
    return [r for r in page if isinstance(r, dict)]


class RowLimitReached(BuildFailure, ValueError):
    """A build read ``max_rows`` rows of a connection table and the remote had more.

    A replica is served as the whole table, so a build never swaps in a table cut at the bound:
    it fails, naming it (REQ-1915)."""

    code = "replication.row_limit_reached"

    def __init__(self, table: str, max_rows: int, rows: int) -> None:
        self.params = {"table": table, "max_rows": max_rows, "rows": rows}
        super().__init__(
            f"the replica of {table} was not built: a read of it stops at max_rows={max_rows} "
            f"(graphql_remote.max_rows), and the remote had more after {rows:,} rows. A replica "
            "is the whole table, so it is not cut at the bound. Raise graphql_remote.max_rows."
        )


def answer_row_cut_warning(table: str, max_rows: int) -> ServerWarning:
    """The warning a statement answered from a connection read cut at ``max_rows`` carries
    (REQ-1350)."""
    return ServerWarning(
        code="api.answer_row_cut",
        params={"table": table, "max_rows": max_rows},
        message=(
            f"the answer for {table} was cut at max_rows={max_rows}: the remote has more. "
            "Raise graphql_remote.max_rows to read it all."
        ),
    )


async def connection_pages(  # REQ-309
    read: _ConnectionRead, page_size: int
) -> AsyncGenerator[tuple[list[dict], bool]]:
    """A connection table's pages, in order, following its cursor: each page's rows and whether
    the remote reports a page after it. A missing parent (the root field returned null) has no
    pages. One page is held at a time."""
    cursor: str | None = None
    async with httpx.AsyncClient(
        timeout=30.0, event_hooks=sandbox_egress.http_event_hooks()
    ) as client:
        while True:
            connection, page_size = await _connection_page(client, read, cursor, page_size)
            if connection is None:
                return
            page_info = connection.get("pageInfo") or {}
            more = bool(page_info.get("hasNextPage"))
            yield _page_rows(connection, read.by_edges), more
            if not more:
                return
            cursor = page_info.get("endCursor")


async def bounded_pages(
    read: _ConnectionRead, page_size: int, max_rows: int | None
) -> AsyncGenerator[tuple[list[dict], int | None]]:
    """:func:`connection_pages` up to ``max_rows`` rows: each page's rows (the last one trimmed
    to the bound) and, on the page where the read stops at the bound with more to read, the
    total the remote had at least (``None`` on every other page)."""
    read_so_far = 0
    async with aclosing(connection_pages(read, page_size)) as pages:
        async for rows, more in pages:
            read_so_far += len(rows)
            if (
                max_rows is not None
                and read_so_far >= max_rows
                and (more or read_so_far > max_rows)
            ):
                yield rows[: len(rows) - (read_so_far - max_rows)], read_so_far
                return
            yield rows, None


def _connection_read(
    url: str,
    auth: dict | None,
    field_name: str,
    columns: list[str],
    rows_path: list[str],
    variables: dict | None,
    required_args: list[dict] | None,
    error_policy: ErrorPolicy,
) -> _ConnectionRead:
    return _ConnectionRead(
        url=url,
        headers={"Content-Type": "application/json", **_build_headers(auth)},
        field_name=field_name,
        columns=list(columns),
        rows_path=rows_path,
        variables=variables or {},
        required_args=[a for a in required_args or [] if a["name"] in (variables or {})],
        policy=error_policy,
    )


async def whole_connection(  # REQ-1915, REQ-1923
    url: str,
    auth: dict | None,
    field_name: str,
    columns: list[str],
    rows_path: list[str],
    *,
    table: str,
    page_size: int,
    max_rows: int | None,
    error_policy: ErrorPolicy = NO_POLICY,
    variables: dict | None = None,
    required_args: list[dict] | None = None,
) -> AsyncGenerator[list[dict]]:
    """A connection table's whole collection, a page at a time, for a read that must have all of
    it (a replica build, an event-loop land): the read never holds more than one page, and one
    that reaches ``max_rows`` with more to read fails by name (:class:`RowLimitReached`, naming
    ``table``) instead of yielding a table cut at the bound. ``variables``: the arguments every
    page is read with -- a table's fixed arguments (REQ-2061) -- declared by ``required_args``.
    ``page_size``: the rows asked for a page, the size the table's query was priced at when it
    was registered (``graphql_remote.max_list_items``)."""
    read = _connection_read(
        url, auth, field_name, columns, rows_path, variables, required_args, error_policy
    )
    async with aclosing(bounded_pages(read, page_size, max_rows)) as pages:
        async for page, cut_at in pages:
            if cut_at is not None:
                assert max_rows is not None  # a cut is reported only under a bound
                raise RowLimitReached(table, max_rows, cut_at)
            _flatten_scalar_projections(page, read.columns)
            yield page


@dataclass(frozen=True)
class RemoteAnswer:
    """What a request's read of a remote GraphQL table returned. ``cut``: the read stopped at
    ``max_rows`` with more to read -- the statement carries ``api.answer_row_cut``, and the rows
    are never cached as the table's answer (REQ-1350)."""

    rows: list[dict]
    cut: bool = False


async def _execute_connection(  # REQ-309
    read: _ConnectionRead, page_size: int, max_rows: int | None
) -> RemoteAnswer:
    """A request's read of a connection table, up to ``max_rows`` rows. An answer cut at the
    bound is what the request gets, and it says so (REQ-1350)."""
    rows: list[dict] = []
    cut = False
    async with aclosing(bounded_pages(read, page_size, max_rows)) as pages:
        async for page, cut_at in pages:
            rows.extend(page)
            if cut_at is not None:
                assert max_rows is not None  # a cut is reported only under a bound
                warn(answer_row_cut_warning(read.table, max_rows))
                cut = True
    return RemoteAnswer(rows, cut)


async def execute_remote(  # REQ-309, REQ-307, REQ-310, REQ-313
    url: str,
    auth: dict | None,
    field_name: str,
    columns: list[str],
    variables: dict | None = None,
    required_args: list[dict] | None = None,
    limit: int | None = None,
    offset: int | None = None,
    pagination: dict | None = None,
    rows_path: list[str] | None = None,
    max_rows: int | None = None,
    error_policy: ErrorPolicy = NO_POLICY,
) -> RemoteAnswer:
    """Build a minimal GraphQL query and forward to the remote endpoint.

    Returns the row dicts from data.<field_name> (:class:`RemoteAnswer`).
    Raises httpx.HTTPError on network failure.
    Raises ValueError if the response contains errors.

    If the server rejects OBJECT-type fields (no subselection), retries once
    with those fields removed so scalar columns are still returned.

    When pagination is provided and the remote supports limit/offset args,
    passes them as literal arg values to cap rows at the remote rather than
    fetching all and truncating locally.
    """
    selected_cols = list(columns)
    headers = {"Content-Type": "application/json", **_build_headers(auth)}

    if rows_path:
        # A connection table (mapper._map_connection_table): its rows sit under ``rows_path`` and
        # are read by cursor. ``limit`` is the page size here, ``max_rows`` the bound on the read.
        # A Relay server requires a page size and prices the query by it, so the caller names
        # the one the table was priced at (graphql_remote.max_list_items).
        if limit is None:
            raise ValueError(f"{field_name}: a connection table's read names its page size")
        read = _connection_read(
            url, auth, field_name, selected_cols, rows_path, variables, required_args, error_policy
        )
        answer = await _execute_connection(read, limit, max_rows)
        _flatten_scalar_projections(answer.rows, selected_cols)
        return answer

    pagination_arg_strs: list[str] = []
    if pagination and limit is not None:
        limit_arg = pagination.get("limit_arg")
        if limit_arg:
            pagination_arg_strs.append(f"{limit_arg}: {limit}")
        offset_arg = pagination.get("offset_arg")
        if offset is not None and offset_arg:
            pagination_arg_strs.append(f"{offset_arg}: {offset}")

    data: dict = {}
    async with httpx.AsyncClient(
        timeout=30.0, event_hooks=sandbox_egress.http_event_hooks()
    ) as client:
        for attempt in range(2):
            supplied = [a for a in required_args or [] if a["name"] in (variables or {})]
            query = _field_query(field_name, selected_cols, supplied, pagination_arg_strs)
            payload: dict = {"query": query}
            if variables:
                payload["variables"] = variables
            resp = await _post(client, url, payload, headers, error_policy)
            resp.raise_for_status()
            data = _safe_json(resp)
            if "errors" in data:
                object_fields = _object_fields_from_errors(data["errors"])
                if object_fields and attempt == 0:
                    selected_cols = [c for c in selected_cols if c.split()[0] not in object_fields]
                    continue
                # A row is the root field's value, or an element of it when it is a list.
                value = (data.get("data") or {}).get(field_name)
                _accept_row_field_errors(
                    data["errors"],
                    2 if isinstance(value, list) else 1,
                    error_policy.row_field,
                    field_name,
                )
            break

    rows = (data.get("data") or {}).get(field_name, [])
    rows = rows if isinstance(rows, list) else [rows]
    _flatten_scalar_projections(rows, selected_cols)
    return RemoteAnswer(rows)


def iter_remote_rows_spooled(  # REQ-1915, REQ-1923
    url: str,
    auth: dict | None,
    field_name: str,
    columns: list[str],
    spooled: Any,
    error_policy: ErrorPolicy = NO_POLICY,
) -> Iterator[dict]:
    """The whole-collection read of :func:`execute_remote` (no variables, no paging), with the
    endpoint's answer written to a spool file as it arrives and parsed from the file: first its
    ``errors`` (same one retry without the fields that need a subselection; then the source's
    ``error_policy`` decides, as it does for :func:`execute_remote`), then the rows of
    ``data.<field_name>`` one at a time.

    The document is parsed as the endpoint sent it: the repair :func:`_safe_json` makes to an
    invalid ``\\u`` escape needs the whole text in memory and is not applied. Such an answer
    fails the read by name (``replica_spool.AnswerNotJson``): the table, the parser's reason
    and where in the answer it is."""
    from provisa.federation.replica_spool import json_items, json_starts

    selected = list(columns)
    headers = {"Content-Type": "application/json", **_build_headers(auth)}
    narrowed = False
    throttled = 0
    with httpx.Client(timeout=30.0, event_hooks=sandbox_egress.http_sync_event_hooks()) as client:
        while True:
            selection = "\n".join(selected) if selected else "__typename"
            payload = {"query": f"query {{ {field_name} {{ {selection} }} }}"}
            with spooled(
                lambda: client.stream("POST", url, json=payload, headers=headers)  # noqa: B023
            ) as body:
                errors = list(json_items(body, "errors.item"))
                at = f"data.{field_name}"
                listed = json_starts(body, at) == "start_array"
                if errors and error_policy.throttled_by(errors):
                    # The same wait :func:`_post` honours, read off the spooled answer.
                    answer = {
                        "errors": errors,
                        "extensions": next(json_items(body, "extensions"), {}),
                    }
                    wait = _throttle_wait(answer)
                    said = f"{errors} cost={answer['extensions'].get('cost')}"
                    if (
                        wait is None
                        or wait > _RETRY_AFTER_MAX_SECONDS
                        or throttled == _RETRY_AFTER_ATTEMPTS
                    ):
                        raise Throttled(url, said)
                    throttled += 1
                    log.warning(
                        "graphql_remote %s: rate limited (%s); retrying in %.1fs", url, said, wait
                    )
                    time.sleep(wait)
                    continue
                if errors:
                    object_fields = _object_fields_from_errors(errors)
                    if object_fields and not narrowed:
                        narrowed = True
                        selected = [c for c in selected if c.split()[0] not in object_fields]
                        continue
                    # A row is the root field's value, or an element of it when it is a list.
                    _accept_row_field_errors(
                        errors, 2 if listed else 1, error_policy.row_field, field_name
                    )
                # A list field is read an element at a time; a single object is one row.
                rows = json_items(body, f"{at}.item") if listed else json_items(body, at)
                for row in rows:
                    if row is None:
                        continue
                    _flatten_scalar_projections([row], selected)
                    yield row
            return
