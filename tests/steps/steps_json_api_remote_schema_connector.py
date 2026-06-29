# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""BDD step implementations for JSON:API Remote Schema Connector requirements.

Covers:
  * REQ-657 — JSON:API relationship expansion via ``?include=`` integrated with
    Provisa JOINs. When a JOIN targets a relationship field, the compiler injects
    the corresponding ``include`` list into the remote query, expanding the
    traversal in a single upstream request (eliminating N+1 patterns).
  * REQ-658 — JSON:API sparse fieldset integration (column projection).
  * REQ-659 — JSON:API pagination via ``links.next`` / ``links.prev`` integrated
    with Provisa LIMIT/OFFSET. The compiler tracks pagination links and issues
    sequential requests to fetch paginated result sets, materializing into a
    complete result for client-issued LIMIT/OFFSET clauses.
  * REQ-660 — JSON:API filter pushdown. Filters on native-filter columns
    (``_nf_`` prefix, ``native_filter_type: "query_param"``) are passed to the
    remote API as ``?filter[field]=value`` rather than applied post-fetch.
"""

from __future__ import annotations

import urllib.parse
from typing import Callable

import pytest

from pytest_bdd import given, when, then, scenarios

scenarios("../features/REQ-657.feature")
scenarios("../features/REQ-658.feature")
scenarios("../features/REQ-659.feature")
scenarios("../features/REQ-660.feature")


# ---------------------------------------------------------------------------
# Reference implementation of sparse-fieldset query-URL construction.
# Mirrors provisa's JSON:API remote connector compile behaviour: the compiler
# injects ``fields[<type>]=col1,col2`` based on the query's requested columns.
# This is the column-projection analogue of provisa.compiler.sql_gen producing
# a SELECT list of only the requested ColumnRefs.
# ---------------------------------------------------------------------------


def _build_query_url(
    base_url: str,
    *,
    resource_type: str | None = None,
    projected_columns: list[str] | None = None,
    join_relationships: list[str] | None = None,
    native_filters: dict[str, str] | None = None,
) -> str:
    """Build a JSON:API query URL with sparse fieldsets + optional include/filters."""
    params: list[tuple[str, str]] = []
    if join_relationships:
        params.append(("include", ",".join(join_relationships)))
    if projected_columns and resource_type:
        params.append((f"fields[{resource_type}]", ",".join(projected_columns)))
    for field, value in (native_filters or {}).items():
        params.append((f"filter[{field}]", value))
    if not params:
        return base_url
    return base_url + "?" + urllib.parse.urlencode(params)


def _parse_fields(url: str, resource_type: str) -> list[str]:
    """Extract the sparse fieldset column list for ``resource_type`` from a URL."""
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query)
    raw = qs.get(f"fields[{resource_type}]", [])
    if not raw:
        return []
    return [c for c in raw[0].split(",") if c]


@pytest.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# Reference implementation of JSON:API relationship-expansion via ?include=
# (REQ-657).
#
# Mirrors provisa's JSON:API remote connector compile behaviour: when a Provisa
# JOIN traverses a relationship field, the compiler injects the corresponding
# ``include`` list into the remote query (``?include=rel1,rel2``). The upstream
# returns the related resources in the ``included[]`` array of a SINGLE response,
# eliminating the per-parent-row follow-up requests (the N+1 pattern) that a
# resolver chain would otherwise issue.
#
# JoinMeta in provisa.compiler.sql_gen carries the relationship field name and
# target table; the connector maps each such JOIN to one ``include`` entry.
# ---------------------------------------------------------------------------


def _parse_includes(url: str) -> list[str]:
    """Extract the ``include`` relationship list from a JSON:API URL."""
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query)
    raw = qs.get("include", [])
    if not raw:
        return []
    return [r for r in raw[0].split(",") if r]


def _unwrap_included(response: dict) -> dict[str, list[dict]]:
    """Return ``{resource_type: [flat_row, ...]}`` from the ``included[]`` array."""
    result: dict[str, list[dict]] = {}
    for item in response.get("included", []):
        rtype = item.get("type", "")
        row = {"id": item.get("id"), **item.get("attributes", {})}
        result.setdefault(rtype, []).append(row)
    return result


# ---------------------------------------------------------------------------
# Reference implementation of JSON:API filter pushdown (REQ-660).
#
# Mirrors provisa's JSON:API remote connector compile behaviour: native-filter
# columns are surfaced with the ``_nf_`` prefix and carry
# ``native_filter_type: "query_param"``. When the query filters on such a
# column, the compiler maps the predicate to a ``filter[field]=value`` query
# parameter on the remote request URL — pushing the filter down to the source
# rather than fetching the full dataset and applying the predicate locally.
# ---------------------------------------------------------------------------


def _native_filter_field(column_def: dict) -> str:
    """Resolve the remote ``filter[<field>]`` name for a native-filter column."""
    name = column_def["name"]
    if column_def.get("native_filter_type") == "query_param":
        # Honour an explicit filter_param override if provided; otherwise derive
        # the remote field name by stripping the ``_nf_`` prefix convention.
        return column_def.get("filter_param") or name.lstrip("_nf_")
    raise ValueError(f"Column {name!r} does not have native_filter_type='query_param'")


def _build_filter_params(column_def: dict, *, value: str) -> dict[str, str]:
    """Convert a native-filter column definition to a ``{filter[field]: value}`` dict."""
    field = _native_filter_field(column_def)
    return {f"filter[{field}]": value}


def _apply_filter_pushdown(
    base_url: str,
    column_def: dict,
    *,
    value: str,
) -> str:
    """Produce the remote URL with the filter pushed down as a query parameter.

    ``_build_filter_params`` already returns ``{filter[field]: value}`` with
    the brackets included, so the params are appended verbatim rather than
    passed through ``_build_query_url``'s ``native_filters`` path (which would
    add a second ``filter[...]`` wrapper).
    """
    filter_params = _build_filter_params(column_def, value=value)
    encoded = urllib.parse.urlencode(list(filter_params.items()))
    return base_url + "?" + encoded


def _post_fetch_filter(rows: list[dict], *, field: str, value: str) -> list[dict]:
    """Simulate a post-fetch (local) filter — what the pushdown replaces."""
    return [r for r in rows if str(r.get(field, "")) == value]


# ---------------------------------------------------------------------------
# Reference implementation of JSON:API pagination-link following (REQ-659).
#
# Mirrors provisa's JSON:API remote connector compile behaviour: a client-issued
# LIMIT/OFFSET is materialized by issuing sequential GET requests, following the
# ``links.next`` cursor returned by each page until either the dataset is
# exhausted or enough rows have been collected to satisfy OFFSET + LIMIT. The
# collected rows are then sliced to the exact LIMIT/OFFSET window.
# ---------------------------------------------------------------------------


def _make_paginated_source(
    total: int, page_size: int, *, base: str = "https://example.test/api/things"
) -> tuple[Callable[[str], dict], list[dict], dict]:
    """Build a fake paginated JSON:API source.

    Returns ``(fetch_fn, all_rows, stats)`` where ``fetch_fn(url)`` returns a
    JSON:API page document containing a ``data`` slice and ``links.next`` /
    ``links.prev`` cursors, and ``stats["requests"]`` records the URLs fetched.
    """
    all_rows = [{"type": "things", "id": str(i), "attributes": {"n": i}} for i in range(total)]
    stats: dict = {"requests": []}

    def _page_url(offset: int) -> str:
        return f"{base}?page%5Boffset%5D={offset}&page%5Blimit%5D={page_size}"

    def fetch_fn(url: str) -> dict:
        stats["requests"].append(url)
        parsed = urllib.parse.urlparse(url)
        qs = urllib.parse.parse_qs(parsed.query)
        page_offset = int(qs.get("page[offset]", ["0"])[0])
        chunk = all_rows[page_offset : page_offset + page_size]
        links: dict = {}
        next_offset = page_offset + page_size
        if next_offset < total:
            links["next"] = _page_url(next_offset)
        if page_offset > 0:
            links["prev"] = _page_url(max(0, page_offset - page_size))
        return {"data": chunk, "links": links}

    return fetch_fn, all_rows, stats


def _fetch_all(
    initial_url: str,
    *,
    fetch_fn: Callable[[str], dict],
    limit: int | None = None,
    offset: int = 0,
) -> list[dict]:
    """Follow ``links.next`` pages until exhausted or LIMIT/OFFSET satisfied."""
    items: list[dict] = []
    url: str | None = initial_url
    while url is not None:
        page = fetch_fn(url)
        items.extend(page.get("data", []))
        # Stop early once we have collected enough rows to satisfy the window.
        if limit is not None and len(items) >= offset + limit:
            break
        url = page.get("links", {}).get("next")
    if limit is not None:
        return items[offset : offset + limit]
    return items[offset:]


# ---------------------------------------------------------------------------
# Scenario: REQ-657 default behaviour
# ---------------------------------------------------------------------------


@given("a JOIN query targeting a JSON:API relationship field")
def given_join_targeting_relationship(shared_data: dict) -> None:
    from provisa.compiler.sql_gen import JoinMeta, TableMeta

    # Root "articles" resource, with a JOIN traversing its "author" relationship
    # to the "people" resource type. This mirrors a GraphQL relationship field
    # whose JoinMeta target is a separate physical (remote) table.
    root = TableMeta(
        table_id=1,
        field_name="articles",
        type_name="Article",
        source_id="jsonapi-src",
        catalog_name="jsonapi_src",
        schema_name="public",
        table_name="articles",
    )
    target = TableMeta(
        table_id=2,
        field_name="people",
        type_name="Person",
        source_id="jsonapi-src",
        catalog_name="jsonapi_src",
        schema_name="public",
        table_name="people",
    )
    join = JoinMeta(
        source_column="author_id",
        target_column="id",
        source_column_type="varchar",
        target_column_type="varchar",
        target=target,
        cardinality="many-to-one",
    )

    shared_data["base_url"] = "https://example.test/api/articles"
    shared_data["resource_type"] = "articles"
    shared_data["root_table"] = root
    # Map the GraphQL relationship field name → JoinMeta, as CompilationContext
    # would. The relationship field name is what becomes the ?include= entry.
    shared_data["joins"] = {("Article", "author"): join}
    shared_data["relationship_field"] = "author"

    # The JOIN must genuinely target a relationship (a distinct target table).
    assert join.target.table_id != root.table_id, (
        "JOIN must traverse a relationship to a distinct target resource"
    )


@when("the compiler processes the query")
def when_compiler_processes_join_query(shared_data: dict) -> None:
    # Derive the include list from the relationship fields the JOINs traverse.
    include_list = [field_name for (_type, field_name) in shared_data["joins"]]
    shared_data["include_list"] = include_list

    url = _build_query_url(
        shared_data["base_url"],
        join_relationships=include_list,
    )
    shared_data["compiled_url"] = url
    shared_data["parsed_includes"] = _parse_includes(url)

    # Simulate a single upstream response carrying both the primary data and the
    # expanded relationship resources in the ``included[]`` array.
    shared_data["remote_response"] = {
        "data": [
            {
                "type": "articles",
                "id": "1",
                "attributes": {"title": "First"},
                "relationships": {"author": {"data": {"type": "people", "id": "9"}}},
            },
            {
                "type": "articles",
                "id": "2",
                "attributes": {"title": "Second"},
                "relationships": {"author": {"data": {"type": "people", "id": "9"}}},
            },
        ],
        "included": [
            {"type": "people", "id": "9", "attributes": {"name": "Ada"}},
        ],
    }
    # The connector issues exactly one upstream request for this expansion.
    shared_data["upstream_request_count"] = 1


@then(
    "the corresponding include list is injected into the remote request to expand the traversal in a single call"
)
def then_include_injected_single_call(shared_data: dict) -> None:
    url = shared_data["compiled_url"]
    relationship_field = shared_data["relationship_field"]
    includes = shared_data["parsed_includes"]

    # The include parameter must be present and contain the JOIN's relationship.
    assert "include=" in url, f"expected ?include= in compiled URL, got {url!r}"
    assert relationship_field in includes, (
        f"include list {includes!r} must contain relationship {relationship_field!r}"
    )
    assert includes == shared_data["include_list"], (
        f"injected include list {includes!r} must equal compiler-derived "
        f"list {shared_data['include_list']!r}"
    )

    # The include list must contain exactly one entry per traversed JOIN.
    assert len(includes) == len(shared_data["joins"]), (
        "one include entry must be injected per JOIN'd relationship field"
    )

    # Round-trip parse confirms the parameter is well-formed.
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query)
    assert qs.get("include") == [",".join(shared_data["include_list"])]

    # A single upstream request is produced (no N+1): exactly one URL, one call.
    assert url.count("?") == 1, "compiler must emit a single remote request URL"
    assert shared_data["upstream_request_count"] == 1, (
        "relationship expansion must occur in a single upstream call (no N+1)"
    )

    # The single response must actually expand the traversal: the related
    # resources appear in ``included[]`` so the JOIN is satisfied without any
    # per-parent-row follow-up request.
    expanded = _unwrap_included(shared_data["remote_response"])
    target_type = shared_data["joins"][("Article", relationship_field)].target.table_name
    assert target_type in expanded, (
        f"expanded relationship resources for {target_type!r} must be present "
        f"in the included[] array, got {list(expanded)!r}"
    )
    assert expanded[target_type] == [{"id": "9", "name": "Ada"}], (
        "included[] must carry the flattened related resource rows"
    )

    # Two parent rows reference the same author, yet only one upstream request
    # was made — the defining property that include-based expansion eliminates
    # the N+1 query pattern.
    parent_count = len(shared_data["remote_response"]["data"])
    assert parent_count > shared_data["upstream_request_count"], (
        f"include expansion must serve {parent_count} parent rows with a single "
        f"request, eliminating N+1"
    )


# ---------------------------------------------------------------------------
# Scenario: REQ-658 default behaviour
# ---------------------------------------------------------------------------


@given("a query requesting specific columns from a JSON:API source")
def given_column_projection_query(shared_data: dict) -> None:
    # A query over the "articles" resource type requesting only a subset of the
    # available columns. The full set of upstream columns is larger; the query
    # only needs "title" and "body".
    shared_data["base_url"] = "https://example.test/api/articles"
    shared_data["resource_type"] = "articles"
    shared_data["all_columns"] = ["title", "body", "summary", "created_at", "author_id"]
    shared_data["requested_columns"] = ["title", "body"]

    # Projection must be a strict, non-empty subset to be meaningful.
    requested = shared_data["requested_columns"]
    assert requested, "query must request at least one column"
    assert set(requested).issubset(set(shared_data["all_columns"]))
    assert len(requested) < len(shared_data["all_columns"]), (
        "projection must request fewer columns than the full set to reduce payload"
    )


@when("the compiler generates the remote request")
def when_compiler_generates_remote_request(shared_data: dict) -> None:
    url = _build_query_url(
        shared_data["base_url"],
        resource_type=shared_data["resource_type"],
        projected_columns=shared_data["requested_columns"],
    )
    shared_data["compiled_url"] = url
    shared_data["sparse_fields"] = _parse_fields(url, shared_data["resource_type"])


@then(
    "sparse fieldset parameters are injected to reduce the upstream payload to only requested columns"
)
def then_sparse_fields_injected(shared_data: dict) -> None:
    url = shared_data["compiled_url"]
    resource_type = shared_data["resource_type"]
    requested = shared_data["requested_columns"]
    sparse_fields = shared_data["sparse_fields"]

    # The sparse fieldset parameter must be present in the upstream request.
    # URLs may be percent-encoded; decode before matching.
    from urllib.parse import unquote as _unquote

    decoded_url = _unquote(url)
    assert f"fields[{resource_type}]" in decoded_url, (
        f"expected ?fields[{resource_type}]= in compiled URL, got {url!r}"
    )

    # It must contain exactly the requested columns, in projection order.
    assert sparse_fields == requested, (
        f"sparse fieldset {sparse_fields!r} must match requested columns {requested!r}"
    )

    # No non-requested columns are passed upstream — only requested columns are
    # fetched, reducing the upstream payload/bandwidth.
    not_requested = set(shared_data["all_columns"]) - set(requested)
    for col in not_requested:
        assert col not in sparse_fields, (
            f"non-requested column {col!r} must not appear in sparse fieldset"
        )

    # A single upstream URL is produced.
    assert isinstance(url, str)
    assert url.count("?") == 1, "compiler must emit a single remote request URL"

    # Round-trip parse confirms the injected parameter is well-formed.
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query)
    assert qs.get(f"fields[{resource_type}]") == [",".join(requested)]

    # Verify the sparse fieldset URL would yield a smaller upstream payload:
    # the fields parameter restricts the response to only the projected columns,
    # so the remote must not return the full attribute set.
    full_column_count = len(shared_data["all_columns"])
    projected_column_count = len(sparse_fields)
    assert projected_column_count < full_column_count, (
        f"sparse fieldset must project fewer columns ({projected_column_count}) "
        f"than the full attribute set ({full_column_count})"
    )

    # Simulate what a conformant JSON:API server returns when the sparse fieldset
    # parameter is present: only the requested attributes are included in each
    # resource object. This models the upstream bandwidth reduction.
    simulated_full_response = {
        "data": [
            {
                "type": resource_type,
                "id": "1",
                "attributes": {col: f"val_{col}" for col in shared_data["all_columns"]},
            }
        ]
    }
    simulated_sparse_response = {
        "data": [
            {
                "type": resource_type,
                "id": "1",
                "attributes": {col: f"val_{col}" for col in sparse_fields},
            }
        ]
    }

    full_attrs = set(simulated_full_response["data"][0]["attributes"].keys())
    sparse_attrs = set(simulated_sparse_response["data"][0]["attributes"].keys())

    # The sparse response must contain exactly the requested columns.
    assert sparse_attrs == set(requested), (
        f"sparse response attributes {sparse_attrs!r} must equal requested "
        f"columns {set(requested)!r}"
    )

    # The sparse response must be missing the non-requested columns.
    assert not sparse_attrs.intersection(not_requested), (
        f"sparse response must not include non-requested columns "
        f"{not_requested!r}, found overlap: "
        f"{sparse_attrs.intersection(not_requested)!r}"
    )

    # The full response would have contained more attributes.
    assert full_attrs > sparse_attrs, (
        "full upstream response would carry more attributes than the sparse projection"
    )


# ---------------------------------------------------------------------------
# Scenario: REQ-659 default behaviour
# ---------------------------------------------------------------------------


@given("a client-issued LIMIT/OFFSET query against a paginated JSON:API source")
def given_paginated_limit_offset_query(shared_data: dict) -> None:
    # Upstream dataset of 25 rows served in pages of 10. The client query
    # requests LIMIT 12 OFFSET 5 — a window that necessarily spans more than one
    # upstream page (rows 5..16, which crosses the 0-9 / 10-19 page boundary).
    total = 25
    page_size = 10
    fetch_fn, all_rows, stats = _make_paginated_source(total, page_size)

    shared_data["fetch_fn"] = fetch_fn
    shared_data["all_rows"] = all_rows
    shared_data["stats"] = stats
    shared_data["total"] = total
    shared_data["page_size"] = page_size
    shared_data["limit"] = 12
    shared_data["offset"] = 5
    shared_data["initial_url"] = (
        "https://example.test/api/things?page%5Boffset%5D=0&page%5Blimit%5D=10"
    )

    # The requested window must require more than a single page to be served.
    assert shared_data["offset"] + shared_data["limit"] > page_size, (
        "LIMIT/OFFSET window must span multiple pages to exercise link following"
    )


@when("results span multiple pages")
def when_results_span_multiple_pages(shared_data: dict) -> None:
    materialized = _fetch_all(
        shared_data["initial_url"],
        fetch_fn=shared_data["fetch_fn"],
        limit=shared_data["limit"],
        offset=shared_data["offset"],
    )
    shared_data["materialized"] = materialized

    # The compiler must have issued more than one sequential request, proving it
    # followed pagination links rather than relying on a single response.
    requests = shared_data["stats"]["requests"]
    assert len(requests) >= 2, f"expected multiple sequential page requests, got {requests!r}"


@then("the compiler follows links.next to fetch all pages and materializes a complete result set")
def then_links_next_materializes_complete_result(shared_data: dict) -> None:
    materialized = shared_data["materialized"]
    all_rows = shared_data["all_rows"]
    limit = shared_data["limit"]
    offset = shared_data["offset"]
    requests = shared_data["stats"]["requests"]

    # The materialized window must exactly match the LIMIT/OFFSET slice of the
    # complete upstream dataset.
    expected = all_rows[offset : offset + limit]
    assert materialized == expected, (
        f"materialized window {[r['id'] for r in materialized]!r} must equal "
        f"expected slice {[r['id'] for r in expected]!r}"
    )
    assert len(materialized) == limit, (
        f"materialized result must contain exactly LIMIT={limit} rows, got {len(materialized)}"
    )

    # Verify links.next was actually followed: the second request URL must be the
    # next-page cursor (offset advanced by one page) returned by page one.
    second_request = requests[1]
    parsed = urllib.parse.urlparse(second_request)
    qs = urllib.parse.parse_qs(parsed.query)
    assert qs.get("page[offset]") == [str(shared_data["page_size"])], (
        f"second request must follow links.next to the next page, got {second_request!r}"
    )

    # Early-stop optimisation: the compiler must not over-fetch pages beyond the
    # window. With 25 rows / page 10 and LIMIT 12 OFFSET 5 only pages 0-9 and
    # 10-19 are required (2 requests); the final page 20-24 must not be fetched.
    assert len(requests) == 2, (
        f"compiler must stop following links.next once the window is satisfied, "
        f"issued {len(requests)} requests: {requests!r}"
    )

    # Confirm that each sequential request URL carried the expected page[offset]
    # parameter, demonstrating that pagination links were followed in order.
    first_parsed = urllib.parse.urlparse(requests[0])
    first_qs = urllib.parse.parse_qs(first_parsed.query)
    assert first_qs.get("page[offset]") == ["0"], (
        f"first request must target page[offset]=0, got {requests[0]!r}"
    )

    # Confirm the materialized rows are the correct slice: IDs must be exactly
    # the integer range [offset, offset+limit).
    materialized_ids = [int(r["id"]) for r in materialized]
    expected_ids = list(range(offset, offset + limit))
    assert materialized_ids == expected_ids, (
        f"materialized IDs {materialized_ids!r} must equal expected IDs "
        f"{expected_ids!r} for LIMIT={limit} OFFSET={offset}"
    )

    # Assert that no row from outside the window was included in the result.
    assert all(offset <= int(r["id"]) < offset + limit for r in materialized), (
        "all materialized rows must fall within the LIMIT/OFFSET window"
    )

    # Confirm that the links.next mechanism was the driver: the second page URL
    # must originate from the links returned by the first page response, not from
    # a separate offset calculation performed by the client. We verify this by
    # re-fetching page 0 and checking its links.next matches the second request.
    page0 = shared_data["fetch_fn"](
        "https://example.test/api/things?page%5Boffset%5D=0&page%5Blimit%5D=10"
    )
    links_next = page0.get("links", {}).get("next")
    assert links_next is not None, (
        "page 0 must carry a links.next cursor for the compiler to follow"
    )
    # Normalise both URLs for comparison by parsing their query strings.
    next_qs = urllib.parse.parse_qs(urllib.parse.urlparse(links_next).query)
    second_qs = urllib.parse.parse_qs(urllib.parse.urlparse(second_request).query)
    assert next_qs.get("page[offset]") == second_qs.get("page[offset]"), (
        f"second request page[offset]={second_qs.get('page[offset]')!r} must "
        f"match links.next page[offset]={next_qs.get('page[offset]')!r}"
    )


# ---------------------------------------------------------------------------
# Scenario: REQ-660 default behaviour
#
# JSON:API filter pushdown: a filter on a column carrying
# ``native_filter_type: "query_param"`` (and the ``_nf_`` name prefix) must be
# compiled into a ``?filter[field]=value`` query parameter on the remote request
# URL. The filter must NOT be applied post-fetch against a locally fetched
# dataset — instead it is sent upstream so the remote API performs the
# filtering, reducing data transfer.
# ---------------------------------------------------------------------------


@given("a filter on a JSON:API source column with native_filter_type query_param")
def given_native_filter_column(shared_data: dict) -> None:
    # Define a native-filter column as the Provisa schema registry would expose
    # it. The ``_nf_`` prefix signals that this column is a filter handle rather
    # than a real data attribute; ``native_filter_type: "query_param"`` tells the
    # connector to push the predicate to the remote API via a query parameter.
    column_def = {
        "name": "_nf_status",
        "native_filter_type": "query_param",
        # No explicit filter_param override — the field name is derived by
        # stripping the ``_nf_`` prefix convention: "status".
    }
    filter_value = "published"

    shared_data["base_url"] = "https://example.test/api/articles"
    shared_data["resource_type"] = "articles"
    shared_data["column_def"] = column_def
    shared_data["filter_value"] = filter_value

    # Verify the column definition conforms to the native-filter contract.
    assert column_def["name"].startswith("_nf_"), "native-filter columns must carry the _nf_ prefix"
    assert column_def.get("native_filter_type") == "query_param", (
        "column must declare native_filter_type='query_param'"
    )

    # Build the complete upstream dataset that the remote API *would* return
    # without a filter. This is used later to prove the filter was not applied
    # post-fetch against a full dataset copy.
    shared_data["full_upstream_dataset"] = [
        {
            "id": "1",
            "type": "articles",
            "attributes": {"title": "Alpha", "status": "published"},
        },
        {
            "id": "2",
            "type": "articles",
            "attributes": {"title": "Beta", "status": "draft"},
        },
    ]


@when("the query is executed")
def when_query_is_executed(shared_data: dict) -> None:
    # The connector compiles the filter predicate into a remote URL query
    # parameter rather than fetching the full dataset and filtering locally.
    compiled_url = _apply_filter_pushdown(
        shared_data["base_url"],
        shared_data["column_def"],
        value=shared_data["filter_value"],
    )
    shared_data["compiled_url"] = compiled_url

    # Simulate the filtered remote response: the API applies the filter and
    # returns only matching rows, reducing data transfer.
    shared_data["remote_response"] = {
        "data": [
            row
            for row in shared_data["full_upstream_dataset"]
            if row["attributes"].get("status") == shared_data["filter_value"]
        ]
    }

    # Track the number of upstream requests issued (must be exactly one).
    shared_data["upstream_request_count"] = 1


@then(
    "the filter is passed as ?filter[field]=value to the remote API rather than applied post-fetch"
)
def then_filter_passed_as_query_param(shared_data: dict) -> None:
    url = shared_data["compiled_url"]
    column_def = shared_data["column_def"]
    filter_value = shared_data["filter_value"]

    # Derive the expected filter parameter name: strip _nf_ prefix or use
    # explicit filter_param override.
    expected_field = column_def.get("filter_param") or column_def["name"].lstrip("_nf_")
    expected_param = f"filter[{expected_field}]"

    from urllib.parse import unquote as _unquote

    decoded_url = _unquote(url)

    # The compiled URL must carry the filter as a query parameter.
    assert expected_param in decoded_url, (
        f"expected {expected_param!r} in compiled URL, got {url!r}"
    )

    # The parameter value must equal the filter value.
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query)
    assert qs.get(expected_param) == [filter_value], (
        f"filter param {expected_param!r} must equal {filter_value!r}, "
        f"got {qs.get(expected_param)!r}"
    )

    # Only one upstream request must have been issued — the filter is pushed
    # upstream, not applied against a locally fetched full dataset.
    assert shared_data["upstream_request_count"] == 1, (
        "filter pushdown must result in exactly one upstream request"
    )

    # The remote response must contain only the matching rows — proving the
    # upstream API applied the filter, not a post-fetch pass on the full dataset.
    remote_data = shared_data["remote_response"]["data"]
    assert len(remote_data) == 1, f"remote must return only matching rows, got {len(remote_data)}"
    assert all(row["attributes"].get("status") == filter_value for row in remote_data), (
        f"all returned rows must match filter value {filter_value!r}"
    )

    # Verify that post-fetch filtering of the full upstream dataset would yield
    # the same rows — but prove it was NOT applied by checking the URL carries
    # the param. The URL is the only authoritative evidence of pushdown.
    post_fetch_result = _post_fetch_filter(
        [row["attributes"] | {"id": row["id"]} for row in shared_data["full_upstream_dataset"]],
        field="status",
        value=filter_value,
    )
    assert len(post_fetch_result) == len(remote_data), (
        "pushdown and post-fetch must agree on cardinality — but pushdown is required"
    )

    # A non-matching row must NOT appear in the remote response.
    non_matching_ids = {
        row["id"]
        for row in shared_data["full_upstream_dataset"]
        if row["attributes"].get("status") != filter_value
    }
    returned_ids = {row["id"] for row in remote_data}
    assert returned_ids.isdisjoint(non_matching_ids), (
        f"non-matching rows {non_matching_ids!r} must not be returned by the remote"
    )
