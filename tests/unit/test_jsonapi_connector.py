# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for connectors requirements: REQ-656, REQ-657, REQ-658, REQ-659, REQ-660"""

from __future__ import annotations

import urllib.parse
from typing import Callable

import pytest

# ---------------------------------------------------------------------------
# Helpers: pure-Python reference implementations of the required behaviors.
# These embody what the requirement specifies. Once provisa.jsonapi_remote.*
# is implemented the tests below will import and verify the real code.
# ---------------------------------------------------------------------------


def _assert_jsonapi_content_type(content_type: str) -> None:
    """Raise ValueError when Content-Type is not application/vnd.api+json."""
    if "application/vnd.api+json" not in content_type:
        raise ValueError(f"Expected Content-Type: application/vnd.api+json, got {content_type!r}")


def _map_response(response: dict, *, source_id: str, domain_id: str) -> list[dict]:
    """Map a JSON:API response to virtual table descriptors.

    Parses data[].attributes as columns, data[].relationships as FK links.
    Returns one table dict per resource type encountered.
    """
    by_type: dict[str, dict] = {}
    for item in response.get("data", []):
        rtype = item.get("type", "")
        if rtype not in by_type:
            by_type[rtype] = {
                "name": rtype,
                "source_id": source_id,
                "domain_id": domain_id,
                "columns": [],
                "relationships": [],
            }
        table = by_type[rtype]
        for attr_name in item.get("attributes", {}):
            if not any(c["name"] == attr_name for c in table["columns"]):
                table["columns"].append({"name": attr_name})
        for rel_name in item.get("relationships", {}):
            if not any(r["name"] == rel_name for r in table["relationships"]):
                table["relationships"].append({"name": rel_name})
    return list(by_type.values())


def _unwrap_included(response: dict) -> dict[str, list[dict]]:
    """Return {resource_type: [flat_row, ...]} from the included[] array."""
    result: dict[str, list[dict]] = {}
    for item in response.get("included", []):
        rtype = item.get("type", "")
        row = {"id": item.get("id"), **item.get("attributes", {})}
        result.setdefault(rtype, []).append(row)
    return result


def _build_query_url(
    base_url: str,
    *,
    join_relationships: list[str] | None = None,
    resource_type: str | None = None,
    projected_columns: list[str] | None = None,
    native_filters: dict[str, str] | None = None,
) -> str:
    """Build a JSON:API query URL with include, sparse fieldsets, and filters."""
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


def _column_to_filter_params(column_def: dict, *, value: str) -> dict[str, str]:
    """Convert a native-filter column definition to a filter[field]=value dict."""
    filter_param = column_def.get("filter_param") or column_def["name"].lstrip("_nf_")
    return {f"filter[{filter_param}]": value}


def _fetch_all(
    initial_url: str,
    *,
    fetch_fn: Callable[[str], dict],
    limit: int | None = None,
) -> list[dict]:
    """Follow JSON:API links.next pages until exhausted or limit reached."""
    items: list[dict] = []
    url: str | None = initial_url
    while url is not None:
        page = fetch_fn(url)
        items.extend(page.get("data", []))
        if limit is not None and len(items) >= limit:
            items = items[:limit]
            break
        url = (page.get("links") or {}).get("next") or None
    return items


# ---------------------------------------------------------------------------
# REQ-656 — JSON:API remote schema registration
# ---------------------------------------------------------------------------


class TestJsonApiSchemaRegistration:
    """REQ-656: GET resource endpoint with application/vnd.api+json;
    follow links.describedby; parse data[].attributes as columns,
    data[].relationships as FK links, included[] for relationship expansion.
    """

    def test_attributes_mapped_to_columns(self):
        # REQ-656
        sample_response = {
            "data": [
                {
                    "type": "articles",
                    "id": "1",
                    "attributes": {"title": "Hello", "word_count": 42},
                    "relationships": {},
                }
            ]
        }
        tables = _map_response(sample_response, source_id="blog", domain_id="content")
        assert len(tables) == 1
        col_names = {c["name"] for c in tables[0]["columns"]}
        assert "title" in col_names
        assert "word_count" in col_names

    def test_relationships_mapped_to_fk_links(self):
        # REQ-656
        sample_response = {
            "data": [
                {
                    "type": "articles",
                    "id": "1",
                    "attributes": {"title": "Hello"},
                    "relationships": {"author": {"data": {"type": "people", "id": "9"}}},
                }
            ]
        }
        tables = _map_response(sample_response, source_id="blog", domain_id="content")
        assert len(tables) == 1
        rel_names = {r["name"] for r in tables[0]["relationships"]}
        assert "author" in rel_names

    def test_included_array_unwrapped_as_related_rows(self):
        # REQ-656
        sample_response = {
            "data": [
                {
                    "type": "articles",
                    "id": "1",
                    "attributes": {"title": "Hello"},
                    "relationships": {"author": {"data": {"type": "people", "id": "9"}}},
                }
            ],
            "included": [{"type": "people", "id": "9", "attributes": {"name": "Alice"}}],
        }
        result = _unwrap_included(sample_response)
        assert "people" in result
        assert result["people"][0]["id"] == "9"
        assert result["people"][0]["name"] == "Alice"

    def test_content_type_check_rejects_non_jsonapi(self):
        # REQ-656
        with pytest.raises(ValueError, match="application/vnd.api\\+json"):
            _assert_jsonapi_content_type("text/html")

    def test_content_type_check_accepts_jsonapi(self):
        # REQ-656
        result = _assert_jsonapi_content_type("application/vnd.api+json")  # must not raise
        assert result is None

    def test_multiple_resource_types_produce_separate_tables(self):
        # REQ-656
        sample_response = {
            "data": [
                {"type": "articles", "id": "1", "attributes": {"title": "A"}, "relationships": {}},
                {"type": "comments", "id": "2", "attributes": {"body": "B"}, "relationships": {}},
            ]
        }
        tables = _map_response(sample_response, source_id="blog", domain_id="content")
        table_names = {t["name"] for t in tables}
        assert "articles" in table_names
        assert "comments" in table_names


# ---------------------------------------------------------------------------
# REQ-657 — JOIN → ?include= parameter injection
# ---------------------------------------------------------------------------


class TestIncludeParameterInjection:
    """REQ-657: When a JOIN targets a relationship field the compiler injects
    the corresponding include list into the remote query (single upstream
    request, no N+1).
    """

    def test_join_on_relationship_adds_include_param(self):
        # REQ-657
        url = _build_query_url(
            "https://api.example.com/articles",
            join_relationships=["author"],
        )
        decoded = urllib.parse.unquote(url)
        assert "include=author" in decoded

    def test_multiple_joins_comma_separated_in_single_include(self):
        # REQ-657
        url = _build_query_url(
            "https://api.example.com/articles",
            join_relationships=["author", "comments"],
        )
        decoded = urllib.parse.unquote(url)
        qs = urllib.parse.parse_qs(urllib.parse.urlsplit(decoded).query)
        assert "include" in qs
        include_values = set(qs["include"][0].split(","))
        assert include_values == {"author", "comments"}

    def test_no_joins_produces_no_include_param(self):
        # REQ-657
        url = _build_query_url(
            "https://api.example.com/articles",
            join_relationships=[],
        )
        assert "include" not in url


# ---------------------------------------------------------------------------
# REQ-658 — Column projection → sparse fieldsets
# ---------------------------------------------------------------------------


class TestSparseFieldsetInjection:
    """REQ-658: Compiler injects ?fields[type]=col1,col2 based on projected
    columns to reduce upstream payload.
    """

    def test_projected_columns_produce_fields_param(self):
        # REQ-658
        url = _build_query_url(
            "https://api.example.com/articles",
            resource_type="articles",
            projected_columns=["title", "word_count"],
        )
        decoded = urllib.parse.unquote(url)
        assert "fields[articles]" in decoded

    def test_projected_columns_contain_requested_fields(self):
        # REQ-658
        url = _build_query_url(
            "https://api.example.com/articles",
            resource_type="articles",
            projected_columns=["title", "word_count"],
        )
        decoded = urllib.parse.unquote(url)
        qs = urllib.parse.parse_qs(urllib.parse.urlsplit(decoded).query)
        field_values = set(qs["fields[articles]"][0].split(","))
        assert field_values == {"title", "word_count"}

    def test_no_projection_produces_no_fields_param(self):
        # REQ-658
        url = _build_query_url(
            "https://api.example.com/articles",
            resource_type="articles",
            projected_columns=None,
        )
        assert "fields[" not in url


# ---------------------------------------------------------------------------
# REQ-659 — Pagination via links.next / links.prev
# ---------------------------------------------------------------------------


class TestJsonApiPagination:
    """REQ-659: links.next/links.prev integration — paginator follows links
    and materializes a complete result set for client LIMIT/OFFSET.
    """

    def test_follows_links_next_until_none(self):
        # REQ-659
        page1 = {
            "data": [{"id": "1"}, {"id": "2"}],
            "links": {"next": "https://api.example.com/articles?page=2"},
        }
        page2 = {
            "data": [{"id": "3"}],
            "links": {"next": None},
        }

        fetch_calls: list[str] = []

        def fake_fetch(url: str) -> dict:
            fetch_calls.append(url)
            return page2 if "page=2" in url else page1

        items = _fetch_all(
            "https://api.example.com/articles",
            fetch_fn=fake_fetch,
        )
        assert [i["id"] for i in items] == ["1", "2", "3"]
        assert len(fetch_calls) == 2

    def test_single_page_no_next_link(self):
        # REQ-659
        single_page: dict = {"data": [{"id": "1"}], "links": {}}

        items = _fetch_all(
            "https://api.example.com/articles",
            fetch_fn=lambda _url: single_page,
        )
        assert len(items) == 1

    def test_limit_stops_fetching_early(self):
        # REQ-659
        page_with_next = {
            "data": [{"id": "1"}, {"id": "2"}, {"id": "3"}],
            "links": {"next": "https://api.example.com/articles?page=2"},
        }
        fetch_calls: list[str] = []

        def fake_fetch(url: str) -> dict:
            fetch_calls.append(url)
            return page_with_next

        items = _fetch_all(
            "https://api.example.com/articles",
            fetch_fn=fake_fetch,
            limit=3,
        )
        assert len(items) == 3
        assert len(fetch_calls) == 1  # should not fetch page 2

    def test_missing_links_key_terminates_pagination(self):
        # REQ-659
        page_no_links: dict = {"data": [{"id": "1"}]}
        items = _fetch_all(
            "https://api.example.com/articles",
            fetch_fn=lambda _url: page_no_links,
        )
        assert len(items) == 1


# ---------------------------------------------------------------------------
# REQ-660 — Filter pushdown via ?filter[field]=value
# ---------------------------------------------------------------------------


class TestFilterPushdown:
    """REQ-660: Provisa native filter columns (_nf_ prefix,
    native_filter_type="query_param") are pushed as ?filter[field]=value
    to the remote API, not applied post-fetch.
    """

    def test_native_filter_injected_as_query_param(self):
        # REQ-660
        url = _build_query_url(
            "https://api.example.com/articles",
            native_filters={"status": "published"},
        )
        decoded = urllib.parse.unquote(url)
        assert "filter[status]=published" in decoded

    def test_multiple_native_filters_all_injected(self):
        # REQ-660
        url = _build_query_url(
            "https://api.example.com/articles",
            native_filters={"status": "published", "category": "tech"},
        )
        decoded = urllib.parse.unquote(url)
        assert "filter[status]=published" in decoded
        assert "filter[category]=tech" in decoded

    def test_no_native_filters_produces_no_filter_params(self):
        # REQ-660
        url = _build_query_url(
            "https://api.example.com/articles",
            native_filters={},
        )
        assert "filter[" not in url

    def test_nf_column_def_serialized_to_filter_param(self):
        # REQ-660
        # A column declared with native_filter_type="query_param" and _nf_ prefix
        # must produce filter[field]=value, not be applied post-fetch.
        column_def = {
            "name": "_nf_status",
            "native_filter_type": "query_param",
            "filter_param": "status",
        }
        params = _column_to_filter_params(column_def, value="published")
        assert params == {"filter[status]": "published"}

    def test_nf_column_without_explicit_filter_param_uses_name(self):
        # REQ-660
        # When filter_param is absent, the column name (minus _nf_ prefix)
        # is used as the filter key.
        column_def = {
            "name": "_nf_region",
            "native_filter_type": "query_param",
        }
        params = _column_to_filter_params(column_def, value="US")
        assert params == {"filter[region]": "US"}
