# Copyright (c) 2026 Kenneth Stott
# Canary: 82a345f6-921a-4768-820f-15d9b7e2c2ae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Paging suggested from the spec (REQ-318) for a cursor the answer holds deeper than one level
or names in camelCase (REQ-1874: the brands over OpenAPI). The suggestion follows the response
schema to the property named for the request's cursor parameter and records its dotted path."""

# Requirements: REQ-318, REQ-1874

from __future__ import annotations

import logging

import pytest

from provisa.openapi.mapper import parse_spec

_ROW = {"type": "object", "properties": {"id": {"type": "string"}, "name": {"type": "string"}}}
_TEXT = {"type": "string"}


def _obj(**properties) -> dict:
    return {"type": "object", "properties": properties}


def _spec(answer: dict, params: dict[str, str]) -> dict:
    """One list operation answering ``answer`` (its rows under ``data``) to ``params``."""
    return {
        "openapi": "3.0.0",
        "paths": {
            "/things": {
                "get": {
                    "operationId": "listThings",
                    "parameters": [
                        {"name": n, "in": "query", "schema": {"type": t}} for n, t in params.items()
                    ],
                    "responses": {"200": {"content": {"application/json": {"schema": answer}}}},
                }
            }
        },
    }


def _page(**extra) -> dict:
    answer = _obj(data={"type": "array", "items": _ROW}, **extra)
    answer["required"] = ["data"]
    return answer


def _offered(answer: dict, params: dict[str, str]) -> dict | None:
    (query,), _ = parse_spec(_spec(answer, params))
    paging = query.pagination
    return None if paging is None else paging.model_dump(mode="json", exclude_unset=True)


def _cursor(param: str, field: str, size: str | None = None) -> dict:
    declared = {
        "rows_field": "data",
        "type": "cursor",
        "cursor_param": param,
        "cursor_field": field,
    }
    return declared | ({"page_size_param": size} if size else {})


@pytest.mark.parametrize(
    ("brand", "answer", "params", "expected"),
    [
        (
            "calendly",
            _page(pagination=_obj(count={"type": "integer"}, next_page_token=_TEXT)),
            {"page_token": "string", "count": "integer"},
            _cursor("page_token", "pagination.next_page_token", "count"),
        ),
        (
            "orb",
            _page(pagination_metadata=_obj(has_more={"type": "boolean"}, next_cursor=_TEXT)),
            {"cursor": "string", "limit": "integer"},
            _cursor("cursor", "pagination_metadata.next_cursor", "limit"),
        ),
        (
            "slack",
            _page(ok={"type": "boolean"}, response_metadata=_obj(next_cursor=_TEXT)),
            {"cursor": "string", "limit": "integer"},
            _cursor("cursor", "response_metadata.next_cursor", "limit"),
        ),
        (
            "hubspot",
            _page(paging=_obj(next=_obj(after=_TEXT, link=_TEXT))),
            {"after": "string", "limit": "integer"},
            _cursor("after", "paging.next.after", "limit"),
        ),
        (
            "calcom",
            _page(pagination=_obj(totalItems={"type": "integer"}, nextCursor=_TEXT)),
            {"cursor": "string", "limit": "integer"},
            _cursor("cursor", "pagination.nextCursor", "limit"),
        ),
        (
            "camelCase at the top",
            _page(nextCursor=_TEXT),
            {"cursor": "string"},
            _cursor("cursor", "nextCursor"),
        ),
        (
            "camelCase parameter, snake_case answer",
            _page(meta=_obj(next_page_token=_TEXT)),
            {"pageToken": "string", "pageSize": "integer"},
            _cursor("pageToken", "meta.next_page_token", "pageSize"),
        ),
        (
            "notion",
            _page(
                has_more={"type": "boolean"},
                next_cursor={"type": ["string", "null"]},
                object=_TEXT,
            ),
            {"start_cursor": "string", "page_size": "integer"},
            _cursor("start_cursor", "next_cursor", "page_size"),
        ),
        (
            "two holders deep",
            _page(meta=_obj(pagination=_obj(next_cursor=_TEXT))),
            {"cursor": "string"},
            _cursor("cursor", "meta.pagination.next_cursor"),
        ),
    ],
)  # fmt: skip
def test_a_nested_or_camel_case_cursor_is_offered_by_its_path(brand, answer, params, expected):
    assert _offered(answer, params) == expected, brand


def test_the_one_level_next_property_still_offered():
    answer = _page(next_page=_TEXT)
    assert _offered(answer, {"page": "string", "limit": "integer"}) == _cursor(
        "page", "next_page", "limit"
    )


def test_a_cursor_inside_a_referenced_holder_is_found():
    spec = _spec({"$ref": "#/components/schemas/Page"}, {"cursor": "string", "limit": "integer"})
    spec["components"] = {
        "schemas": {
            "Page": _page(pagination={"$ref": "#/components/schemas/Meta"}),
            "Meta": _obj(next_cursor=_TEXT),
        }
    }
    (query,), _ = parse_spec(spec)
    assert query.pagination.model_dump(mode="json", exclude_unset=True) == _cursor(
        "cursor", "pagination.next_cursor", "limit"
    )


def test_two_places_naming_the_next_cursor_are_not_guessed_between(caplog):
    answer = _page(next_cursor=_TEXT, response_metadata=_obj(next_cursor=_TEXT))
    with caplog.at_level(logging.WARNING, logger="provisa.openapi.mapper"):
        assert _offered(answer, {"cursor": "string", "limit": "integer"}) == {"rows_field": "data"}
    (record,) = caplog.records
    assert "GET /things" in record.getMessage()
    assert "next_cursor" in record.getMessage()
    assert "response_metadata.next_cursor" in record.getMessage()


def test_snake_and_camel_spellings_side_by_side_are_two_candidates(caplog):
    answer = _page(pagination=_obj(next_cursor=_TEXT, nextCursor=_TEXT))
    with caplog.at_level(logging.WARNING, logger="provisa.openapi.mapper"):
        assert _offered(answer, {"cursor": "string"}) == {"rows_field": "data"}
    assert "pagination.nextCursor" in caplog.records[0].getMessage()


def test_two_parameters_each_with_a_next_value_are_not_guessed_between(caplog):
    answer = _page(next_page=_TEXT, next_cursor=_TEXT)
    with caplog.at_level(logging.WARNING, logger="provisa.openapi.mapper"):
        assert _offered(answer, {"page": "string", "cursor": "string"}) == {"rows_field": "data"}
    assert caplog.records


def test_an_object_named_next_for_the_parameter_is_not_a_cursor():
    # A `next_page` holding a link object is where the next page is described, not its value.
    answer = _page(next_page=_obj(href=_TEXT))
    assert _offered(answer, {"page": "integer", "per_page": "integer"}) == {
        "rows_field": "data",
        "type": "page_number",
        "page_param": "page",
        "page_size_param": "per_page",
    }


def test_the_rows_are_not_searched_for_a_cursor():
    row = _obj(id=_TEXT, next_cursor=_TEXT)
    answer = _obj(data={"type": "array", "items": row}, meta=_obj(total={"type": "integer"}))
    answer["required"] = ["data"]
    assert _offered(answer, {"cursor": "string"}) == {"rows_field": "data"}
