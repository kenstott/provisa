# Copyright (c) 2026 Kenneth Stott
# Canary: 5c0e7a13-2b9f-4d61-9a8e-7f3d1c2e4b90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A list answered as either the rows or a page wrapping them (REQ-1874: the brands over
OpenAPI). Auth0's ``GET /users`` and Clerk's ``GET /v1/users`` declare their answer as ``oneOf``
an array of rows and an object holding that array: the table reads the row's columns, its rows
from the array unless its paging names the wrapper's property."""

# Requirements: REQ-316, REQ-318, REQ-1874

from __future__ import annotations

import logging

import pytest

from provisa.openapi.mapper import NoRowsField, parse_spec

_TEXT = {"type": "string"}
_INT = {"type": "integer"}
_USER = {
    "type": "object",
    "properties": {"user_id": _TEXT, "id": _TEXT, "email": _TEXT, "logins_count": _INT},
}
_ORG = {"type": "object", "properties": {"id": _TEXT, "display_name": _TEXT}}


def _ref(name: str) -> dict:
    return {"$ref": f"#/components/schemas/{name}"}


def _list_of(name: str) -> dict:
    return {"type": "array", "items": _ref(name)}


def _spec(answer: dict, params: dict[str, str], schemas: dict | None = None) -> dict:
    """One ``GET /users`` answering ``answer`` to the query ``params``."""
    return {
        "openapi": "3.1.0",
        "paths": {
            "/users": {
                "get": {
                    "operationId": "getUsers",
                    "parameters": [
                        {"name": n, "in": "query", "schema": {"type": t}} for n, t in params.items()
                    ],
                    "responses": {"200": {"content": {"application/json": {"schema": answer}}}},
                }
            }
        },
        "components": {"schemas": {"User": _USER, "Organization": _ORG, **(schemas or {})}},
    }


# Auth0: ``include_totals=true`` wraps the users with the page's start, limit and total.
_AUTH0 = {
    "oneOf": [
        _list_of("User"),
        {
            "type": "object",
            "properties": {
                "start": {"type": "number"},
                "limit": {"type": "number"},
                "length": {"type": "number"},
                "total": {"type": "number"},
                "users": _list_of("User"),
            },
        },
    ]
}
_AUTH0_PARAMS = {"page": "integer", "per_page": "integer", "include_totals": "boolean"}

# Clerk: ``pagination=cursor`` answers ``{data, cursor}`` in place of the array.
_CLERK = {"oneOf": [_list_of("User"), _ref("UserCursorPage")]}
_CLERK_PAGE = {
    "type": "object",
    "properties": {
        "data": _list_of("User"),
        "cursor": {
            "type": "object",
            "properties": {
                "starting_after": _TEXT,
                "ending_before": _TEXT,
                "has_next_page": {"type": "boolean"},
            },
        },
    },
    "required": ["data", "cursor"],
}
_CLERK_PARAMS = {
    "limit": "integer",
    "offset": "integer",
    "starting_after": "string",
    "pagination": "string",
}


def _query(spec: dict, rows_fields: dict[str, str | None] | None = None):
    queries, _ = parse_spec(spec, rows_fields=rows_fields)
    (query,) = queries
    return query


def _paging(query) -> dict | None:
    paging = query.pagination
    return None if paging is None else paging.model_dump(mode="json", exclude_unset=True)


@pytest.mark.parametrize(
    ("answer", "params", "schemas", "wrapper"),
    [
        (_AUTH0, _AUTH0_PARAMS, None, "users"),
        (_CLERK, _CLERK_PARAMS, {"UserCursorPage": _CLERK_PAGE}, "data"),
    ],
    ids=["auth0", "clerk"],
)
def test_list_or_page_reads_the_rows_columns(answer, params, schemas, wrapper):
    query = _query(_spec(answer, params, schemas))
    assert not query.columns_declared
    assert query.is_list
    assert set(query.response_schema["properties"]) == set(_USER["properties"])
    assert query.returns_type == "User"
    # The rows are the array, as the default call answers; the wrapper is recorded beside it.
    assert query.rows_field is None
    assert query.wrapped_rows_field == wrapper


def test_auth0_pages_the_array_by_page_number():
    query = _query(_spec(_AUTH0, _AUTH0_PARAMS))
    assert _paging(query) == {
        "type": "page_number",
        "page_param": "page",
        "page_size_param": "per_page",
    }


def test_clerk_pages_the_array_after_the_last_rows_id():
    query = _query(_spec(_CLERK, _CLERK_PARAMS, {"UserCursorPage": _CLERK_PAGE}))
    assert _paging(query) == {
        "type": "last_row",
        "cursor_param": "starting_after",
        "cursor_field": "id",
        "page_size_param": "limit",
    }


@pytest.mark.parametrize(
    ("answer", "params", "schemas", "wrapper"),
    [
        (_AUTH0, _AUTH0_PARAMS, None, "users"),
        (_CLERK, _CLERK_PARAMS, {"UserCursorPage": _CLERK_PAGE}, "data"),
    ],
    ids=["auth0", "clerk"],
)
def test_paging_that_names_the_wrapper_reads_the_wrapped_rows(answer, params, schemas, wrapper):
    query = _query(_spec(answer, params, schemas), {"getUsers": wrapper})
    assert query.rows_field == wrapper
    assert query.wrapped_rows_field == wrapper
    assert query.is_list
    assert set(query.response_schema["properties"]) == set(_USER["properties"])
    assert query.returns_type == "User"
    assert query.pagination is not None
    assert query.pagination.rows_field == wrapper


def test_clerk_wrapped_answer_holds_its_cursor():
    """Read wrapped, the answer searched for the next page's cursor is the wrapper."""
    page = {**_CLERK_PAGE, "properties": {**_CLERK_PAGE["properties"], "next_cursor": _TEXT}}
    query = _query(
        _spec(_CLERK, {"limit": "integer", "cursor": "string"}, {"UserCursorPage": page}),
        {"getUsers": "data"},
    )
    assert _paging(query) == {
        "rows_field": "data",
        "type": "cursor",
        "cursor_param": "cursor",
        "cursor_field": "next_cursor",
        "page_size_param": "limit",
    }


def test_paging_that_names_no_wrapper_property_is_refused():
    with pytest.raises(NoRowsField):
        _query(_spec(_AUTH0, _AUTH0_PARAMS), {"getUsers": "people"})


@pytest.mark.parametrize("keyword", ["oneOf", "anyOf"])
def test_members_of_different_row_types_give_no_columns(keyword, caplog):
    answer = {
        keyword: [
            _list_of("User"),
            {"type": "object", "properties": {"total": _INT, "users": _list_of("Organization")}},
        ]
    }
    with caplog.at_level(logging.WARNING, logger="provisa.openapi.mapper"):
        query = _query(_spec(answer, _AUTH0_PARAMS))
    assert query.columns_declared
    assert query.response_schema is None
    assert any(
        "User" in r.getMessage() and "Organization" in r.getMessage() for r in caplog.records
    )


def test_plain_array_is_unchanged():
    query = _query(_spec(_list_of("User"), _AUTH0_PARAMS))
    assert query.is_list
    assert query.rows_field is None
    assert query.wrapped_rows_field is None
    assert set(query.response_schema["properties"]) == set(_USER["properties"])
    assert query.returns_type == "User"
    assert _paging(query) == {
        "type": "page_number",
        "page_param": "page",
        "page_size_param": "per_page",
    }
