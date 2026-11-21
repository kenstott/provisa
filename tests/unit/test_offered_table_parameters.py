# Copyright (c) 2026 Kenneth Stott
# Canary: 5b8e1f3a-2c47-4d9e-b6a0-7e3c9d1f4a28
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The tables Register Table offers name the parameters each requires (REQ-1986), so the picker
can list a lookup apart from a table that lists rows with none."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from provisa.api.admin._graphql_table_registration import parameter_columns
from provisa.api.admin.introspect import native_tables, openapi_parameter_columns
from provisa.graphql_remote.brands import BRANDS, map_table
from provisa.openapi.mapper import parse_spec


@pytest.fixture(autouse=True)
def _one_organisation(monkeypatch):
    """A deployment of one organization (REQ-2006), said here since no API layer is imported."""
    from provisa.core import secrets_providers

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)


def _shopify_state() -> SimpleNamespace:
    reg = {
        "source_id": "my-shop",
        "url": BRANDS["shopify"].url,
        "namespace": "shop",
        "auth": {},
        "brand": "shopify",
        "tables": [],
    }
    return SimpleNamespace(
        graphql_remote_sources={"my-shop": reg},
        config=SimpleNamespace(graphql_remote=SimpleNamespace(max_rows=10000)),
    )


async def _offered(state, source_id: str, source_type: str, schema: str) -> dict:
    # Neither a pool nor the config store is touched for a source read from its specification.
    tables = await native_tables(source_id, source_type, schema, MagicMock(), MagicMock(), state)
    assert tables is not None
    return {t.name: t for t in tables}


async def test_a_shopify_lookup_names_its_required_id_and_a_connection_names_none():
    offered = await _offered(_shopify_state(), "my-shop", "graphql_remote", "graphql")
    assert offered["shop__company"].required_parameters == ["id"]
    assert offered["shop__companies"].paging_kind == "connection"
    assert offered["shop__companies"].required_parameters == []


async def test_a_lookups_parameters_are_the_ones_its_registration_requires():
    """The list names what the registered table's ``_nf_`` columns require, nothing else."""
    offered = await _offered(_shopify_state(), "my-shop", "graphql_remote", "graphql")
    table = map_table(BRANDS["shopify"], "shop", "my-shop", "", "shop__company")
    required = [c.name for c in parameter_columns(table) if c.native_filter_required]
    assert ["_nf_" + p for p in offered["shop__company"].required_parameters] == required


_SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "t", "version": "1"},
    "paths": {
        "/stores": {
            "get": {
                "operationId": "listStores",
                "parameters": [{"name": "region", "in": "query", "schema": {"type": "string"}}],
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {"id": {"type": "integer"}},
                                    },
                                }
                            }
                        }
                    }
                },
            }
        },
        "/stores/{id}": {
            "get": {
                "operationId": "getStore",
                "parameters": [
                    {"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}},
                    {"name": "lang", "in": "query", "required": True, "schema": {"type": "string"}},
                    {"name": "fields", "in": "query", "schema": {"type": "string"}},
                ],
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"id": {"type": "integer"}},
                                }
                            }
                        }
                    }
                },
            }
        },
    },
}


async def test_an_openapi_operation_names_its_path_and_required_query_parameters():
    state = SimpleNamespace(openapi_specs={"api": {"spec": _SPEC, "base_url": "http://x"}})
    offered = await _offered(state, "api", "openapi", "openapi")
    assert offered["getStore"].required_parameters == ["id", "lang"]
    assert offered["listStores"].required_parameters == []


def test_an_openapi_operations_parameter_columns_say_which_are_required():
    queries, _ = parse_spec(_SPEC)
    q = next(q for q in queries if q.operation_id == "getStore")
    assert [
        (c["name"], c["native_filter_type"], c["native_filter_required"])
        for c in openapi_parameter_columns(q)
    ] == [
        ("_nf_id", "path_param", True),
        ("_nf_lang", "query_param", True),
        ("_nf_fields", "query_param", False),
    ]
