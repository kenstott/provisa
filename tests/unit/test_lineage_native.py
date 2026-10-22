# Copyright (c) 2026 Kenneth Stott
# Canary: 0f6c2d1e-5a3b-4e8f-9c7d-2b1a4e6f8d90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2116: lineage reaches below the registered table to the source itself.

Each source column gains one native node beneath it: the raw field as its source reads it, in a
box for the address it is read from, with the renaming on the way. The hop is shown only to a user
with source registration, and nothing secret is copied.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from provisa.api.admin import lineage_router
from provisa.api.admin.lineage_router import _role_seeds, lineage_graph_for
from provisa.lineage.native import (
    native_address,
    native_table,
    served,
    sql_column_name,
    with_native_hop,
)
from provisa.lineage.graph import LineageGraph, Node

CUSTOMERS = {
    "source_id": "crm",
    "domain_id": "sales",
    "schema_name": "openapi",
    "table_name": "customers",
    "columns": [
        {"column_name": "id", "alias": None, "path": None, "native_filter_type": None},
        {"column_name": "postalCode", "alias": None, "path": None, "native_filter_type": None},
        {"column_name": "_nf_limit", "alias": None, "path": None, "native_filter_type": "int"},
    ],
}
ENDPOINT = {
    "method": "GET",
    "path": "/v2/customers",
    "default_params": {"region": "eu", "api_key": "${secret:CRM_KEY}"},
    "response_root": "data.items",
}


def _source(type_: str = "openapi", **kw) -> SimpleNamespace:
    return SimpleNamespace(
        id=kw.pop("id", "crm"),
        type=type_,
        path=kw.pop("path", None),
        federation_hints=kw.pop("federation_hints", {}),
    )


def _customers_native(**served_kw):
    address = native_address("openapi", CUSTOMERS, _source(), endpoint=ENDPOINT)
    return native_table(
        "sales.customers",
        _source(federation_hints={"brand": "stripe", "client_secret": "s3cr3t"}),
        CUSTOMERS,
        address,
        served(**served_kw) if served_kw else served(False, None, None, None),
    )


def test_an_openapi_table_is_read_from_its_operation_and_row_location() -> None:
    native = _customers_native()
    assert native.address == (
        ("operation", "GET /v2/customers"),
        ("parameters", "api_key, region=eu"),
        ("rows", "data.items"),
    )
    # The raw field is the API's own name; the registered column its SQL name.
    fields = {f.raw: (f.column, f.rename) for f in native.fields}
    assert fields == {"id": ("id", None), "postalCode": ("postal_code", "naming")}


def test_the_view_reading_postal_code_reaches_the_raw_field() -> None:
    sql = "SELECT postal_code FROM sales.report"
    views = [("sales.report", "SELECT c.postal_code FROM sales.customers c")]
    out = lineage_graph_for(
        sql, {}, views=views, natives=[_customers_native()], relations=["sales.customers"]
    )
    native_id = "native:sales.customers.postalCode"
    nodes = {n["id"]: n for n in out["nodes"]}
    assert nodes[native_id]["kind"] == "native"
    assert nodes[native_id]["column"] == "postalCode"
    edge = next(e for e in out["edges"] if e["source"] == native_id)
    assert edge["target"] == "sales.customers.postal_code"
    assert edge["ops"] == [{"name": "naming", "kind": "rename", "args": []}]
    box = out["natives"]["native:sales.customers"]
    assert box["source"] == "crm" and box["type"] == "openapi"
    assert box["served"] == {"mode": "virtual"}


def test_without_source_registration_the_graph_starts_at_the_table() -> None:
    sql = "SELECT c.postal_code FROM sales.customers c"
    out = lineage_graph_for(sql, {}, natives=[])
    assert all(n["kind"] != "native" for n in out["nodes"])
    assert out["natives"] == {}


def test_the_hop_is_withheld_from_a_user_without_source_registration(monkeypatch) -> None:
    monkeypatch.setattr(lineage_router, "has_capability_request", lambda request, cap: False)
    assert asyncio.run(lineage_router._native_tables(object(), SimpleNamespace())) == []


def test_credentials_and_secret_references_are_never_copied() -> None:
    box = _customers_native().to_dict()
    assert box["hints"] == {"brand": "stripe"}
    flat = repr(box)
    assert "s3cr3t" not in flat and "CRM_KEY" not in flat and "${" not in flat
    files = native_address(
        "csv",
        {"schema_name": "main", "table_name": "t", "file_glob": None},
        _source("csv", path="https://user:pw@files.example.com/a.csv?sig=abc"),
    )
    assert files == (("path", "https://files.example.com/a.csv"),)


def test_a_replica_says_its_freshness_and_last_load() -> None:
    loaded = datetime(2026, 10, 20, 9, 0, tzinfo=timezone.utc)
    due = datetime(2026, 10, 20, 10, 0, tzinfo=timezone.utc)
    assert served(True, 3600, loaded, due) == {
        "mode": "replica",
        "cache_ttl": 3600,
        "last_load": "2026-10-20T09:00:00+00:00",
        "next_load": "2026-10-20T10:00:00+00:00",
    }


@pytest.mark.parametrize(
    ("stype", "table", "kw", "expected"),
    [
        (
            "mongodb",
            {"schema_name": "shop", "table_name": "orders"},
            {},
            (("database", "shop"), ("collection", "orders")),
        ),
        ("elasticsearch", {"schema_name": "x", "table_name": "logs"}, {}, (("index", "logs"),)),
        (
            "kafka",
            {"schema_name": "x", "table_name": "clicks"},
            {"topic": "web.clicks"},
            (("topic", "web.clicks"),),
        ),
        ("python", {"schema_name": "x", "table_name": "forecast"}, {}, (("function", "forecast"),)),
        (
            "parquet",
            {"schema_name": "main", "table_name": "s", "file_glob": "*.parquet"},
            {},
            (("path", "/data/supplies"), ("files", "*.parquet")),
        ),
        (
            "graphql_remote",
            {"schema_name": "graphql", "table_name": "issues"},
            {
                "graphql": {
                    "field_name": "issues",
                    # As graphql_remote.mapper writes it: each argument's name and GraphQL type.
                    "required_args": [
                        {"name": "owner", "gql_type": "String!"},
                        {"name": "repo", "gql_type": "String!"},
                    ],
                    "rows_path": "nodes",
                }
            },
            (("field", "issues"), ("arguments", "owner, repo"), ("rows", "nodes")),
        ),
        (
            "postgresql",
            {"schema_name": "public", "table_name": "pets"},
            {},
            (("table", "public.pets"),),
        ),
    ],
)
def test_each_kind_of_source_names_its_own_address(stype, table, kw, expected) -> None:
    assert native_address(stype, table, _source(stype, path="/data/supplies"), **kw) == expected


def test_a_field_path_and_an_alias_are_renames() -> None:
    table = {
        "columns": [
            {
                "column_name": "zip",
                "alias": None,
                "path": "address.zip",
                "native_filter_type": None,
            },
            {"column_name": "nm", "alias": "name", "path": None, "native_filter_type": None},
        ]
    }
    native = native_table("s.t", _source("mongodb"), table, (), served(False, None, None, None))
    assert [(f.raw, f.column, f.rename) for f in native.fields] == [
        ("address.zip", "zip", "path"),
        ("nm", "name", "alias"),
    ]


def test_a_statement_names_a_base_table_by_its_bare_name() -> None:
    graph = LineageGraph()
    graph.add_node(
        Node(id="customers.postal_code", column="postal_code", relation="customers", kind="source")
    )
    with_native_hop(graph, [_customers_native()])
    assert "native:sales.customers.postalCode" in graph.nodes


def test_a_registered_column_is_named_as_a_view_reads_it() -> None:
    row = {
        "domain_id": "sales",
        "table_name": "customers",
        "column_name": "postalCode",
        "alias": None,
        "visible_to": [],
    }
    seeds, _ = _role_seeds([row], set(), set())
    assert seeds == {"sales.customers.postal_code"}
    assert sql_column_name({"column_name": "x", "alias": "y"}) == "y"
