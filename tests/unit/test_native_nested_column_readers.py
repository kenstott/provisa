# Copyright (c) 2026 Kenneth Stott
# Canary: 6d0c1b7e-3f52-4a8b-9c1d-2e7a5b90f4c3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1965: a table with a natively array, struct or map column is registered with the source's
own spelling as the column's type, and every later reader of that stored type must resolve it --
landing, the reconcile worklist, import typing, Arrow -- to a JSON column, never raise on it."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.core.models import Source, SourceType
from provisa.executor.drivers.base import QueryResult

# The spelling each source reports for one native nested column, as registration stores it
# (provisa/api/admin/schema_query.py lower-cases what the source's own driver lists).
_STORED = [
    "varchar[]",  # duckdb list
    "integer[][]",  # duckdb nested list
    "array(string)",  # clickhouse
    "array<string>",  # bigquery, databricks, hive
    "struct<a int64>",  # bigquery struct
    "map<string,int>",  # databricks map
    "variant",  # snowflake
]


def _col(name, data_type, pk=False):
    return SimpleNamespace(name=name, data_type=data_type, is_primary_key=pk, embedding=False)


@pytest.mark.parametrize("stored", _STORED)
def test_landing_resolves_a_native_nested_column_as_json(stored):
    from provisa.federation.residency import resolve_landing_args

    source = SimpleNamespace(
        id="s1",
        type="duckdb",
        change_signal="ttl",
        freshness_gate=False,
        cache_ttl=None,
        replicate=None,
        load_protected=False,
    )
    table = SimpleNamespace(
        source_id="s1",
        schema_name="main",
        table_name="orders",
        change_signal=None,
        watermark_column=None,
        probe_type=None,
        landing_filter=None,
        live=None,
        columns=[_col("id", "bigint", pk=True), _col("tags", stored)],
    )

    args = resolve_landing_args(source, table)

    assert args.columns == [("id", "bigint"), ("tags", "json")]


@pytest.mark.parametrize("stored", _STORED)
async def test_the_reconcile_worklist_resolves_a_native_nested_column_as_json(monkeypatch, stored):
    from provisa.federation import replica_routing

    source = SimpleNamespace(id="s1")
    registered = {
        "schema_name": "main",
        "table_name": "orders",
        "landing_filter": None,
        "row_materialize": False,
        "columns": [
            {
                "column_name": "id",
                "data_type": "bigint",
                "is_primary_key": True,
                "native_filter_type": None,
            },
            {
                "column_name": "tags",
                "data_type": stored,
                "is_primary_key": False,
                "native_filter_type": None,
            },
        ],
    }

    async def _replica_tables(_engine, _state):
        return [(source, registered)]

    monkeypatch.setattr(replica_routing, "replica_tables", _replica_tables)

    work = await replica_routing.landing_worklist(SimpleNamespace(name="duckdb"), object())

    assert work == [(source, "main", "orders", [("id", "bigint"), ("tags", "json")], ["id"], None)]


@pytest.mark.parametrize("stored", _STORED)
def test_arrow_carries_a_native_nested_column_as_json_text(stored):
    import pyarrow as pa

    from provisa.core.ir_arrow import arrow_schema

    schema = arrow_schema([("id", "bigint"), ("tags", stored)])

    assert schema.field("tags").type == pa.string()


@pytest.mark.parametrize(
    ("catalog_row", "expected"),
    [
        (("ARRAY", "_text", "text[]", "b"), ("json", 1, "text", "text[]")),
        (("USER-DEFINED", "address", "address", "c"), ("json", 0, None, "address")),
        (("USER-DEFINED", "order_status", "order_status", "e"), ("text", 0, None, None)),
    ],
)
async def test_import_typing_types_a_postgres_array_or_composite_column(catalog_row, expected):
    """An imported model's untyped columns are filled from the source's catalog.
    information_schema's data_type for an array is the marker ``ARRAY`` and for an enum, a domain
    or a composite ``USER-DEFINED``; pg_catalog states the rest."""
    from provisa.api.admin.import_typing import _discover

    class _Pool:
        def has(self, _source_id):
            return True

        async def execute(self, _source_id, _sql, _params=None):
            return QueryResult(
                rows=[("id", "integer", "int4", "integer", "b"), ("tags", *catalog_row)],
                column_names=[],
            )

    source = Source(
        id="orders-pg",
        type=SourceType.postgresql,
        host="orders-db.internal",
        port=5432,
        database="shop",
        username="reader",
        password="secret",
    )

    types = await _discover(_Pool(), source, "public", "orders")

    assert types["id"].data_type == "integer"
    tags = types["tags"]
    assert (tags.data_type, tags.array_depth, tags.element_type, tags.native_type) == expected
