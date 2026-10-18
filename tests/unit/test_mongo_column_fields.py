# Copyright (c) 2026 Kenneth Stott
# Canary: d4ac355a-bd38-4512-b30a-f72750a68b2f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""#266: a MongoDB column named apart from the field it reads (its path) is found on DuckDB.

DuckDB's mongo extension reads a collection by its fields, a nested path joined with `_`; the
attached table's view exposes each such column under its registered name beside them."""

from __future__ import annotations

from types import SimpleNamespace

import psycopg

import provisa.api.app  # noqa: F401 -- the API layer says whether the deployment holds many orgs, which reading a source's secrets asks
from provisa.core.models import Column, SourceType, Table
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
from provisa.federation.native_backend import _mongo_column_fields
from tests.embedded_pg import DATABASE, PASSWORD, ROLE, start_local_postgres

_VIS = ["org_admin"]


def test_a_column_named_apart_from_its_field_is_listed_by_its_table():
    config_table = Table(
        source_id="catalog",
        domain_id="store",
        schema="catalog",
        table="products",
        columns=[
            Column(name="product_id", data_type="integer", path="_id", visible_to=_VIS),
            Column(
                name="battery_hours",
                data_type="integer",
                path="specs.battery_hours",
                visible_to=_VIS,
            ),
            Column(
                name="specs_weight_g", data_type="integer", path="specs.weight_g", visible_to=_VIS
            ),
            Column(name="name", data_type="varchar", visible_to=_VIS),
        ],
    )
    registered_row = {
        "source_id": "catalog",
        "schema_name": "catalog",
        "table_name": "ratings",
        "columns": [{"column_name": "stars", "path": "score.stars"}, {"column_name": "id"}],
    }
    assert _mongo_column_fields([config_table, registered_row]) == {
        ("catalog", "catalog", "products"): [
            ("_id", "product_id"),
            ("specs_battery_hours", "battery_hours"),
        ],
        ("catalog", "catalog", "ratings"): [("score_stars", "stars")],
    }


def test_the_attached_view_reads_each_renamed_column_by_its_name():
    pg = start_local_postgres()
    try:
        with psycopg.connect(pg.dsn(), autocommit=True) as conn:
            conn.execute("CREATE TABLE products (_id INTEGER, specs_battery_hours INTEGER)")
            conn.execute("INSERT INTO products VALUES (6, 5)")
        runtime = DuckDBFederationRuntime()
        try:
            source = dict(
                id="catalog",
                type=SourceType.postgresql,
                catalog="catalog",
                host="127.0.0.1",
                port=pg.port,
                database=DATABASE,
                username=ROLE,
                password=PASSWORD,
                federation_hints={},
                mapping={},
                max_live_concurrency=None,
                schema_name="public",
                table_name="products",
            )
            fields = [("_id", "product_id"), ("specs_battery_hours", "battery_hours")]
            runtime.attach_source(SimpleNamespace(**source, column_fields=fields))
            read = 'SELECT product_id, battery_hours FROM "catalog"."public"."products"'
            assert runtime.run_sync(read).rows == [(6, 5)]
            # Attached again from no table model (introspection): the view keeps its names.
            runtime.attach_source(SimpleNamespace(**source))
            assert runtime.run_sync(read).rows == [(6, 5)]
        finally:
            runtime.close()
    finally:
        pg.stop()
