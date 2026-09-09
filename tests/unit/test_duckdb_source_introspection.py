# Copyright (c) 2026 Kenneth Stott
# Canary: 19cb9795-52fa-4f7e-9471-dcf80c971bc1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1673: a native engine lists a source's schemas, tables and columns BEFORE any table on it is
registered — the DuckDB runtime attaches the raw source. Exercised with a sqlite file, the one
ATTACH source that needs no network or extension download."""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from provisa.core.models import SourceType


@pytest.fixture
def sqlite_source(tmp_path):
    db = tmp_path / "shop.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, sku TEXT, qty INTEGER)")
    con.execute("CREATE TABLE skus (sku TEXT PRIMARY KEY)")
    con.commit()
    con.close()
    return SimpleNamespace(
        id="shop",
        type=SourceType.sqlite,
        host="",
        port=0,
        database="",
        username="",
        password="",
        path=str(db),
        federation_hints={},
        mapping={},
        schema_name="main",
        table_name="orders",
    )


def test_runtime_lists_schemas_and_tables_of_an_unregistered_source(sqlite_source):
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    rt = DuckDBFederationRuntime()
    try:
        assert rt.introspect_schemas(sqlite_source) == ["main"]
        assert rt.introspect_tables(sqlite_source, "main") == ["orders", "skus"]
        assert rt.introspect_tables(sqlite_source, "nope") == []
    finally:
        rt.close()


def test_backend_seams_answer_for_an_attach_source_and_decline_otherwise(sqlite_source):
    from provisa.federation.backend import EngineBackend
    from provisa.federation.engine import build_engine

    backend = EngineBackend(build_engine())  # the embedded DuckDB engine
    assert backend.introspect_schemas(None, sqlite_source) == ["main"]
    assert backend.introspect_tables(None, sqlite_source, "main") == ["orders", "skus"]
    cols = backend.introspect_columns(None, sqlite_source, "main", "orders")
    assert set(cols) == {"id", "sku", "qty"}
    # An adapter-fetched source has no database to attach: no seam, the caller falls through.
    api = SimpleNamespace(**{**vars(sqlite_source), "type": SourceType.openapi})
    assert backend.introspect_schemas(None, api) is None


@pytest.mark.parametrize(
    "source_type", ["mongodb", "sqlserver", "snowflake", "bigquery", "firebird"]
)
def test_extension_connectors_attach_under_a_private_alias(source_type):
    """The attach alias must differ from the source id: the runtime creates the physical catalog
    under the id, and DuckDB refuses a second database of that name."""
    from provisa.federation.engine import build_engine

    engine = build_engine()
    src = SimpleNamespace(
        id="shop",
        type=SimpleNamespace(value=source_type),
        host="h",
        port=1,
        database="d",
        username="u",
        password="p",
        path=None,
        base_url="grpc://h:1",
        federation_hints={"project": "proj"},
        mapping={},
    )
    details = engine.connector_for(source_type).details(src)
    assert details["raw_alias"] == "_src_shop"
    assert 'AS "_src_shop"' in details["attach"]
    assert 'AS "shop"' not in details["attach"]
