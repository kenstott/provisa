# Copyright (c) 2026 Kenneth Stott
# Canary: 9e1b7d4c-3a56-4f89-b0d2-8c6a2e5f4d17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1871: PgWrappersMongoDbConnector — live ATTACH_R reach for a mongodb source via Supabase's
`wrappers` framework's MongoDB FDW, alongside the existing FETCH (materialize) reach.

Live-verified this session against the exact MongoDB instance the rejected mongo_fdw candidate
(REQ-1869) failed against: real data, correct values, exact filtered counts. This suite is pure
logic — no live MongoDB/Postgres — driven by a fake fetch callable.
"""

from __future__ import annotations

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.connector import Mechanism
from provisa.federation.connector_duckdb import PgWrappersMongoDbConnector


def _src(sid: str, **kw) -> Source:
    return Source(id=sid, type=SourceType.mongodb, **kw)


class _FakeFetch:
    def __init__(self, *, installed: bool, available: bool = False, handler: bool = False):
        self._installed = installed
        self._available = available
        self._handler = handler

    async def __call__(self, sql: str):
        if "pg_extension" in sql:
            return [{"one": 1}] if self._installed else []
        if "pg_available_extensions" in sql:
            return [{"one": 1}] if self._available else []
        if "mongodb_fdw_handler" in sql:
            return [{"one": 1}] if self._handler else []
        return []


def test_connector_identity_and_reach_modes():
    c = PgWrappersMongoDbConnector()
    assert c.engine == "postgres"
    assert c.source_type == "mongodb"
    assert c.key == "wrappers_mongodb"
    assert c.mechanism is Mechanism.ATTACH_R
    assert c.reach_modes == frozenset({Mechanism.ATTACH_R, Mechanism.FETCH})
    assert c.reads_in_place is True


def test_capability_pushes_down_literal_predicates_not_join_predicates():
    # Live-verified via EXPLAIN (2026-09-28): a literal/constant WHERE on this table alone
    # pushes down; a join-derived predicate does not (no parameterized-path support).
    cap = PgWrappersMongoDbConnector().capability()
    assert cap.predicate_pushdown is True
    assert cap.join_pushdown is False
    assert cap.aggregate_pushdown is False


def test_details_emit_extension_wrapper_server_and_foreign_table():
    details = PgWrappersMongoDbConnector().details(
        _src(
            "orders_docs",
            host="mongodb",
            port=27017,
            database="app",
            federation_hints={"collection": "order_docs"},
        )
    )
    ddl = details["attach_ddl"]
    assert ddl[0] == "CREATE EXTENSION IF NOT EXISTS wrappers"
    assert any("CREATE FOREIGN DATA WRAPPER mongodb_wrapper" in s for s in ddl)
    assert any(
        'CREATE SERVER IF NOT EXISTS "mongo_orders_docs"' in s
        and "mongodb://mongodb:27017/app" in s
        for s in ddl
    )
    assert any('CREATE SCHEMA IF NOT EXISTS "mongo_orders_docs"' in s for s in ddl)
    assert any(
        'CREATE FOREIGN TABLE IF NOT EXISTS "mongo_orders_docs"."order_docs"' in s
        and "database 'app'" in s
        and "collection 'order_docs'" in s
        and "rowid_column '_id'" in s
        for s in ddl
    )
    assert details["local_schema"] == "mongo_orders_docs"
    assert details["local_table"] == "order_docs"


def test_details_declares_typed_columns_from_columns_hint():
    # wrappers' mongodb_wrapper has no IMPORT FOREIGN SCHEMA -- every column is declared upfront,
    # and Connector.details() is never given the registered Table's column list, so
    # federation_hints['columns'] is the escape hatch.
    details = PgWrappersMongoDbConnector().details(
        _src(
            "typed",
            host="mongodb",
            database="app",
            federation_hints={
                "collection": "order_docs",
                "columns": "order_id:integer,status:text",
            },
        )
    )
    create = next(s for s in details["attach_ddl"] if "CREATE FOREIGN TABLE" in s)
    assert '_id text, "order_id" integer, "status" text' in create
    assert "__doc" not in create


def test_details_falls_back_to_raw_document_shape_without_columns_hint():
    details = PgWrappersMongoDbConnector().details(
        _src("raw", host="mongodb", database="app", federation_hints={"collection": "order_docs"})
    )
    create = next(s for s in details["attach_ddl"] if "CREATE FOREIGN TABLE" in s)
    assert "_id text, __doc jsonb" in create


def test_details_includes_direct_connection_when_configured():
    # A replica set reached via its published host port (not its internal Docker/Compose
    # address) needs directConnection=true, not replicaSet= -- confirmed live 2026-09-28.
    details = PgWrappersMongoDbConnector().details(
        _src(
            "direct",
            host="localhost",
            port=27317,
            database="app",
            federation_hints={"collection": "order_docs", "direct_connection": "true"},
        )
    )
    conn_str = next(s for s in details["attach_ddl"] if "conn_string" in s)
    assert "directConnection=true" in conn_str
    assert "replicaSet" not in conn_str


def test_direct_connection_wins_over_replica_set_when_both_set():
    details = PgWrappersMongoDbConnector().details(
        _src(
            "both",
            host="mongodb",
            database="app",
            federation_hints={
                "collection": "order_docs",
                "direct_connection": "true",
                "replica_set": "rs0",
            },
        )
    )
    conn_str = next(s for s in details["attach_ddl"] if "conn_string" in s)
    assert "directConnection=true" in conn_str
    assert "replicaSet" not in conn_str


def test_details_includes_replica_set_when_configured():
    details = PgWrappersMongoDbConnector().details(
        _src(
            "rs",
            host="mongodb",
            database="app",
            federation_hints={"collection": "order_docs", "replica_set": "rs0"},
        )
    )
    conn_str = next(s for s in details["attach_ddl"] if "conn_string" in s)
    assert "replicaSet=rs0" in conn_str


def test_details_includes_credentials_when_set():
    details = PgWrappersMongoDbConnector().details(
        _src(
            "creds",
            host="mongodb",
            database="app",
            username="u",
            password="p",
            federation_hints={"collection": "order_docs"},
        )
    )
    conn_str = next(s for s in details["attach_ddl"] if "conn_string" in s)
    assert "mongodb://u:p@mongodb:27017/app" in conn_str


def test_details_raises_loud_when_collection_missing():
    with pytest.raises(ValueError, match="collection"):
        PgWrappersMongoDbConnector().details(_src("no_collection", host="mongodb", database="app"))


async def test_probe_available_when_wrappers_installed_with_mongodb_handler():
    r = await PgWrappersMongoDbConnector().probe(_FakeFetch(installed=True, handler=True))
    assert r.available is True


async def test_probe_unavailable_when_wrappers_absent():
    r = await PgWrappersMongoDbConnector().probe(_FakeFetch(installed=False, available=False))
    assert r.available is False


async def test_probe_unavailable_when_wrappers_installed_but_no_mongodb_handler():
    r = await PgWrappersMongoDbConnector().probe(_FakeFetch(installed=True, handler=False))
    assert r.available is False
    assert "mongodb" in r.reason
