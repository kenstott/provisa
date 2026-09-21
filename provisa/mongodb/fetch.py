# Copyright (c) 2026 Kenneth Stott
# Canary: b4e9f2a6-7c1d-4e83-9a5f-6d2b8c4e91f7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MongoDB over the wire protocol, engine-independently (REQ-1730).

Trino's own connector (``TrinoMongoConnector``) was the only reader of a MongoDB source — every
self-only warehouse engine (Snowflake, BigQuery, Databricks, mssql/Fabric/Synapse) and DuckDB itself
has no live MongoDB reach at all, the same gap REQ-1672/1675/1676 already closed for
Elasticsearch/Redis/Cassandra. This module is the native reader (``pymongo``): a database is the
schema, a collection is the table. Synchronous — callers run it in a thread.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator

_DEFAULT_PORT = 27017


@dataclass(frozen=True)
class MongoConnection:  # REQ-1730
    host: str
    port: int = _DEFAULT_PORT
    username: str | None = None
    password: str | None = None

    @classmethod
    def build(
        cls, host: str, port: int, *, username: str | None = None, password: str | None = None
    ) -> "MongoConnection":
        return cls(
            host=host or "localhost",
            port=port or _DEFAULT_PORT,
            username=username or None,
            password=password if (username and password) else None,
        )

    @contextmanager
    def client(self) -> Iterator[Any]:
        from pymongo import MongoClient

        client: Any = MongoClient(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            serverSelectionTimeoutMS=30000,
        )
        try:
            yield client
        finally:
            client.close()


def _coerce(value: Any) -> Any:
    """A document field value, JSON/Arrow-safe: BSON's ``ObjectId``/``datetime`` pass through
    everywhere else in the pipeline as opaque objects otherwise (the same reason ``fetch_rows``'s
    every sibling module — cassandra/redis/elasticsearch — never returns a driver-native type)."""
    from bson import ObjectId

    if isinstance(value, ObjectId):
        return str(value)
    return value


def fetch_rows(
    conn: MongoConnection, database: str, collection: str, columns: list[str]
) -> list[dict]:  # REQ-1730
    """Every document of ``database.collection``, ``columns`` projected (``_id`` excluded unless
    named), read via a single find-all cursor."""
    projection = {c: 1 for c in columns}
    if "_id" not in columns:
        projection["_id"] = 0
    rows: list[dict] = []
    with conn.client() as client:
        for doc in client[database][collection].find({}, projection):
            rows.append({c: _coerce(doc.get(c)) for c in columns})
    return rows
