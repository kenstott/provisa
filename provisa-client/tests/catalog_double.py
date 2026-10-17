# Copyright (c) 2026 Kenneth Stott
# Canary: 726711a9-4e50-4bca-a5ad-1665638bb26e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A /data/catalog answer and a SQLAlchemy connection over a Provisa DB-API connection, for the
dialect's reflection tests."""

from __future__ import annotations

import base64
from unittest.mock import MagicMock

import pyarrow as pa

from provisa_client.dbapi import Connection

BASE = "http://localhost:8001"

ORDERS = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False, metadata={b"primary_key": b"true"}),
        pa.field("amount", pa.decimal128(12, 2), metadata={b"description": b"Order total"}),
        pa.field("region", pa.utf8()),
        pa.field("placed_at", pa.timestamp("us", tz="UTC")),
    ],
    metadata={b"domain": b"shop"},
)
USERS = pa.schema([pa.field("id", pa.int32()), pa.field("active", pa.bool_())])
TICKETS = pa.schema([pa.field("id", pa.int64()), pa.field("opened", pa.date32())])


def catalog(*tables: tuple[str, str, pa.Schema]) -> dict:
    """The /data/catalog body for ``(domain, table, schema)`` triples."""
    return {
        "tables": [
            {
                "path": [domain, name],
                "schema": base64.b64encode(schema.serialize().to_pybytes()).decode("ascii"),
            }
            for domain, name, schema in tables
        ]
    }


SHOP_AND_SUPPORT = catalog(
    ("shop", "orders", ORDERS), ("shop", "users", USERS), ("support", "tickets", TICKETS)
)


def sa_connection(*, token: str | None = "tok", role: str | None = "analyst", org=None):
    """A SQLAlchemy connection whose DB-API connection is Provisa's, as an engine hands one out."""
    raw = Connection(base_url=BASE, token=token, role=role, org=org)
    conn = MagicMock()
    conn.connection.dbapi_connection = raw
    return conn
