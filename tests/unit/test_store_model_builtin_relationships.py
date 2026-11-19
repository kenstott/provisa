# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a1c62-9d47-4b0e-b5a3-2c6e8f1d7a94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The store's model leaves out Provisa's built-in relationships (``meta:``/``ontology:``).

The startup seed writes them directly and is their only writer; they are the deployment's, not the
model's. When the model read from the store carried them, the process's configuration did too, and
the graphql-demo auto-registration's retry of deferred config relationships
(``graphql_remote_router._upsert_relationships_to_semantic_layer``) sent each one through the user
path, which refuses the reserved prefix: the demo source never registered.
"""

from __future__ import annotations

import contextlib
from types import SimpleNamespace

import pytest
from sqlalchemy import insert

import provisa.api.app  # noqa: F401
from provisa.api.admin.graphql_remote_router import _upsert_relationships_to_semantic_layer
from provisa.core import secrets_store
from provisa.core.config_loader import parse_config_dict, seed_config, store_config
from provisa.core.database import Database
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import metadata
from provisa.core.store_config import store_model

_RAW = {
    "sources": [{"id": "cfg", "type": "postgresql", "host": "h", "port": 5432, "database": "d"}],
    "domains": [{"id": "sales"}],
    "roles": [
        {
            "id": "seller",
            "description": "Test role.",
            "capabilities": [],
            "domain_access": ["sales"],
        }
    ],
    "tables": [
        {
            "source_id": "cfg",
            "domain_id": "sales",
            "schema": "public",
            "table": name,
            "columns": [{"name": "id", "data_type": "integer", "visible_to": ["seller"]}],
        }
        for name in ("orders", "customers")
    ],
    "relationships": [
        {
            "id": "orders-to-customers",
            "source_table_id": "orders",
            "target_table_id": "customers",
            "source_column": "id",
            "target_column": "id",
            "cardinality": "many-to-one",
        }
    ],
}

_BUILTIN_ID = "meta:orders:customers"


@pytest.fixture(autouse=True)
def no_vault(monkeypatch):
    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)


@pytest.fixture
async def db(control_plane) -> Database:
    plane = control_plane.org()
    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(metadata.tables["sources"]).values(id="__derived__", type="postgresql")
        )
        await seed_config(parse_config_dict(_RAW), conn)
        # A built-in relationship, written as the startup seed writes its own: directly.
        orders = await table_repo.find_by_table_name(conn, "orders")
        customers = await table_repo.find_by_table_name(conn, "customers")
        assert orders is not None and customers is not None
        await conn.execute_core(
            insert(metadata.tables["relationships"]).values(
                id=_BUILTIN_ID,
                source_table_id=orders["id"],
                target_table_id=customers["id"],
                source_column="id",
                target_column="id",
                cardinality="many-to-one",
                alias="builtin_customer",
            )
        )
    return plane


async def test_the_store_model_leaves_out_built_in_relationships(db):
    async with db.acquire() as conn:
        model = await store_model(conn)
    assert [r["id"] for r in model["relationships"]] == ["orders-to-customers"]


async def test_the_deferred_config_retry_sends_no_built_in_relationship_through_the_user_path(db):
    async with db.acquire() as conn:
        config = await store_config(_RAW, conn)
    assert [r.id for r in config.relationships] == ["orders-to-customers"]
    # The graphql-demo auto-registration's call: no detected relationships, then the retry of the
    # configuration's relationships. A built-in one reaching it raises ReservedRelationshipPrefix.
    await _upsert_relationships_to_semantic_layer([], db, SimpleNamespace(config=config))
