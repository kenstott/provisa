# Copyright (c) 2026 Kenneth Stott
# Canary: 1681c66e-c6cc-447e-ba2a-96f58e08aa13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where an environment's replicas go, and what retiring it may remove (REQ-1622, REQ-1912).

Every org environment has a replicas schema of its own, named for the org and the environment, so
no two environments and no two orgs write the same replica. A store with no schemas cannot give
one and is refused. Retiring an environment drops its replicas schema and never prod's.
"""

# Requirements: REQ-1622, REQ-1912, REQ-1620

from __future__ import annotations

from types import SimpleNamespace

from contextlib import asynccontextmanager

import pytest

from provisa.core.environments import PROD
from provisa.core.request_context import reset_current_env, set_current_env
from provisa.federation.replica_address import (
    StoreHasNoSchemas,
    replica_schema,
    require_schema_capable_store,
)
from provisa.federation.store_scope import drop_env_store

PG = "postgresql://u:p@h:5432/store"
SQLITE = "sqlite:////var/provisa/store.db"


def _in_env(env: str | None, org_id: str) -> str:
    token = set_current_env(env)
    try:
        return replica_schema(org_id)
    finally:
        reset_current_env(token)


# --- every org environment has a replicas schema of its own ---------------------


def test_prod_replicas_schema_is_named_for_the_org():
    assert _in_env(None, "acme") == "org_acme_replicas"
    assert _in_env(PROD, "acme") == "org_acme_replicas"


def test_non_prod_gets_its_own_schema():
    assert _in_env("feature_x", "acme") == "org_acme_env_feature_x_replicas"


def test_two_environments_do_not_share_a_schema():
    assert _in_env("feature_x", "acme") != _in_env("feature_y", "acme")
    assert _in_env("feature_x", "acme") != _in_env(PROD, "acme")


def test_two_orgs_do_not_share_a_schema():
    assert _in_env(PROD, "acme") != _in_env(PROD, "globex")
    assert _in_env("feature_x", "acme") != _in_env("feature_x", "globex")


# --- a store with no schemas is refused, for every environment -------------------


def test_a_schemaless_store_is_refused_naming_the_store():
    with pytest.raises(StoreHasNoSchemas) as excinfo:
        require_schema_capable_store(SQLITE)
    message = str(excinfo.value)
    assert "'sqlite'" in message
    assert "Replicas and materialized views are each written to a schema" in message
    assert excinfo.value.dsn_scheme == "sqlite"


@pytest.mark.parametrize(
    "dsn", [PG, "duckdb:////var/provisa/materialize.duckdb", "mssql://server/warehouse"]
)
def test_a_store_with_schemas_is_accepted(dsn):
    require_schema_capable_store(dsn)


def test_the_engine_refuses_a_schemaless_store_where_it_resolves_its_store(monkeypatch):
    """``materialize_store()`` is the one door every replica address and every write comes
    through, so the refusal there covers both — and the start, which resolves the store."""
    from provisa.federation.engine import build_engine

    monkeypatch.setenv("PROVISA_MATERIALIZE_URL", SQLITE)
    with pytest.raises(StoreHasNoSchemas):
        build_engine("duckdb").materialize_store()


# --- what retire may drop -------------------------------------------------------


@pytest.fixture
def store(monkeypatch):
    """The store write face, recording the statements issued and the DSN they went to."""
    seen: list[tuple[str, str]] = []

    class _Conn:
        def __init__(self, dsn: str) -> None:
            self._dsn = dsn

        async def execute_core(self, stmt):
            seen.append((self._dsn, str(stmt)))
            # The store's schemas, as a retire lists them for the environment's synthetic datasets.
            return SimpleNamespace(
                fetchall=lambda: [
                    ("org_acme_env_feature_x_syn__load",),
                    ("org_acme_env_feature_xx_syn__other",),
                    ("org_acme_env_feature_x_replicas",),
                ]
            )

    @asynccontextmanager
    async def _connection(dsn: str):
        yield _Conn(dsn)

    monkeypatch.setattr("provisa.federation.store_writer.store_connection", _connection)
    return seen


@pytest.mark.asyncio
async def test_retire_drops_the_environments_replicas_schema(store):
    dropped = await drop_env_store(PG, "acme", "feature_x")
    assert dropped == "org_acme_env_feature_x_replicas"
    # The export views a store publishes over those replicas go first: they select from them.
    assert store == [
        (PG, 'DROP SCHEMA IF EXISTS "org_acme_env_feature_x_export" CASCADE'),
        (PG, 'DROP SCHEMA IF EXISTS "org_acme_env_feature_x_replicas" CASCADE'),
        (PG, "SELECT schema_name FROM information_schema.schemata"),
        # REQ-1939: its synthetic datasets' schemas go too, and no other environment's.
        (PG, 'DROP SCHEMA IF EXISTS "org_acme_env_feature_x_syn__load" CASCADE'),
    ]


@pytest.mark.asyncio
async def test_retire_never_reaches_prods_schema(store):
    assert await drop_env_store(PG, "acme", PROD) is None
    assert store == [], "retiring prod connected to the store"
