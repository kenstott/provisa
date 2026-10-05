# Copyright (c) 2026 Kenneth Stott
# Canary: e5ed226b-4ff5-4e49-b5d0-6fb89dc39e42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: what the pg engine keeps to reach a source live — its foreign server, user
mapping, imported foreign tables and live view — is named after the source's catalog name, which
carries its org and environment (REQ-1266, REQ-1529).

Two orgs keep one engine database, each with a source ``sales`` of its own (in two databases of a
real source Postgres). Each org's runtime attaches its source; each reads its own rows, through
objects of its own."""

# Requirements: REQ-1266, REQ-1529

from __future__ import annotations

from types import SimpleNamespace

import psycopg
import pytest

from provisa.compiler.naming import live_view_schema, org_prefixed_catalog, source_to_catalog
from tests.integration.test_pg_engine_landing_never_writes_source_e2e import _SourceAndEngine

pytestmark = [pytest.mark.integration]

_BOOT = "acme"


def _source(stack: _SourceAndEngine, database: str, org: str, env: str | None) -> SimpleNamespace:
    return SimpleNamespace(
        id="sales",
        catalog=org_prefixed_catalog(org, source_to_catalog("sales"), default_org=_BOOT, env=env),
        type=SimpleNamespace(value="postgresql"),
        host="127.0.0.1",
        port=stack.source_port,
        database=database,
        username="provisa",
        password="provisa",
        federation_hints={"schema": "public"},
        schema_name="public",
        table_name="orders",
    )


def _ids(runtime, source: SimpleNamespace) -> list[int]:
    schema = live_view_schema(source.catalog, source.schema_name)
    return [r[0] for r in runtime.run_sync(f'SELECT id FROM "{schema}"."orders" ORDER BY id').rows]


@pytest.fixture
def stack():
    s = _SourceAndEngine()
    s.start()
    try:
        with psycopg.connect(s.url(s.source_port, "shop"), autocommit=True) as conn:
            conn.execute("CREATE DATABASE shop_beta")
            conn.execute("CREATE DATABASE shop_beta_qa")
        for database, row in (("shop_beta", 900), ("shop_beta_qa", 950)):
            with psycopg.connect(s.url(s.source_port, database), autocommit=True) as conn:
                conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)")
                conn.execute("INSERT INTO orders VALUES (%s)", (row,))
        yield s
    finally:
        s.stop()


def test_orgs_and_environments_on_one_engine_database_each_read_their_own_source(stack):
    from provisa.federation.pg_runtime import PgFederationRuntime

    dsn = stack.url(stack.engine_port, "provisa")
    boot = _source(stack, "shop", _BOOT, None)
    beta = _source(stack, "shop_beta", "beta", None)
    beta_qa = _source(stack, "shop_beta_qa", "beta", "qa")
    runtimes = [PgFederationRuntime(engine_dsn=dsn) for _ in range(3)]
    try:
        # The other org's attach first: the boot org's must not land on its objects, nor theirs
        # on the boot org's.
        for runtime, source in zip((runtimes[1], runtimes[2], runtimes[0]), (beta, beta_qa, boot)):
            runtime.attach_source(source)

        assert _ids(runtimes[0], boot) == [1, 2, 3, 4, 5]
        assert _ids(runtimes[1], beta) == [900]
        assert _ids(runtimes[2], beta_qa) == [950]
        with psycopg.connect(dsn, autocommit=True) as conn:
            servers = {r[0] for r in conn.execute("SELECT srvname FROM pg_foreign_server")}
        assert servers == {"fdw_sales", "fdw_org_beta__sales", "fdw_org_beta_env_qa__sales"}
    finally:
        for runtime in runtimes:
            runtime.close()
